const connection = @import("connection.zig");
const compress = @import("compress.zig");
const std = @import("std");

pub const UserId = connection.UserId;

pub const Context = struct {
    io: std.Io,
    allocator: std.mem.Allocator,

    mutex: std.Io.Mutex,
    connections: std.AutoHashMap(UserId, connection.Connection),
    pending_reads: std.ArrayList(UserId),

    next_user_identifier: UserId,

    pub fn init(io: std.Io, allocator: std.mem.Allocator) Context {
        return .{
            .io = io,
            .allocator = allocator,

            .mutex = .init,
            .connections = .init(allocator),
            .pending_reads = .empty,

            .next_user_identifier = 1,
        };
    }

    pub fn deinit(self: *Context) !void {
        try self.lockContext();
        defer self.unlockContext();

        var connection_iterator = self.connections.valueIterator();

        while (connection_iterator.next()) |existing_connection| {
            try existing_connection.close();
        }

        self.connections.deinit();
        self.pending_reads.deinit(self.allocator);
    }

    pub fn addConnection(self: *Context, stream: std.Io.net.Stream) !UserId {
        try self.lockContext();
        defer self.unlockContext();

        const assigned_identifier = self.next_user_identifier;

        self.next_user_identifier += 1;

        const new_connection: connection.Connection = .init(self.io, stream, assigned_identifier);

        try self.connections.put(assigned_identifier, new_connection);

        return assigned_identifier;
    }

    pub fn removeConnection(self: *Context, user_identifier: UserId) !void {
        try self.lockContext();
        defer self.unlockContext();

        if (self.connections.fetchRemove(user_identifier)) |removed_entry| {
            var removed_connection = removed_entry.value;

            try removed_connection.close();
        }

        var index: usize = 0;

        while (index < self.pending_reads.items.len) {
            if (self.pending_reads.items[index] == user_identifier) {
                _ = self.pending_reads.orderedRemove(index);
            } else {
                index += 1;
            }
        }
    }

    pub fn pollConnection(self: *Context, user_identifier: UserId) !?connection.ReceivedPacket {
        try self.lockContext();
        defer self.unlockContext();

        const connection_pointer = self.connections.getPtr(user_identifier) orelse return null;

        if (connection_pointer.tryAwaitRead()) |packet| {
            return packet;
        } else |error_value| {
            std.log.err("[netling] read error for user {}: {}", .{ user_identifier, error_value });

            return null;
        }

        return null;
    }

    pub fn getConnection(self: *Context, user_identifier: UserId) !?connection.Connection {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        return self.connections.get(user_identifier);
    }

    pub fn getConnectionLocked(self: *Context, user_identifier: UserId) ?*connection.Connection {
        return self.connections.getPtr(user_identifier);
    }

    pub fn lockContext(self: *Context) !void {
        try self.mutex.lock(self.io);
    }

    pub fn unlockContext(self: *Context) void {
        self.mutex.unlock(self.io);
    }

    pub fn startReceive(self: *Context, user_identifier: UserId) !void {
        try self.lockContext();
        defer self.unlockContext();

        const connection_pointer = self.connections.getPtr(user_identifier) orelse return error.UnknownUser;

        for (self.pending_reads.items) |pending| {
            if (pending == user_identifier) return error.ReadAlreadyPending;
        }

        try connection_pointer.receivePacketAsync(self.allocator);
        try self.pending_reads.append(self.allocator, user_identifier);
    }

    pub fn pollReceived(self: *Context) !std.ArrayList(ReceivedPacketWithId) {
        try self.lockContext();
        defer self.unlockContext();

        var completed: std.ArrayList(ReceivedPacketWithId) = .empty;
        var index: usize = 0;

        errdefer completed.deinit(self.allocator);

        while (index < self.pending_reads.items.len) {
            const user_id = self.pending_reads.items[index];
            const connection_pointer = self.connections.getPtr(user_id).?;

            const maybe_packet = connection_pointer.tryAwaitRead();

            if (maybe_packet) |packet| {
                _ = self.pending_reads.orderedRemove(index);

                try completed.append(self.allocator, .{
                    .user_identifier = user_id,
                    .packet = packet,
                });
            } else {
                if (connection_pointer.read_task == null) {
                    _ = self.pending_reads.orderedRemove(index);

                    try connection_pointer.close();

                    _ = self.connections.remove(user_id);
                } else index += 1;
            }
        }

        return completed;
    }

    pub fn sendPacket(
        self: *Context,
        user_identifier: UserId,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.Method,
    ) !void {
        try self.lockContext();
        defer self.unlockContext();

        const connection_pointer = self.connections.getPtr(user_identifier) orelse return error.UnknownUser;

        try connection_pointer.sendPacketAsync(
            event_identifier,
            PayloadType,
            payload_value,
            compression_method,
        );
    }
};

pub const ReceivedPacketWithId = struct {
    user_identifier: UserId,
    packet: connection.ReceivedPacket,

    pub fn deinit(self: *ReceivedPacketWithId) void {
        self.packet.deinit();
    }
};
