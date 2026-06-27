const connection = @import("connection.zig");
const std = @import("std");

pub const UserId = connection.UserId;

pub const Context = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    connections: std.AutoHashMap(UserId, connection.Connection),
    next_user_identifier: UserId,

    pub fn init(io: std.Io, allocator: std.mem.Allocator) Context {
        return .{
            .io = io,
            .allocator = allocator,
            .connections = std.AutoHashMap(UserId, connection.Connection).init(allocator),
            .next_user_identifier = 1,
        };
    }

    pub fn deinit(self: *Context) void {
        var connection_iterator = self.connections.valueIterator();

        while (connection_iterator.next()) |existing_connection| existing_connection.close();

        self.connections.deinit();
    }

    pub fn addConnection(self: *Context, stream: std.Io.net.Stream) !UserId {
        const assigned_identifier = self.next_user_identifier;

        self.next_user_identifier += 1;

        const new_connection = connection.Connection.init(self.io, stream, assigned_identifier);

        try self.connections.put(assigned_identifier, new_connection);

        return assigned_identifier;
    }

    pub fn removeConnection(self: *Context, user_identifier: UserId) void {
        if (self.connections.fetchRemove(user_identifier)) |removed_entry| {
            var removed_connection = removed_entry.value;

            removed_connection.close();
        }
    }

    pub fn getConnection(self: *Context, user_identifier: UserId) ?*connection.Connection {
        return self.connections.getPtr(user_identifier);
    }
};
