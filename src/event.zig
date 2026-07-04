const serialize = @import("serialize.zig");
const compress = @import("compress.zig");
const context = @import("context.zig");
const std = @import("std");

var global_event_counter: u16 = 0;

fn allocateEventIdentifier() u16 {
    const identifier = global_event_counter;

    global_event_counter += 1;

    return identifier;
}

pub const Role = enum {
    client,
    server,
};

pub fn Event(comptime IncomingType: type, comptime OutgoingType: type) type {
    return struct {
        pub const Incoming = IncomingType;
        pub const Outgoing = OutgoingType;

        event_identifier: u16,

        io: std.Io,
        compression_method: compress.Method,

        pub fn init(io: std.Io, compression_method: compress.Method) @This() {
            return .{
                .event_identifier = allocateEventIdentifier(),

                .io = io,
                .compression_method = compression_method,
            };
        }

        pub fn iterator(
            self: *const @This(),
            context_state: *context.Context,
        ) EventIterator(IncomingType) {
            return EventIterator(IncomingType).init(
                self.io,
                context_state,
                self.event_identifier,
                self.compression_method,
            );
        }

        pub fn sendTo(
            self: *const @This(),
            context_state: *context.Context,
            outgoing_value: OutgoingType,
            target_user_identifier: context.UserId,
        ) !void {
            var target_connection = context_state.getConnectionLocked(target_user_identifier) orelse return error.UnknownUser;

            try target_connection.sendPacket(
                self.event_identifier,
                OutgoingType,
                outgoing_value,
                self.compression_method,
            );
        }

        pub fn broadcast(
            self: *const @This(),
            context_state: *context.Context,
            outgoing_value: OutgoingType,
        ) !void {
            try context_state.mutex.lock(self.io);
            defer context_state.mutex.unlock(self.io);

            var connection_iterator = context_state.connections.valueIterator();

            while (connection_iterator.next()) |existing_connection| {
                try existing_connection.sendPacket(
                    self.event_identifier,
                    OutgoingType,
                    outgoing_value,
                    self.compression_method,
                );
            }
        }

        pub fn broadcastExcept(
            self: *const @This(),
            context_state: *context.Context,
            outgoing_value: OutgoingType,
            excluded_user_identifier: context.UserId,
        ) !void {
            try context_state.mutex.lock(self.io);
            defer context_state.mutex.unlock(self.io);

            var connection_iterator = context_state.connections.valueIterator();

            while (connection_iterator.next()) |existing_connection| {
                if (existing_connection.user_identifier == excluded_user_identifier) continue;

                try existing_connection.sendPacket(
                    self.event_identifier,
                    OutgoingType,
                    outgoing_value,
                    self.compression_method,
                );
            }
        }
    };
}

pub fn EventIterator(comptime IncomingType: type) type {
    return struct {
        pub const IncomingItem = struct {
            value: IncomingType,
            from_user: context.UserId,
        };

        context_state: *context.Context,
        compression_method: compress.Method,
        io: std.Io,

        event_identifier: u16,
        pending_read_index: usize,
        pending_items: std.ArrayList(IncomingItem),
        disconnected_users: std.ArrayList(context.UserId),

        pub fn init(
            io: std.Io,
            context_state: *context.Context,
            event_identifier: u16,
            compression_method: compress.Method,
        ) @This() {
            return .{
                .io = io,
                .context_state = context_state,
                .compression_method = compression_method,

                .event_identifier = event_identifier,
                .pending_read_index = 0,

                .pending_items = .empty,
                .disconnected_users = .empty,
            };
        }

        pub fn deinit(self: *@This()) void {
            self.pending_items.deinit(self.context_state.allocator);
            self.disconnected_users.deinit(self.context_state.allocator);
        }

        pub fn next(self: *@This()) !?IncomingItem {
            if (self.pending_read_index < self.pending_items.items.len) {
                const current_item = self.pending_items.items[self.pending_read_index];

                self.pending_read_index += 1;

                return current_item;
            }

            self.pending_items.clearRetainingCapacity();
            self.pending_read_index = 0;

            for (self.disconnected_users.items) |disconnected_identifier| {
                try self.context_state.removeConnection(disconnected_identifier);
            }

            self.disconnected_users.clearRetainingCapacity();

            var packets = try self.context_state.pollReceived();

            defer packets.deinit(self.context_state.allocator);

            for (packets.items) |*packet_with_id| {
                defer packet_with_id.packet.deinit();

                if (packet_with_id.packet.event_identifier != self.event_identifier) continue;

                var payload_reader: std.Io.Reader = .fixed(packet_with_id.packet.payload);

                const deserialized_value = try serialize.deserializeValue(
                    IncomingType,
                    &payload_reader,
                    self.context_state.allocator,
                );

                try self.pending_items.append(self.context_state.allocator, .{
                    .value = deserialized_value,
                    .from_user = packet_with_id.user_identifier,
                });
            }

            if (self.pending_items.items.len == 0) return null;

            const first_item = self.pending_items.items[0];

            self.pending_read_index = 1;

            return first_item;
        }
    };
}
