const serialize = @import("serialize.zig");
const compress = @import("compress.zig");
const context = @import("context.zig");
const std = @import("std");

var global_event_counter: u16 = 0;

fn allocateEventIdentifier() u16 {
    global_event_counter += 1;

    return global_event_counter;
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
        compression_method: compress.Method,

        pub fn init(compression_method: compress.Method) @This() {
            return .{
                .event_identifier = allocateEventIdentifier(),
                .compression_method = compression_method,
            };
        }

        pub fn iterator(
            self: *const @This(),
            context_state: *context.Context,
            comptime role: Role,
        ) EventIterator(IncomingType, OutgoingType, role) {
            return EventIterator(IncomingType, OutgoingType, role).init(
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
            const target_connection = context_state.getConnection(target_user_identifier) orelse return error.UnknownUser;

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
    };
}

pub fn EventIterator(comptime IncomingType: type) type {
    return struct {
        pub const IncomingItem = struct {
            value: IncomingType,
            from_user: context.UserId,
        };

        context_state: *context.Context,
        event_identifier: u16,
        compression_method: compress.Method,
        pending_items: std.ArrayList(IncomingItem),
        pending_read_index: usize,
        disconnected_users: std.ArrayList(context.UserId),

        pub fn init(
            context_state: *context.Context,
            event_identifier: u16,
            compression_method: compress.Method,
        ) @This() {
            return .{
                .context_state = context_state,
                .event_identifier = event_identifier,
                .compression_method = compression_method,

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
                self.context_state.removeConnection(disconnected_identifier);
            }

            self.disconnected_users.clearRetainingCapacity();

            var connection_iterator = self.context_state.connections.iterator();

            while (connection_iterator.next()) |connection_entry| {
                const user_identifier = connection_entry.key_ptr.*;
                const existing_connection = connection_entry.value_ptr;

                var received_packet = existing_connection.receivePacket(
                    self.context_state.allocator,
                ) catch |receive_error| switch (receive_error) {
                    error.WouldBlock, error.EndOfStream => continue,
                    else => {
                        try self.disconnected_users.append(self.context_state.allocator, user_identifier);
                        continue;
                    },
                };

                defer received_packet.deinit();

                if (received_packet.event_identifier != self.event_identifier) continue;

                var payload_reader: std.Io.Reader = .fixed(received_packet.payload);

                const deserialized_value = try serialize.deserializeValue(
                    IncomingType,
                    &payload_reader,
                    self.context_state.allocator,
                );

                try self.pending_items.append(self.context_state.allocator, .{
                    .value = deserialized_value,
                    .from_user = user_identifier,
                });
            }

            if (self.pending_items.items.len == 0) return null;

            const first_item = self.pending_items.items[0];

            self.pending_read_index = 1;

            return first_item;
        }
    };
}
