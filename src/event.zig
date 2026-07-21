const state = @import("state.zig");
const wire = @import("wire.zig");
const root = @import("root.zig");
const std = @import("std");

fn sendValueTo(target_user: root.UserId, event_identifier: u16, comptime ValueType: type, value: ValueType) !void {
    try state.requireInitialized();

    const connection = state.findConnection(target_user) orelse return state.NetworkError.UnknownUser;

    try connection.queueSend(state.sharedAllocator(), event_identifier, ValueType, value);
}

fn broadcastValueAll(event_identifier: u16, comptime ValueType: type, value: ValueType) !void {
    try state.requireInitialized();

    var connection_iterator = state.connectionValues();

    while (connection_iterator.next()) |connection| {
        try connection.queueSend(state.sharedAllocator(), event_identifier, ValueType, value);
    }
}

fn broadcastValueExcept(excluded_user: root.UserId, event_identifier: u16, comptime ValueType: type, value: ValueType) !void {
    try state.requireInitialized();

    var connection_iterator = state.connectionValues();

    while (connection_iterator.next()) |connection| {
        if (connection.user_identifier == excluded_user) continue;

        try connection.queueSend(state.sharedAllocator(), event_identifier, ValueType, value);
    }
}

fn pollEventValues(comptime ValueType: type, from_user: root.UserId, event_identifier: u16) ![]ValueType {
    try state.requireInitialized();

    const packet_list = state.findIncoming(from_user) orelse return &.{};
    const allocator = state.sharedAllocator();

    var result: std.ArrayList(ValueType) = .empty;
    var write_index: usize = 0;

    for (packet_list.items) |*packet| {
        if (packet.event_identifier != event_identifier) {
            packet_list.items[write_index] = packet.*;
            write_index += 1;

            continue;
        }

        var payload_reader: std.Io.Reader = .fixed(packet.payload);
        const value = try wire.deserializeValue(ValueType, &payload_reader, allocator);

        try result.append(allocator, value);

        packet.deinit(allocator);
    }

    packet_list.shrinkRetainingCapacity(write_index);

    return try result.toOwnedSlice(allocator);
}

pub fn OutEvent(comptime ValueType: type) type {
    return struct {
        event_identifier: u16,

        pub fn init(event_identifier: u16) @This() {
            return .{ .event_identifier = event_identifier };
        }

        pub fn sendTo(self: @This(), target_user: root.UserId, value: ValueType) !void {
            try sendValueTo(target_user, self.event_identifier, ValueType, value);
        }

        pub fn broadcastAll(self: @This(), value: ValueType) !void {
            try broadcastValueAll(self.event_identifier, ValueType, value);
        }

        pub fn broadcastExcept(self: @This(), excluded_user: root.UserId, value: ValueType) !void {
            try broadcastValueExcept(excluded_user, self.event_identifier, ValueType, value);
        }
    };
}

pub fn Event(comptime ValueType: type) type {
    return struct {
        event_identifier: u16,

        pub fn init(event_identifier: u16) @This() {
            return .{ .event_identifier = event_identifier };
        }

        pub fn sendTo(self: @This(), target_user: root.UserId, value: ValueType) !void {
            try sendValueTo(target_user, self.event_identifier, ValueType, value);
        }

        pub fn broadcastAll(self: @This(), value: ValueType) !void {
            try broadcastValueAll(self.event_identifier, ValueType, value);
        }

        pub fn broadcastExcept(self: @This(), excluded_user: root.UserId, value: ValueType) !void {
            try broadcastValueExcept(excluded_user, self.event_identifier, ValueType, value);
        }

        pub fn poll(self: @This(), from_user: root.UserId) ![]ValueType {
            return pollEventValues(ValueType, from_user, self.event_identifier);
        }
    };
}
