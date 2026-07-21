const state = @import("state.zig");
const event = @import("event.zig");
const std = @import("std");

pub const OutEvent = event.OutEvent;
pub const Event = event.Event;
pub const UserId = u32;

pub const NetworkError = error{
    AlreadyInitialized,
    NotInitialized,
    UnknownUser,
    NotAServer,
    NotAClient,
    ConnectionClosed,
    WriteInProgress,
    ReadInProgress,
    AcceptInProgress,
};

pub fn initServer(io: std.Io, allocator: std.mem.Allocator, bind_address: std.Io.net.IpAddress) !void {
    return state.initServer(io, allocator, bind_address);
}

pub fn initClient(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !UserId {
    return state.initClient(io, allocator, server_address);
}

pub fn shutdown() void {
    state.shutdown();
}

pub fn poll() !void {
    state.poll();
}

pub fn getConnectedCount() usize {
    return state.getConnectedCount();
}

pub fn isUserConnected(user_identifier: UserId) bool {
    return state.isConnected(user_identifier);
}

pub fn isServer() bool {
    return state.isServerRole();
}

pub fn isClient() bool {
    return state.isClientRole();
}
