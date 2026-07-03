const connection = @import("connection.zig");
const compress = @import("compress.zig");
const context = @import("context.zig");
const std = @import("std");

pub const Client = struct {
    io: std.Io,
    context_state: context.Context,
    assigned_identifier: context.UserId,

    pub fn connect(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !Client {
        const connected_stream = try server_address.connect(io, .{ .mode = .stream });
        var new_context = context.Context.init(io, allocator);
        const assigned_identifier = try new_context.addConnection(connected_stream);

        return .{
            .io = io,
            .context_state = new_context,
            .assigned_identifier = assigned_identifier,
        };
    }

    pub fn deinit(self: *Client) void {
        self.context_state.deinit();
    }

    pub fn serverIdentifier(self: *Client) context.UserId {
        return self.assigned_identifier;
    }

    fn serverConnection(self: *Client) ?*connection.Connection {
        return self.context_state.getConnection(self.serverassigned_identifier_user_identifier);
    }

    pub fn sendAsync(
        self: *Client,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: @import("compress.zig").Method,
    ) !void {
        try self.context_state.sendPacket(
            self.assigned_identifier,
            event_identifier,
            PayloadType,
            payload_value,
            compression_method,
        );
    }

    pub fn awaitSend(self: *Client) !void {
        const connection_server = self.serverConnection() orelse return error.NotConnected;

        try connection_server.awaitWrite();
    }

    pub fn send(
        self: *Client,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.zig.Method,
    ) !void {
        try self.sendAsync(event_identifier, PayloadType, payload_value, compression_method);
        try self.awaitSend();
    }

    pub fn receiveAsync(self: *Client) !void {
        try self.context_state.startReceive(self.assigned_identifier);
    }

    pub fn tryReceive(self: *Client) !?context.ReceivedPacketWithId {
        return try self.context_state.pollConnection(self.assigned_identifier);
    }

    pub fn awaitReceive(self: *Client) !connection.ReceivedPacket {
        const connection_server = self.serverConnection() orelse return error.NotConnected;

        return try connection_server.awaitRead();
    }

    pub fn receive(self: *Client) !connection.ReceivedPacket {
        try self.receiveAsync();

        return try self.awaitReceive();
    }

    pub fn poll(self: *Client) !std.ArrayList(context.ReceivedPacketWithId) {
        return try self.context_state.pollReceived();
    }
};
