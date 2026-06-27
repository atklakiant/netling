const context = @import("context.zig");
const std = @import("std");

pub const Client = struct {
    io: std.Io,
    context_state: context.Context,
    server_user_identifier: context.UserId,

    pub fn connect(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !Client {
        const connected_stream = try server_address.connect(io, .{ .mode = .stream });
        var new_context = context.Context.init(io, allocator);
        const assigned_identifier = try new_context.addConnection(connected_stream);

        return .{
            .io = io,
            .context_state = new_context,
            .server_user_identifier = assigned_identifier,
        };
    }

    pub fn deinit(self: *Client) void {
        self.context_state.deinit();
    }

    pub fn serverIdentifier(self: *Client) context.UserId {
        return self.server_user_identifier;
    }
};
