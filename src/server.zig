const context = @import("context.zig");
const std = @import("std");

pub const Server = struct {
    io: std.Io,
    context_state: context.Context,
    listening_server: std.Io.net.Server,

    pub fn init(
        io: std.Io,
        allocator: std.mem.Allocator,
        bind_address: std.Io.net.IpAddress,
    ) !Server {
        const listening_server = try bind_address.listen(io, .{ .reuse_address = true });

        return .{
            .io = io,
            .context_state = context.Context.init(io, allocator),
            .listening_server = listening_server,
        };
    }

    pub fn deinit(self: *Server) void {
        self.listening_server.deinit(self.io);
        self.context_state.deinit();
    }

    pub fn acceptConnection(self: *Server) !context.UserId {
        const accepted_stream = try self.listening_server.accept(self.io);

        return self.context_state.addConnection(accepted_stream);
    }
};
