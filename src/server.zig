const context = @import("context.zig");
const std = @import("std");

pub const Server = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    context_state: context.Context,
    listening_server: std.Io.net.Server,

    accept_task: ?std.Io.Future(AcceptResult) = null,

    pub const AcceptResult = struct {
        stream: ?std.Io.net.Stream = null,
        err: ?anyerror = null,
    };

    pub fn init(
        io: std.Io,
        allocator: std.mem.Allocator,
        bind_address: std.Io.net.IpAddress,
    ) !Server {
        const listening_server = try bind_address.listen(io, .{ .reuse_address = true });

        return .{
            .io = io,
            .allocator = allocator,
            .context_state = context.Context.init(io, allocator),
            .listening_server = listening_server,
            .accept_task = null,
        };
    }

    pub fn deinit(self: *Server) !void {
        if (self.accept_task) |*task| {
            _ = task.cancel(self.io);

            self.accept_task = null;
        }

        self.listening_server.deinit(self.io);

        try self.context_state.deinit();
    }

    pub fn acceptAsync(self: *Server) !void {
        if (self.accept_task != null) return error.AcceptAlreadyPending;

        const AcceptContext = struct {
            server: *std.Io.net.Server,
            io: std.Io,

            pub fn run(accept_context: @This()) AcceptResult {
                const stream = accept_context.server.accept(accept_context.io) catch |error_value| {
                    return .{ .err = error_value };
                };

                return .{ .stream = stream };
            }
        };

        self.accept_task = self.io.async(AcceptContext.run, .{.{
            .server = &self.listening_server,
            .io = self.io,
        }});
    }

    pub fn tryAccept(self: *Server) !?context.UserId {
        const task = self.accept_task orelse return null;

        if (task.tryAwait(self.io)) |result| {
            self.accept_task = null;

            if (result.err) |error_value| return error_value;

            const new_stream = result.stream.?;

            return try self.context_state.addConnection(new_stream);
        }

        return null;
    }

    pub fn awaitAccept(self: *Server) !context.UserId {
        var task = self.accept_task orelse return error.NoAcceptPending;

        defer self.accept_task = null;

        const result = task.await(self.io);

        if (result.err) |error_value| return error_value;

        const new_stream = result.stream.?;

        return try self.context_state.addConnection(new_stream);
    }

    pub fn acceptConnection(self: *Server) !context.UserId {
        try self.acceptAsync();

        return try self.awaitAccept();
    }

    pub fn tick(self: *Server) !ServerTickResult {
        var result = ServerTickResult.init(self.allocator);

        errdefer result.deinit();

        if (try self.tryAccept()) |user_id| {
            try result.new_connections.append(user_id);
        }

        var packets = try self.context_state.pollReceived();

        defer packets.deinit();
        try result.packets.appendSlice(packets.items);

        return result;
    }

    pub fn startReceives(self: *Server) !void {
        var iterator = self.context_state.connections.keyIterator();

        while (iterator.next()) |user_id| {
            try self.context_state.startReceive(user_id.*);
        }
    }

    pub fn sendTo(
        self: *Server,
        user_identifier: context.UserId,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: @import("compress.zig").Method,
    ) !void {
        try self.context_state.sendPacket(
            user_identifier,
            event_identifier,
            PayloadType,
            payload_value,
            compression_method,
        );
    }

    pub fn awaitSendTo(self: *Server, user_identifier: context.UserId) !void {
        const maybe_connection = try self.context_state.getConnection(user_identifier);
        const connection = maybe_connection orelse return error.UnknownUser;

        try connection.awaitWrite();
    }

    pub fn disconnect(self: *Server, user_identifier: context.UserId) !void {
        try self.context_state.removeConnection(user_identifier);
    }
};

pub const ServerTickResult = struct {
    allocator: std.mem.Allocator,
    new_connections: std.ArrayList(context.UserId),
    packets: std.ArrayList(context.ReceivedPacketWithId),

    pub fn init(allocator: std.mem.Allocator) ServerTickResult {
        return .{
            .allocator = allocator,
            .new_connections = .empty,
            .packets = .empty,
        };
    }

    pub fn deinit(self: *ServerTickResult) void {
        for (self.packets.items) |*packet| {
            packet.deinit(self.allocator);
        }

        self.packets.deinit(self.allocator);
        self.new_connections.deinit(self.allocator);
    }
};
