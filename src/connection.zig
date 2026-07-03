const serialize = @import("serialize.zig");
const compress = @import("compress.zig");
const wire = @import("wire.zig");
const std = @import("std");

pub const UserId = u32;

pub const Connection = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    user_identifier: UserId,

    read_buffer: [8192]u8,
    write_buffer: [8192]u8,
    read_mutex: std.Io.Mutex,
    write_mutex: std.Io.Mutex,

    read_task: ?std.Io.Future(ReadResult) = null,
    write_task: ?std.Io.Future(WriteResult) = null,

    closed: bool = false,
    close_mutex: std.Io.Mutex,

    pub const ReadResult = struct {
        packet: ?ReceivedPacket = null,
        err: ?anyerror = null,
    };

    pub const WriteResult = struct {
        err: ?anyerror = null,
    };

    pub fn init(io: std.Io, stream: std.Io.net.Stream, user_identifier: UserId) Connection {
        return .{
            .io = io,
            .stream = stream,
            .user_identifier = user_identifier,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .read_mutex = .init,
            .write_mutex = .init,
            .close_mutex = .init,
        };
    }

    pub fn close(self: *Connection) !void {
        {
            try self.close_mutex.lock(self.io);
            defer self.close_mutex.unlock(self.io);

            if (self.closed) return;

            self.closed = true;
        }

        if (self.read_task) |*task| {
            _ = task.cancel(self.io);

            self.read_task = null;
        }

        if (self.write_task) |*task| {
            _ = task.cancel(self.io);

            self.write_task = null;
        }

        self.stream.close(self.io);
    }

    pub fn sendPacketAsync(
        self: *Connection,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.Method,
    ) !void {
        {
            try self.write_mutex.lock(self.io);
            defer self.write_mutex.unlock(self.io);

            if (self.write_task != null) return error.WriteInProgress;
        }

        const SendContext = struct {
            connection: *Connection,
            event_id: u16,
            payload: PayloadType,
            method: compress.Method,

            pub fn run(context: @This()) WriteResult {
                context.connection.sendPacketBlocking(
                    context.event_id,
                    PayloadType,
                    context.payload,
                    context.method,
                ) catch |error_value| {
                    return .{ .err = error_value };
                };

                return .{};
            }
        };

        self.write_task = self.io.async(SendContext.run, .{.{
            .connection = self,
            .event_id = event_identifier,
            .payload = payload_value,
            .method = compression_method,
        }});
    }

    pub fn awaitWrite(self: *Connection) !void {
        var task = self.write_task orelse return;
        const result = task.await(self.io);

        defer self.write_task = null;

        if (result.err) |error_value| return error_value;
    }

    fn sendPacketBlocking(
        self: *Connection,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.Method,
    ) !void {
        try self.write_mutex.lock(self.io);
        defer self.write_mutex.unlock(self.io);

        {
            try self.close_mutex.lock(self.io);
            defer self.close_mutex.unlock(self.io);
            if (self.closed) return error.ConnectionClosed;
        }

        var serialized_buffer: [wire.maximum_packet_payload_size]u8 = undefined;
        var serialized_writer: std.Io.Writer = .fixed(&serialized_buffer);
        try serialize.serializeValue(PayloadType, payload_value, &serialized_writer);

        const serialized_bytes = serialized_writer.buffered();
        var compressed_buffer: [wire.maximum_packet_payload_size]u8 = undefined;

        const compressed_length = compress.compressWithMethod(
            compression_method,
            serialized_bytes,
            &compressed_buffer,
        );

        const compressed_bytes = compressed_buffer[0..compressed_length];

        const packet_header = wire.WireHeader{
            .event_identifier = event_identifier,
            .payload_length = @intCast(compressed_length),
            .flags = wire.methodToFlags(compression_method),
        };

        var stream_writer = self.stream.writer(self.io, &self.write_buffer);

        // Wrap all stream operations with disconnect-aware error handling
        stream_writer.interface.writeStruct(packet_header, .little) catch |err| {
            switch (err) {
                error.Unexpected, error.ConnectionResetByPeer, error.BrokenPipe, error.NotConnected, error.WouldBlock => return error.ConnectionClosed,
                else => return err,
            }
        };

        stream_writer.interface.writeAll(compressed_bytes) catch |err| {
            switch (err) {
                error.Unexpected,
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                error.NotConnected,
                error.WouldBlock,
                => return error.ConnectionClosed,
                else => return err,
            }
        };

        stream_writer.interface.flush() catch |err| {
            switch (err) {
                error.Unexpected,
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                error.NotConnected,
                error.WouldBlock,
                => return error.ConnectionClosed,
                else => return err,
            }
        };
    }

    pub fn receivePacketAsync(self: *Connection, allocator: std.mem.Allocator) !void {
        {
            try self.read_mutex.lock(self.io);
            defer self.read_mutex.unlock(self.io);

            if (self.read_task != null) return error.ReadInProgress;
        }

        const ReadContext = struct {
            connection: *Connection,
            alloc: std.mem.Allocator,

            pub fn run(context: @This()) ReadResult {
                const packet = context.connection.receivePacketBlocking(context.alloc) catch |error_value| {
                    return .{ .err = error_value };
                };

                return .{ .packet = packet };
            }
        };

        self.read_task = self.io.async(ReadContext.run, .{.{
            .connection = self,
            .alloc = allocator,
        }});
    }

    pub fn awaitRead(self: *Connection) !ReceivedPacket {
        var task = self.read_task orelse return error.NoReadPending;
        const result = task.await(self.io);

        defer self.read_task = null;

        if (result.err) |error_value| return error_value;

        return result.packet.?;
    }

    pub fn tryAwaitRead(self: *Connection) ?ReceivedPacket {
        if (self.read_task == null) return null;
        
        var task: std.Io.Future(ReadResult) = self.read_task.?;
        
        self.read_task = null;
        
        const result = task.await(self.io);
        
        if (result.err) |error_value| {
            std.log.err("[netling] read failed: {}", .{error_value});

            return null;
        }
        
        return result.packet;
    }

    fn receivePacketBlocking(self: *Connection, allocator: std.mem.Allocator) !ReceivedPacket {
        try self.read_mutex.lock(self.io);
        defer self.read_mutex.unlock(self.io);

        {
            try self.close_mutex.lock(self.io);
            defer self.close_mutex.unlock(self.io);
            if (self.closed) return error.ConnectionClosed;
        }

        var stream_reader = self.stream.reader(self.io, &self.read_buffer);

        const packet_header = stream_reader.interface.takeStruct(wire.WireHeader, .little) catch |err| {
            switch (err) {
                error.Unexpected,
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                error.NotConnected,
                error.WouldBlock,
                => return error.ConnectionClosed,
                else => return err,
            }
        };

        var compressed_payload_buffer: [wire.maximum_packet_payload_size]u8 = undefined;
        const compressed_payload = compressed_payload_buffer[0..packet_header.payload_length];

        stream_reader.interface.readSliceAll(compressed_payload) catch |err| {
            switch (err) {
                error.Unexpected,
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                error.NotConnected,
                error.WouldBlock,
                => return error.ConnectionClosed,
                else => return err,
            }
        };

        const decompression_method = wire.flagsToMethod(packet_header.flags);
        var decompressed_buffer: [wire.maximum_packet_payload_size]u8 = undefined;

        const decompressed_length = compress.decompressWithMethod(
            decompression_method,
            compressed_payload,
            &decompressed_buffer,
        );

        const owned_payload = try allocator.dupe(u8, decompressed_buffer[0..decompressed_length]);

        return .{
            .event_identifier = packet_header.event_identifier,
            .payload = owned_payload,
            .allocator = allocator,
        };
    }

    pub fn sendPacket(
        self: *Connection,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.Method,
    ) !void {
        try self.sendPacketAsync(event_identifier, PayloadType, payload_value, compression_method);
        try self.awaitWrite();
    }

    pub fn receivePacket(self: *Connection, allocator: std.mem.Allocator) !ReceivedPacket {
        try self.receivePacketAsync(allocator);

        return try self.awaitRead();
    }
};

pub const ReceivedPacket = struct {
    event_identifier: u16,
    payload: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ReceivedPacket) void {
        self.allocator.free(self.payload);
    }
};
