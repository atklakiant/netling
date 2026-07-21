const wire = @import("wire.zig");
const root = @import("root.zig");
const std = @import("std");

pub const ReadOutcome = struct {
    packet: ?RawPacket = null,
    error_value: ?anyerror = null,
};

pub const WriteOutcome = struct {
    error_value: ?anyerror = null,
};

pub const RawPacket = struct {
    event_identifier: u16,
    payload: []u8,

    pub fn deinit(self: *RawPacket, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }
};

const QueuedWrite = struct {
    event_identifier: u16,
    payload: []u8,
};

const ReceiveContext = struct {
    connection: *PeerConnection,

    io: std.Io,
    allocator: std.mem.Allocator,

    fn run(context: @This()) ReadOutcome {
        defer context.connection.read_done.store(true, .release);

        const packet = context.connection.receiveBlocking(
            context.io,
            context.allocator,
        ) catch |error_value| return .{ .error_value = error_value };

        return .{ .packet = packet };
    }
};

const SendContext = struct {
    connection: *PeerConnection,
    queued_write: QueuedWrite,

    io: std.Io,
    allocator: std.mem.Allocator,

    fn run(context: @This()) WriteOutcome {
        defer context.allocator.free(context.queued_write.payload);
        defer context.connection.write_done.store(true, .release);

        context.connection.sendBlocking(
            context.io,
            context.queued_write.event_identifier,
            context.queued_write.payload,
        ) catch |error_value| {
            return .{ .error_value = error_value };
        };

        return .{};
    }
};

pub const PeerConnection = struct {
    stream: std.Io.net.Stream,
    user_identifier: root.UserId,

    read_buffer: [8192]u8 = undefined,
    write_buffer: [8192]u8 = undefined,

    reader: ?std.Io.net.Stream.Reader = null,
    writer: ?std.Io.net.Stream.Writer = null,

    read_task: ?std.Io.Future(ReadOutcome) = null,
    read_done: std.atomic.Value(bool) = .init(false),

    write_queue: std.ArrayList(QueuedWrite) = .empty,
    write_task: ?std.Io.Future(WriteOutcome) = null,
    write_done: std.atomic.Value(bool) = .init(false),

    closed: bool = false,

    pub fn startReceiveIfIdle(self: *PeerConnection, io: std.Io, allocator: std.mem.Allocator) void {
        if (self.read_task != null or self.closed) return;

        self.read_done.store(false, .release);

        self.read_task = io.async(ReceiveContext.run, .{.{
            .connection = self,
            .io = io,
            .allocator = allocator,
        }});
    }

    pub fn drainReadTasks(self: *PeerConnection, io: std.Io, allocator: std.mem.Allocator, out_results: *std.ArrayList(ReadOutcome)) !void {
        while (self.read_task != null and self.read_done.load(.acquire)) {
            var task = self.read_task.?;

            self.read_task = null;

            try out_results.append(allocator, task.await(io));

            self.startReceiveIfIdle(io, allocator);
        }
    }

    fn receiveBlocking(self: *PeerConnection, io: std.Io, allocator: std.mem.Allocator) !RawPacket {
        if (self.reader == null) self.reader = self.stream.reader(io, &self.read_buffer);

        var stream_reader = &self.reader.?;
        const header = stream_reader.interface.takeStruct(wire.WireHeader, .little) catch return root.NetworkError.ConnectionClosed;

        if (header.payload_length > wire.maximum_payload_size) return root.NetworkError.ConnectionClosed;

        var compressed_buffer: [wire.maximum_payload_size]u8 = undefined;
        const compressed_slice = compressed_buffer[0..header.payload_length];

        stream_reader.interface.readSliceAll(compressed_slice) catch return root.NetworkError.ConnectionClosed;

        var decompressed_buffer: [wire.maximum_payload_size]u8 = undefined;
        const decompressed_length = wire.decompress(compressed_slice, &decompressed_buffer);

        return .{
            .event_identifier = header.event_identifier,
            .payload = try allocator.dupe(u8, decompressed_buffer[0..decompressed_length]),
        };
    }

    pub fn queueSend(
        self: *PeerConnection,
        allocator: std.mem.Allocator,
        event_identifier: u16,
        comptime ValueType: type,
        value: ValueType,
    ) !void {
        var serialized_buffer: [wire.maximum_payload_size]u8 = undefined;
        var buffer_writer: std.Io.Writer = .fixed(&serialized_buffer);

        try wire.serializeValue(ValueType, value, &buffer_writer);

        try self.write_queue.append(allocator, .{
            .event_identifier = event_identifier,
            .payload = try allocator.dupe(u8, buffer_writer.buffered()),
        });
    }

    pub fn pumpWrites(self: *PeerConnection, io: std.Io, allocator: std.mem.Allocator) void {
        if (self.closed) return;

        if (self.write_task != null) {
            if (!self.write_done.load(.acquire)) return;

            var task = self.write_task.?;

            self.write_task = null;

            const outcome = task.await(io);

            if (outcome.error_value) |error_value| std.log.err("[netling] write failed: {}", .{error_value});
        }

        if (self.write_queue.items.len == 0) return;

        const queued_write = self.write_queue.orderedRemove(0);

        self.write_done.store(false, .release);

        self.write_task = io.async(SendContext.run, .{.{
            .connection = self,
            .queued_write = queued_write,
            .io = io,
            .allocator = allocator,
        }});
    }

    fn sendBlocking(self: *PeerConnection, io: std.Io, event_identifier: u16, serialized: []const u8) !void {
        var compressed_buffer: [wire.maximum_payload_size]u8 = undefined;
        const compressed_length = wire.compress(serialized, &compressed_buffer);

        const header: wire.WireHeader = .{
            .event_identifier = event_identifier,
            .payload_length = @intCast(compressed_length),
        };

        if (self.writer == null) self.writer = self.stream.writer(io, &self.write_buffer);

        var stream_writer = &self.writer.?;

        stream_writer.interface.writeStruct(header, .little) catch return root.NetworkError.ConnectionClosed;
        stream_writer.interface.writeAll(compressed_buffer[0..compressed_length]) catch return root.NetworkError.ConnectionClosed;
        stream_writer.interface.flush() catch return root.NetworkError.ConnectionClosed;
    }

    pub fn close(self: *PeerConnection, io: std.Io, allocator: std.mem.Allocator) void {
        if (self.closed) return;

        self.closed = true;

        if (self.read_task) |*task| _ = task.cancel(io);
        if (self.write_task) |*task| _ = task.cancel(io);

        self.reader = null;
        self.writer = null;

        for (self.write_queue.items) |item| allocator.free(item.payload);

        self.write_queue.deinit(allocator);
        self.stream.close(io);
    }
};
