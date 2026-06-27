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

    pub fn init(io: std.Io, stream: std.Io.net.Stream, user_identifier: UserId) Connection {
        return .{
            .io = io,
            .stream = stream,
            .user_identifier = user_identifier,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }

    pub fn close(self: *Connection) void {
        self.stream.close(self.io);
    }

    pub fn sendPacket(
        self: *Connection,
        event_identifier: u16,
        comptime PayloadType: type,
        payload_value: PayloadType,
        compression_method: compress.Method,
    ) !void {
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

        try stream_writer.interface.writeStruct(packet_header, .little);
        try stream_writer.interface.writeAll(compressed_bytes);
        try stream_writer.interface.flush();
    }

    pub fn receivePacket(self: *Connection, allocator: std.mem.Allocator) !ReceivedPacket {
        var stream_reader = self.stream.reader(self.io, &self.read_buffer);
        const packet_header = try stream_reader.interface.takeStruct(wire.WireHeader, .little);

        var compressed_payload_buffer: [wire.maximum_packet_payload_size]u8 = undefined;
        const compressed_payload = compressed_payload_buffer[0..packet_header.payload_length];

        try stream_reader.interface.readSliceAll(compressed_payload);

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
};

pub const ReceivedPacket = struct {
    event_identifier: u16,
    payload: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ReceivedPacket) void {
        self.allocator.free(self.payload);
    }
};
