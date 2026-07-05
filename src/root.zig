const std = @import("std");

pub const UserId = u32;

pub const CompressionMethod = enum {
    none,
    rle,
    bitpack,
    both,
};

pub const NetError = error{
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

const PacketFlags = packed struct(u8) {
    rle_compressed: bool = false,
    bitpacked: bool = false,
    padding: u6 = 0,
};

const WireHeader = extern struct {
    event_identifier: u16,
    payload_length: u32,
    flags: PacketFlags,
};

const maximum_payload_size: usize = 65536;

fn compressionToFlags(method: CompressionMethod) PacketFlags {
    return switch (method) {
        .none => .{},
        .rle => .{ .rle_compressed = true },
        .bitpack => .{ .bitpacked = true },
        .both => .{ .rle_compressed = true, .bitpacked = true },
    };
}

fn flagsToCompression(flags: PacketFlags) CompressionMethod {
    if (flags.rle_compressed and flags.bitpacked) return .both;
    if (flags.rle_compressed) return .rle;
    if (flags.bitpacked) return .bitpack;
    return .none;
}

const RunLength = struct {
    fn encode(input: []const u8, output: []u8) usize {
        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index < input.len) {
            const current_byte = input[input_index];
            var run_length: usize = 1;

            while (input_index + run_length < input.len and input[input_index + run_length] == current_byte and run_length < 255) run_length += 1;

            if (run_length >= 3 or current_byte == 0xFE) {
                if (output_index + 3 > output.len) break;
                output[output_index] = 0xFE;
                output[output_index + 1] = current_byte;
                output[output_index + 2] = @intCast(run_length);
                output_index += 3;
            } else {
                for (0..run_length) |_| {
                    if (output_index >= output.len) break;
                    output[output_index] = current_byte;
                    output_index += 1;
                }
            }

            input_index += run_length;
        }

        return output_index;
    }

    fn decode(input: []const u8, output: []u8) usize {
        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index < input.len) {
            if (input[input_index] == 0xFE and input_index + 2 < input.len) {
                const run_byte = input[input_index + 1];
                const run_count = input[input_index + 2];

                for (0..run_count) |_| {
                    if (output_index >= output.len) break;
                    output[output_index] = run_byte;
                    output_index += 1;
                }

                input_index += 3;
            } else {
                if (output_index >= output.len) break;
                output[output_index] = input[input_index];
                output_index += 1;
                input_index += 1;
            }
        }

        return output_index;
    }
};

const Bitpack = struct {
    fn encode(input: []const u8, output: []u8) usize {
        if (input.len % 4 != 0) {
            const copy_length = @min(input.len, output.len);
            @memcpy(output[0..copy_length], input[0..copy_length]);
            return copy_length;
        }

        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index + 4 <= input.len) {
            const value = std.mem.readInt(u32, input[input_index..][0..4], .little);
            const needed_bytes: u8 = if (value <= 0xFF) 1 else if (value <= 0xFFFF) 2 else if (value <= 0xFFFFFF) 3 else 4;

            if (output_index + 1 + needed_bytes > output.len) break;

            output[output_index] = needed_bytes;
            output_index += 1;

            for (0..needed_bytes) |byte_index| {
                output[output_index] = @intCast((value >> @intCast(byte_index * 8)) & 0xFF);
                output_index += 1;
            }

            input_index += 4;
        }

        return output_index;
    }

    fn decode(input: []const u8, output: []u8) usize {
        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index < input.len) {
            if (input_index + 1 > input.len) break;

            const needed_bytes = input[input_index];
            input_index += 1;

            if (needed_bytes == 0 or needed_bytes > 4) break;
            if (input_index + needed_bytes > input.len) break;
            if (output_index + 4 > output.len) break;

            var value: u32 = 0;
            for (0..needed_bytes) |byte_index| value |= @as(u32, input[input_index + byte_index]) << @intCast(byte_index * 8);

            input_index += needed_bytes;

            std.mem.writeInt(u32, output[output_index..][0..4], value, .little);
            output_index += 4;
        }

        return output_index;
    }
};

fn compressWithMethod(method: CompressionMethod, input: []const u8, output: []u8) usize {
    return switch (method) {
        .none => blk: {
            const copy_length = @min(input.len, output.len);
            @memcpy(output[0..copy_length], input[0..copy_length]);
            break :blk copy_length;
        },
        .rle => RunLength.encode(input, output),
        .bitpack => Bitpack.encode(input, output),
        .both => blk: {
            var middle_buffer: [maximum_payload_size]u8 = undefined;
            const middle_length = RunLength.encode(input, &middle_buffer);
            break :blk Bitpack.encode(middle_buffer[0..middle_length], output);
        },
    };
}

fn decompressWithMethod(method: CompressionMethod, input: []const u8, output: []u8) usize {
    return switch (method) {
        .none => blk: {
            const copy_length = @min(input.len, output.len);
            @memcpy(output[0..copy_length], input[0..copy_length]);
            break :blk copy_length;
        },
        .rle => RunLength.decode(input, output),
        .bitpack => Bitpack.decode(input, output),
        .both => blk: {
            var middle_buffer: [maximum_payload_size]u8 = undefined;
            const middle_length = Bitpack.decode(input, &middle_buffer);
            break :blk RunLength.decode(middle_buffer[0..middle_length], output);
        },
    };
}

fn serializeValue(comptime ValueType: type, value: ValueType, writer: *std.Io.Writer) !void {
    switch (@typeInfo(ValueType)) {
        .void => {},
        .bool => try writer.writeByte(if (value) 1 else 0),
        .int => try writer.writeInt(ValueType, value, .little),
        .float => try writer.writeAll(std.mem.asBytes(&value)),
        .@"struct" => |struct_info| {
            if (@hasDecl(ValueType, "netlingSerialize")) {
                try value.netlingSerialize(writer);
            } else {
                inline for (struct_info.fields) |field| {
                    try serializeValue(field.type, @field(value, field.name), writer);
                }
            }
        },
        .@"enum" => |enum_info| try writer.writeInt(enum_info.tag_type, @intFromEnum(value), .little),
        .array => |array_info| for (value) |element| try serializeValue(array_info.child, element, writer),
        .pointer => |pointer_info| {
            if (pointer_info.size == .Slice and pointer_info.child == u8) {
                try writer.writeInt(u32, @intCast(value.len), .little);
                try writer.writeAll(value);
            } else @compileError("[netling] unsupported pointer type: " ++ @typeName(ValueType));
        },
        .optional => |optional_info| {
            if (value) |inner_value| {
                try writer.writeByte(1);
                try serializeValue(optional_info.child, inner_value, writer);
            } else try writer.writeByte(0);
        },
        else => @compileError("[netling] unsupported type: " ++ @typeName(ValueType)),
    }
}

fn deserializeValue(comptime ValueType: type, reader: *std.Io.Reader, allocator: std.mem.Allocator) !ValueType {
    switch (@typeInfo(ValueType)) {
        .void => return {},
        .bool => return (try reader.takeByte()) != 0,
        .int => return reader.takeInt(ValueType, .little),
        .float => return std.mem.bytesToValue(ValueType, try reader.takeArray(@sizeOf(ValueType))),
        .@"struct" => |struct_info| {
            if (@hasDecl(ValueType, "netlingDeserialize")) return ValueType.netlingDeserialize(reader, allocator);

            var result: ValueType = undefined;
            inline for (struct_info.fields) |field| {
                @field(result, field.name) = try deserializeValue(field.type, reader, allocator);
            }
            return result;
        },
        .@"enum" => |enum_info| return @enumFromInt(try reader.takeInt(enum_info.tag_type, .little)),
        .array => |array_info| {
            var result: ValueType = undefined;
            for (&result) |*element| element.* = try deserializeValue(array_info.child, reader, allocator);
            return result;
        },
        .pointer => |pointer_info| {
            if (pointer_info.size == .Slice and pointer_info.child == u8) {
                const slice_length = try reader.takeInt(u32, .little);
                const buffer = try allocator.alloc(u8, slice_length);
                try reader.readSliceAll(buffer);
                return buffer;
            } else @compileError("[netling] unsupported pointer type: " ++ @typeName(ValueType));
        },
        .optional => |optional_info| {
            if ((try reader.takeByte()) != 0) return try deserializeValue(optional_info.child, reader, allocator);
            return null;
        },
        else => @compileError("[netling] unsupported type: " ++ @typeName(ValueType)),
    }
}

const RawPacket = struct {
    event_identifier: u16,
    payload: []u8,

    fn deinit(self: *RawPacket) void {
        global_state.allocator.free(self.payload);
    }
};

const PeerConnection = struct {
    stream: std.Io.net.Stream,
    user_identifier: UserId,

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

    const ReadOutcome = struct { packet: ?RawPacket = null, err: ?anyerror = null };
    const WriteOutcome = struct { err: ?anyerror = null };

    const QueuedWrite = struct {
        event_identifier: u16,
        payload: []u8,
        method: CompressionMethod,
    };

    fn startReceiveIfIdle(self: *PeerConnection) void {
        if (self.read_task != null or self.closed) return;

        const ReceiveContext = struct {
            connection: *PeerConnection,

            fn run(context: @This()) ReadOutcome {
                defer context.connection.read_done.store(true, .release);

                const packet = context.connection.receiveBlocking() catch |err| return .{ .err = err };
                return .{ .packet = packet };
            }
        };

        self.read_done.store(false, .release);
        self.read_task = global_state.io.async(ReceiveContext.run, .{.{ .connection = self }});
    }

    fn drainReadTasks(self: *PeerConnection, out_results: *std.ArrayList(ReadOutcome)) !void {
        while (self.read_task != null and self.read_done.load(.acquire)) {
            var task = self.read_task.?;
            self.read_task = null;

            try out_results.append(global_state.allocator, task.await(global_state.io));

            self.startReceiveIfIdle();
        }
    }

    fn receiveBlocking(self: *PeerConnection) !RawPacket {
        if (self.reader == null) {
            self.reader = self.stream.reader(global_state.io, &self.read_buffer);
        }
        var stream_reader = &self.reader.?;

        const header = stream_reader.interface.takeStruct(WireHeader, .little) catch return NetError.ConnectionClosed;

        if (header.payload_length > maximum_payload_size)
            return NetError.ConnectionClosed;

        var compressed_buffer: [maximum_payload_size]u8 = undefined;
        const compressed_slice = compressed_buffer[0..header.payload_length];
        stream_reader.interface.readSliceAll(compressed_slice) catch return NetError.ConnectionClosed;

        var decompressed_buffer: [maximum_payload_size]u8 = undefined;
        const decompressed_length = decompressWithMethod(flagsToCompression(header.flags), compressed_slice, &decompressed_buffer);

        return .{
            .event_identifier = header.event_identifier,
            .payload = try global_state.allocator.dupe(u8, decompressed_buffer[0..decompressed_length]),
        };
    }

    fn queueSend(self: *PeerConnection, event_identifier: u16, comptime ValueType: type, value: ValueType, method: CompressionMethod) !void {
        var serialized_buffer: [maximum_payload_size]u8 = undefined;
        var buffer_writer: std.Io.Writer = .fixed(&serialized_buffer);
        try serializeValue(ValueType, value, &buffer_writer);

        try self.write_queue.append(global_state.allocator, .{
            .event_identifier = event_identifier,
            .payload = try global_state.allocator.dupe(u8, buffer_writer.buffered()),
            .method = method,
        });
    }

    fn pumpWrites(self: *PeerConnection) void {
        if (self.closed) return;

        if (self.write_task != null) {
            if (!self.write_done.load(.acquire)) return;

            var task = self.write_task.?;
            self.write_task = null;

            const outcome = task.await(global_state.io);
            if (outcome.err) |err| std.log.err("[netling] write failed: {}", .{err});
        }

        if (self.write_queue.items.len == 0) return;

        const queued_write = self.write_queue.orderedRemove(0);

        const SendContext = struct {
            connection: *PeerConnection,
            queued_write: QueuedWrite,

            fn run(context: @This()) WriteOutcome {
                defer global_state.allocator.free(context.queued_write.payload);
                defer context.connection.write_done.store(true, .release);

                context.connection.sendBlocking(context.queued_write.event_identifier, context.queued_write.payload, context.queued_write.method) catch |err| {
                    return .{ .err = err };
                };

                return .{};
            }
        };

        self.write_done.store(false, .release);
        self.write_task = global_state.io.async(SendContext.run, .{.{ .connection = self, .queued_write = queued_write }});
    }

    fn sendBlocking(self: *PeerConnection, event_identifier: u16, serialized: []const u8, method: CompressionMethod) !void {
        var compressed_buffer: [maximum_payload_size]u8 = undefined;
        const compressed_length = compressWithMethod(method, serialized, &compressed_buffer);

        const header = WireHeader{
            .event_identifier = event_identifier,
            .payload_length = @intCast(compressed_length),
            .flags = compressionToFlags(method),
        };

        if (self.writer == null) {
            self.writer = self.stream.writer(global_state.io, &self.write_buffer);
        }
        var stream_writer = &self.writer.?;

        stream_writer.interface.writeStruct(header, .little) catch return NetError.ConnectionClosed;
        stream_writer.interface.writeAll(compressed_buffer[0..compressed_length]) catch return NetError.ConnectionClosed;
        stream_writer.interface.flush() catch return NetError.ConnectionClosed;
    }

    fn close(self: *PeerConnection) void {
        if (self.closed) return;
        self.closed = true;

        if (self.read_task) |*task| _ = task.cancel(global_state.io);
        if (self.write_task) |*task| _ = task.cancel(global_state.io);

        self.reader = null;
        self.writer = null;

        for (self.write_queue.items) |item| global_state.allocator.free(item.payload);
        self.write_queue.deinit(global_state.allocator);

        self.stream.close(global_state.io);
    }
};

const NetworkRole = enum { client, server };

const GlobalState = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    role: NetworkRole,

    listener: ?std.Io.net.Server = null,
    accept_task: ?std.Io.Future(AcceptOutcome) = null,
    accept_done: std.atomic.Value(bool) = .init(false),

    connections: std.AutoHashMap(UserId, PeerConnection) = undefined,
    next_user_identifier: UserId = 1,

    incoming: std.AutoHashMap(UserId, std.ArrayList(RawPacket)) = undefined,

    disconnected_users: std.ArrayList(UserId) = .empty,
    connected_users: std.ArrayList(UserId) = .empty,

    const AcceptOutcome = struct { stream: ?std.Io.net.Stream = null, err: ?anyerror = null };
};

var global_state: GlobalState = undefined;
var is_initialized = false;

fn ensureInitialized(io: std.Io, allocator: std.mem.Allocator, role: NetworkRole) !void {
    if (is_initialized) return NetError.AlreadyInitialized;

    global_state = .{
        .io = io,
        .allocator = allocator,
        .role = role,
        .connections = .init(allocator),
        .incoming = .init(allocator),
    };

    is_initialized = true;
}

pub fn startServer(io: std.Io, allocator: std.mem.Allocator, bind_address: std.Io.net.IpAddress) !void {
    try ensureInitialized(io, allocator, .server);

    global_state.listener = try bind_address.listen(io, .{ .reuse_address = true });
}

pub fn startClient(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !UserId {
    try ensureInitialized(io, allocator, .client);

    const stream = try server_address.connect(io, .{ .mode = .stream });

    return try addConnection(stream);
}

pub fn shutdown() void {
    if (!is_initialized) return;

    var connection_iterator = global_state.connections.valueIterator();
    while (connection_iterator.next()) |connection| connection.close();
    global_state.connections.deinit();

    var incoming_iterator = global_state.incoming.valueIterator();
    while (incoming_iterator.next()) |packet_list| {
        for (packet_list.items) |*packet| packet.deinit();
        packet_list.deinit(global_state.allocator);
    }
    global_state.incoming.deinit();

    if (global_state.accept_task) |*task| _ = task.cancel(global_state.io);
    if (global_state.listener) |*listener| listener.deinit(global_state.io);

    global_state.disconnected_users.deinit(global_state.allocator);
    global_state.connected_users.deinit(global_state.allocator);

    is_initialized = false;
}

fn addConnection(stream: std.Io.net.Stream) !UserId {
    const assigned_identifier = global_state.next_user_identifier;
    global_state.next_user_identifier += 1;

    try global_state.connections.put(assigned_identifier, .{ .stream = stream, .user_identifier = assigned_identifier });
    try global_state.incoming.put(assigned_identifier, .empty);

    return assigned_identifier;
}

fn removeConnection(user_identifier: UserId) void {
    if (global_state.connections.fetchRemove(user_identifier)) |entry| {
        var connection = entry.value;
        connection.close();
    }

    if (global_state.incoming.fetchRemove(user_identifier)) |entry| {
        var packet_list = entry.value;
        for (packet_list.items) |*packet| packet.deinit();
        packet_list.deinit(global_state.allocator);
    }
}

pub fn poll() !void {
    if (!is_initialized) return NetError.NotInitialized;

    if (global_state.role == .server) try pollAccept();

    var connection_iterator = global_state.connections.iterator();
    var dead_users: std.ArrayList(UserId) = .empty;
    defer dead_users.deinit(global_state.allocator);

    var read_results: std.ArrayList(PeerConnection.ReadOutcome) = .empty;
    defer read_results.deinit(global_state.allocator);

    while (connection_iterator.next()) |entry| {
        const user_identifier = entry.key_ptr.*;
        const connection = entry.value_ptr;

        connection.startReceiveIfIdle();
        connection.pumpWrites();

        read_results.clearRetainingCapacity();
        try connection.drainReadTasks(&read_results);

        for (read_results.items) |result| {
            if (result.err) |_| {
                try dead_users.append(global_state.allocator, user_identifier);
            } else if (result.packet) |packet| {
                const packet_list = global_state.incoming.getPtr(user_identifier).?;
                try packet_list.append(global_state.allocator, packet);
            }
        }
    }

    for (dead_users.items) |user_identifier| {
        removeConnection(user_identifier);
        try global_state.disconnected_users.append(global_state.allocator, user_identifier);
    }
}

fn pollAccept() !void {
    if (global_state.accept_task == null) {
        const AcceptContext = struct {
            server: *std.Io.net.Server,

            fn run(context: @This()) GlobalState.AcceptOutcome {
                defer global_state.accept_done.store(true, .release);

                const stream = context.server.accept(global_state.io) catch |err| return .{ .err = err };
                return .{ .stream = stream };
            }
        };

        global_state.accept_done.store(false, .release);
        global_state.accept_task = global_state.io.async(AcceptContext.run, .{.{ .server = &global_state.listener.? }});
        return;
    }

    if (!global_state.accept_done.load(.acquire)) return;
    var task = global_state.accept_task.?;
    global_state.accept_task = null;

    const outcome = task.await(global_state.io);

    if (outcome.err) |err| {
        std.log.err("[netling] accept failed: {}", .{err});
    } else if (outcome.stream) |stream| {
        const user_identifier = try addConnection(stream);
        try global_state.connected_users.append(global_state.allocator, user_identifier);
    }
}

pub fn takeConnectedUsers(allocator: std.mem.Allocator) ![]UserId {
    if (!is_initialized) return NetError.NotInitialized;

    const result = try allocator.dupe(UserId, global_state.connected_users.items);
    global_state.connected_users.clearRetainingCapacity();

    return result;
}

pub fn takeDisconnectedUsers(allocator: std.mem.Allocator) ![]UserId {
    if (!is_initialized) return NetError.NotInitialized;

    const result = try allocator.dupe(UserId, global_state.disconnected_users.items);
    global_state.disconnected_users.clearRetainingCapacity();

    return result;
}

pub fn connectedUsers(allocator: std.mem.Allocator) ![]UserId {
    if (!is_initialized) return NetError.NotInitialized;

    var result: std.ArrayList(UserId) = .empty;
    var key_iterator = global_state.connections.keyIterator();
    while (key_iterator.next()) |user_identifier| try result.append(allocator, user_identifier.*);

    return try result.toOwnedSlice(allocator);
}

pub fn isConnected(user_identifier: UserId) bool {
    if (!is_initialized) return false;

    return global_state.connections.contains(user_identifier);
}

pub fn registerEvent(comptime ValueType: type, method: CompressionMethod) NetworkEvent(ValueType) {
    const S = struct {
        var counter: u16 = 0;
    };
    const assigned = S.counter;
    S.counter += 1;
    return .{ .event_identifier = assigned, .compression_method = method };
}

pub fn NetworkEvent(comptime ValueType: type) type {
    return struct {
        event_identifier: u16,
        compression_method: CompressionMethod,

        const Self = @This();

        pub fn sendTo(self: Self, target_user: UserId, value: ValueType) !void {
            if (!is_initialized) return NetError.NotInitialized;

            const connection = global_state.connections.getPtr(target_user) orelse return NetError.UnknownUser;

            try connection.queueSend(self.event_identifier, ValueType, value, self.compression_method);
        }

        pub fn broadcastAll(self: Self, value: ValueType) !void {
            if (!is_initialized) return NetError.NotInitialized;

            var connection_iterator = global_state.connections.valueIterator();
            while (connection_iterator.next()) |connection| {
                try connection.queueSend(self.event_identifier, ValueType, value, self.compression_method);
            }
        }

        pub fn broadcastExcept(self: Self, excluded_user: UserId, value: ValueType) !void {
            if (!is_initialized) return NetError.NotInitialized;

            var connection_iterator = global_state.connections.valueIterator();
            while (connection_iterator.next()) |connection| {
                if (connection.user_identifier == excluded_user) continue;
                try connection.queueSend(self.event_identifier, ValueType, value, self.compression_method);
            }
        }

        pub fn pollEvent(self: Self, from_user: UserId) ![]ValueType {
            if (!is_initialized) return NetError.NotInitialized;

            const packet_list = global_state.incoming.getPtr(from_user) orelse return &.{};

            var result: std.ArrayList(ValueType) = .empty;
            var write_index: usize = 0;

            for (packet_list.items) |*packet| {
                if (packet.event_identifier != self.event_identifier) {
                    packet_list.items[write_index] = packet.*;
                    write_index += 1;
                    continue;
                }

                var payload_reader: std.Io.Reader = .fixed(packet.payload);
                const value = try deserializeValue(ValueType, &payload_reader, global_state.allocator);

                try result.append(global_state.allocator, value);

                packet.deinit();
            }

            packet_list.shrinkRetainingCapacity(write_index);

            return try result.toOwnedSlice(global_state.allocator);
        }
    };
}
