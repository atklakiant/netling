//! netling — minimal networked event bus.
//!
//! Usage (this is the entire public API surface):
//!
//!     const netling = @import("netling");
//!
//!     try netling.startServer(io, allocator, address);      // OR
//!     const server_id = try netling.startClient(io, allocator, address);
//!
//!     const Input = []const u8;
//!     const leave_event = netling.registerEvent(Input, .bitpack);
//!
//!     while (running) {
//!         try netling.poll(); // drives accept/read/write + housekeeping, non-blocking
//!
//!         try leave_event.sendTo(server_id, "kicked");
//!         try leave_event.broadcastAll("explosion");
//!         try leave_event.broadcastExcept(server_id, "boom");
//!
//!         for (try leave_event.pollEvent(server_id)) |data| {
//!             // data: Input, already deserialized + decompressed
//!         }
//!     }
//!
//! There is no Context, no Connection, no Mutex, and no other file to import.
//! Every file previously named root/client/server/context/connection/wire/
//! compress/serialize has been folded in here. `Event`/`registerEvent` is the
//! only thing a consumer needs to know exists.

const std = @import("std");

// =====================================================================
// Public types
// =====================================================================

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

// =====================================================================
// Wire format (was wire.zig)
// =====================================================================

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

const maximum_packet_payload_size: usize = 65536;

fn methodToFlags(method: CompressionMethod) PacketFlags {
    return switch (method) {
        .none => .{},
        .rle => .{ .rle_compressed = true },
        .bitpack => .{ .bitpacked = true },
        .both => .{ .rle_compressed = true, .bitpacked = true },
    };
}

fn flagsToMethod(flags: PacketFlags) CompressionMethod {
    if (flags.rle_compressed and flags.bitpacked) return .both;
    if (flags.rle_compressed) return .rle;
    if (flags.bitpacked) return .bitpack;
    return .none;
}

// =====================================================================
// Compression (was compress.zig)
// =====================================================================

const RunLength = struct {
    fn encode(input: []const u8, output: []u8) usize {
        var in_i: usize = 0;
        var out_i: usize = 0;

        while (in_i < input.len) {
            const byte = input[in_i];
            var run: usize = 1;

            while (in_i + run < input.len and input[in_i + run] == byte and run < 255) run += 1;

            if (run >= 3 or byte == 0xFE) {
                if (out_i + 3 > output.len) break;
                output[out_i] = 0xFE;
                output[out_i + 1] = byte;
                output[out_i + 2] = @intCast(run);
                out_i += 3;
            } else {
                for (0..run) |_| {
                    if (out_i >= output.len) break;
                    output[out_i] = byte;
                    out_i += 1;
                }
            }

            in_i += run;
        }

        return out_i;
    }

    fn decode(input: []const u8, output: []u8) usize {
        var in_i: usize = 0;
        var out_i: usize = 0;

        while (in_i < input.len) {
            if (input[in_i] == 0xFE and in_i + 2 < input.len) {
                const run_byte = input[in_i + 1];
                const run_count = input[in_i + 2];

                for (0..run_count) |_| {
                    if (out_i >= output.len) break;
                    output[out_i] = run_byte;
                    out_i += 1;
                }

                in_i += 3;
            } else {
                if (out_i >= output.len) break;
                output[out_i] = input[in_i];
                out_i += 1;
                in_i += 1;
            }
        }

        return out_i;
    }
};

const Bitpack = struct {
    fn encode(input: []const u8, output: []u8) usize {
        if (input.len % 4 != 0) {
            const n = @min(input.len, output.len);
            @memcpy(output[0..n], input[0..n]);
            return n;
        }

        var in_i: usize = 0;
        var out_i: usize = 0;

        while (in_i + 4 <= input.len) {
            const value = std.mem.readInt(u32, input[in_i..][0..4], .little);
            const needed: u8 = if (value <= 0xFF) 1 else if (value <= 0xFFFF) 2 else if (value <= 0xFFFFFF) 3 else 4;

            if (out_i + 1 + needed > output.len) break;

            output[out_i] = needed;
            out_i += 1;

            for (0..needed) |b| {
                output[out_i] = @intCast((value >> @intCast(b * 8)) & 0xFF);
                out_i += 1;
            }

            in_i += 4;
        }

        return out_i;
    }

    fn decode(input: []const u8, output: []u8) usize {
        var in_i: usize = 0;
        var out_i: usize = 0;

        while (in_i < input.len) {
            if (in_i + 1 > input.len) break;

            const needed = input[in_i];
            in_i += 1;

            if (needed == 0 or needed > 4) break;
            if (in_i + needed > input.len) break;
            if (out_i + 4 > output.len) break;

            var value: u32 = 0;
            for (0..needed) |b| value |= @as(u32, input[in_i + b]) << @intCast(b * 8);

            in_i += needed;

            std.mem.writeInt(u32, output[out_i..][0..4], value, .little);
            out_i += 4;
        }

        return out_i;
    }
};

fn compressWithMethod(method: CompressionMethod, input: []const u8, output: []u8) usize {
    return switch (method) {
        .none => blk: {
            const n = @min(input.len, output.len);
            @memcpy(output[0..n], input[0..n]);
            break :blk n;
        },
        .rle => RunLength.encode(input, output),
        .bitpack => Bitpack.encode(input, output),
        .both => blk: {
            var mid: [maximum_packet_payload_size]u8 = undefined;
            const n = RunLength.encode(input, &mid);
            break :blk Bitpack.encode(mid[0..n], output);
        },
    };
}

fn decompressWithMethod(method: CompressionMethod, input: []const u8, output: []u8) usize {
    return switch (method) {
        .none => blk: {
            const n = @min(input.len, output.len);
            @memcpy(output[0..n], input[0..n]);
            break :blk n;
        },
        .rle => RunLength.decode(input, output),
        .bitpack => Bitpack.decode(input, output),
        .both => blk: {
            var mid: [maximum_packet_payload_size]u8 = undefined;
            const n = Bitpack.decode(input, &mid);
            break :blk RunLength.decode(mid[0..n], output);
        },
    };
}

// =====================================================================
// Serialization (was serialize.zig)
// =====================================================================

fn serializeValue(comptime T: type, value: T, writer: *std.Io.Writer) !void {
    switch (@typeInfo(T)) {
        .void => {},
        .bool => try writer.writeByte(if (value) 1 else 0),
        .int => try writer.writeInt(T, value, .little),
        .float => try writer.writeAll(std.mem.asBytes(&value)),
        .@"struct" => |info| {
            if (@hasDecl(T, "netlingSerialize")) {
                try value.netlingSerialize(writer);
            } else {
                inline for (info.fields) |field| {
                    try serializeValue(field.type, @field(value, field.name), writer);
                }
            }
        },
        .@"enum" => |info| try writer.writeInt(info.tag_type, @intFromEnum(value), .little),
        .array => |info| for (value) |el| try serializeValue(info.child, el, writer),
        .pointer => |info| {
            if (info.size == .Slice and info.child == u8) {
                try writer.writeInt(u32, @intCast(value.len), .little);
                try writer.writeAll(value);
            } else @compileError("[netling] unsupported pointer type: " ++ @typeName(T));
        },
        .optional => |info| {
            if (value) |inner| {
                try writer.writeByte(1);
                try serializeValue(info.child, inner, writer);
            } else try writer.writeByte(0);
        },
        else => @compileError("[netling] unsupported type: " ++ @typeName(T)),
    }
}

fn deserializeValue(comptime T: type, reader: *std.Io.Reader, allocator: std.mem.Allocator) !T {
    switch (@typeInfo(T)) {
        .void => return {},
        .bool => return (try reader.takeByte()) != 0,
        .int => return reader.takeInt(T, .little),
        .float => return std.mem.bytesToValue(T, try reader.takeArray(@sizeOf(T))),
        .@"struct" => |info| {
            if (@hasDecl(T, "netlingDeserialize")) return T.netlingDeserialize(reader, allocator);

            var result: T = undefined;
            inline for (info.fields) |field| {
                @field(result, field.name) = try deserializeValue(field.type, reader, allocator);
            }
            return result;
        },
        .@"enum" => |info| return @enumFromInt(try reader.takeInt(info.tag_type, .little)),
        .array => |info| {
            var result: T = undefined;
            for (&result) |*el| el.* = try deserializeValue(info.child, reader, allocator);
            return result;
        },
        .pointer => |info| {
            if (info.size == .Slice and info.child == u8) {
                const len = try reader.takeInt(u32, .little);
                const buf = try allocator.alloc(u8, len);
                try reader.readSliceAll(buf);
                return buf;
            } else @compileError("[netling] unsupported pointer type: " ++ @typeName(T));
        },
        .optional => |info| {
            if ((try reader.takeByte()) != 0) return try deserializeValue(info.child, reader, allocator);
            return null;
        },
        else => @compileError("[netling] unsupported type: " ++ @typeName(T)),
    }
}

// =====================================================================
// Internal connection (was connection.zig) — never touched by consumers
// =====================================================================

const RawPacket = struct {
    event_identifier: u16,
    payload: []u8, // owned by g.allocator

    fn deinit(self: *RawPacket) void {
        g.allocator.free(self.payload);
    }
};

const PeerConnection = struct {
    stream: std.Io.net.Stream,
    user_identifier: UserId,

    read_buffer: [8192]u8 = undefined,
    write_buffer: [8192]u8 = undefined,

    // Persistent across calls: std.Io.Reader/Writer may read/write more
    // than one logical packet's worth of bytes per underlying syscall and
    // buffer the rest internally. Reconstructing a fresh reader/writer on
    // every send/receive silently discards that buffered leftover — which
    // desyncs the framing (manifests as EndOfStream partway through a
    // struct, or a header that decodes to garbage). These must live as
    // long as the connection does.
    stream_reader: ?std.Io.net.Stream.Reader = null,
    stream_writer: ?std.Io.net.Stream.Writer = null,

    read_task: ?std.Io.Future(ReadResult) = null,
    read_done: std.atomic.Value(bool) = .init(false),

    write_queue: std.ArrayList(QueuedWrite) = .empty,
    write_task: ?std.Io.Future(WriteResult) = null,
    write_done: std.atomic.Value(bool) = .init(false),

    closed: bool = false,

    const ReadResult = struct { packet: ?RawPacket = null, err: ?anyerror = null };
    const WriteResult = struct { err: ?anyerror = null };

    const QueuedWrite = struct {
        event_identifier: u16,
        payload: []u8, // owned, freed after send
        method: CompressionMethod,
    };

    fn startReceiveIfIdle(self: *PeerConnection) void {
        if (self.read_task != null or self.closed) return;

        const Ctx = struct {
            conn: *PeerConnection,

            fn run(ctx: @This()) ReadResult {
                defer ctx.conn.read_done.store(true, .release);

                const packet = ctx.conn.receiveBlocking() catch |err| return .{ .err = err };
                return .{ .packet = packet };
            }
        };

        self.read_done.store(false, .release);
        self.read_task = g.io.async(Ctx.run, .{.{ .conn = self }});
    }

    /// Non-blocking: returns the finished result if the in-flight read task
    /// has completed, otherwise null. Never touches the Future unless the
    /// atomic flag confirms the task already finished, so await() returns
    /// immediately.
    fn pollReadTask(self: *PeerConnection) ?ReadResult {
        if (self.read_task == null) return null;
        if (!self.read_done.load(.acquire)) return null;

        var task = self.read_task.?;
        self.read_task = null;

        return task.await(g.io);
    }

    fn receiveBlocking(self: *PeerConnection) !RawPacket {
        var reader = self.stream.reader(g.io, &self.read_buffer);
        const header = reader.interface.takeStruct(WireHeader, .little) catch return NetError.ConnectionClosed;

        var compressed_buf: [maximum_packet_payload_size]u8 = undefined;
        const compressed = compressed_buf[0..header.payload_length];
        reader.interface.readSliceAll(compressed) catch return NetError.ConnectionClosed;

        var decompressed_buf: [maximum_packet_payload_size]u8 = undefined;
        const n = decompressWithMethod(flagsToMethod(header.flags), compressed, &decompressed_buf);

        return .{
            .event_identifier = header.event_identifier,
            .payload = try g.allocator.dupe(u8, decompressed_buf[0..n]),
        };
    }

    /// Non-blocking enqueue. Actual send happens during poll().
    fn queueSend(self: *PeerConnection, event_identifier: u16, comptime T: type, value: T, method: CompressionMethod) !void {
        var serialized_buf: [maximum_packet_payload_size]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&serialized_buf);
        try serializeValue(T, value, &writer);

        try self.write_queue.append(g.allocator, .{
            .event_identifier = event_identifier,
            .payload = try g.allocator.dupe(u8, writer.buffered()),
            .method = method,
        });
    }

    fn pumpWrites(self: *PeerConnection) void {
        if (self.closed) return;

        // Reap a finished write, if any (non-blocking via the done flag).
        if (self.write_task != null) {
            if (!self.write_done.load(.acquire)) return; // still in flight

            var task = self.write_task.?;
            self.write_task = null;

            const result = task.await(g.io);
            if (result.err) |err| std.log.err("[netling] write failed: {}", .{err});
        }

        if (self.write_queue.items.len == 0) return;

        const queued = self.write_queue.orderedRemove(0);

        const Ctx = struct {
            conn: *PeerConnection,
            item: QueuedWrite,

            fn run(ctx: @This()) WriteResult {
                defer g.allocator.free(ctx.item.payload);
                defer ctx.conn.write_done.store(true, .release);

                ctx.conn.sendBlocking(ctx.item.event_identifier, ctx.item.payload, ctx.item.method) catch |err| {
                    return .{ .err = err };
                };

                return .{};
            }
        };

        self.write_done.store(false, .release);
        self.write_task = g.io.async(Ctx.run, .{.{ .conn = self, .item = queued }});
    }

    fn sendBlocking(self: *PeerConnection, event_identifier: u16, serialized: []const u8, method: CompressionMethod) !void {
        var compressed_buf: [maximum_packet_payload_size]u8 = undefined;
        const compressed_len = compressWithMethod(method, serialized, &compressed_buf);

        const header = WireHeader{
            .event_identifier = event_identifier,
            .payload_length = @intCast(compressed_len),
            .flags = methodToFlags(method),
        };

        var writer = self.stream.writer(g.io, &self.write_buffer);
        writer.interface.writeStruct(header, .little) catch return NetError.ConnectionClosed;
        writer.interface.writeAll(compressed_buf[0..compressed_len]) catch return NetError.ConnectionClosed;
        writer.interface.flush() catch return NetError.ConnectionClosed;
    }

    fn close(self: *PeerConnection) void {
        if (self.closed) return;
        self.closed = true;

        if (self.read_task) |*t| _ = t.cancel(g.io);
        if (self.write_task) |*t| _ = t.cancel(g.io);

        for (self.write_queue.items) |item| g.allocator.free(item.payload);
        self.write_queue.deinit(g.allocator);

        self.stream.close(g.io);
    }
};

// =====================================================================
// Global network state — replaces Context/Client/Server entirely.
// Every consumer touches exactly zero of this; it's reached only through
// registerEvent()'s returned handle and the top-level init/poll functions.
// =====================================================================

const Role = enum { client, server };

const GlobalState = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    role: Role,

    listener: ?std.Io.net.Server = null,
    accept_task: ?std.Io.Future(AcceptResult) = null,
    accept_done: std.atomic.Value(bool) = .init(false),

    connections: std.AutoHashMap(UserId, PeerConnection) = undefined,
    next_user_identifier: UserId = 1,

    // incoming[user_id] = list of not-yet-consumed raw packets for that peer
    incoming: std.AutoHashMap(UserId, std.ArrayList(RawPacket)) = undefined,

    // scratch buffer reused by Event.pollEvent so callers never have to free
    scratch: std.ArrayList(u8) = .empty,

    // filled during poll(), drained by takeDisconnected()
    disconnected: std.ArrayList(UserId) = .empty,
    // filled during poll(), drained by takeConnected()
    connected: std.ArrayList(UserId) = .empty,

    const AcceptResult = struct { stream: ?std.Io.net.Stream = null, err: ?anyerror = null };
};

var g: GlobalState = undefined;
var g_initialized = false;

fn ensureInit(io: std.Io, allocator: std.mem.Allocator, role: Role) !void {
    if (g_initialized) return NetError.AlreadyInitialized;

    g = .{
        .io = io,
        .allocator = allocator,
        .role = role,
        .connections = .init(allocator),
        .incoming = .init(allocator),
    };

    g_initialized = true;
}

/// Start listening as a server. Call poll() every frame afterward.
pub fn startServer(io: std.Io, allocator: std.mem.Allocator, bind_address: std.Io.net.IpAddress) !void {
    try ensureInit(io, allocator, .server);

    g.listener = try bind_address.listen(io, .{ .reuse_address = true });
}

/// Connect as a client. Returns the UserId to use when talking to the server.
/// Call poll() every frame afterward.
pub fn startClient(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !UserId {
    try ensureInit(io, allocator, .client);

    const stream = try server_address.connect(io, .{ .mode = .stream });

    return try addConnection(stream);
}

pub fn shutdown() void {
    if (!g_initialized) return;

    var it = g.connections.valueIterator();
    while (it.next()) |conn| conn.close();
    g.connections.deinit();

    var inc_it = g.incoming.valueIterator();
    while (inc_it.next()) |list| {
        for (list.items) |*p| p.deinit();
        list.deinit(g.allocator);
    }
    g.incoming.deinit();

    if (g.accept_task) |*t| _ = t.cancel(g.io);
    if (g.listener) |*l| l.deinit(g.io);

    g.scratch.deinit(g.allocator);
    g.disconnected.deinit(g.allocator);
    g.connected.deinit(g.allocator);

    g_initialized = false;
}

fn addConnection(stream: std.Io.net.Stream) !UserId {
    const id = g.next_user_identifier;
    g.next_user_identifier += 1;

    try g.connections.put(id, .{ .stream = stream, .user_identifier = id });
    try g.incoming.put(id, .empty);

    return id;
}

fn removeConnection(id: UserId) void {
    if (g.connections.fetchRemove(id)) |entry| {
        var conn = entry.value;
        conn.close();
    }

    if (g.incoming.fetchRemove(id)) |entry| {
        var list = entry.value;
        for (list.items) |*p| p.deinit();
        list.deinit(g.allocator);
    }
}

/// Drives everything: accepting new clients, pumping reads/writes, and
/// filing incoming packets so pollEvent() can find them. Call this once per
/// frame/tick from your main loop. Non-blocking.
pub fn poll() !void {
    if (!g_initialized) return NetError.NotInitialized;

    if (g.role == .server) try pollAccept();

    var it = g.connections.iterator();
    var dead: std.ArrayList(UserId) = .empty;
    defer dead.deinit(g.allocator);

    while (it.next()) |entry| {
        const id = entry.key_ptr.*;
        const conn = entry.value_ptr;

        conn.startReceiveIfIdle();
        conn.pumpWrites();

        if (conn.pollReadTask()) |result| {
            if (result.err) |_| {
                try dead.append(g.allocator, id);
            } else if (result.packet) |packet| {
                const list_ptr = g.incoming.getPtr(id).?;
                try list_ptr.append(g.allocator, packet);
            }
        }
    }

    for (dead.items) |id| {
        removeConnection(id);
        try g.disconnected.append(g.allocator, id);
    }
}

fn pollAccept() !void {
    if (g.accept_task == null) {
        const Ctx = struct {
            server: *std.Io.net.Server,

            fn run(ctx: @This()) GlobalState.AcceptResult {
                defer g.accept_done.store(true, .release);

                const stream = ctx.server.accept(g.io) catch |err| return .{ .err = err };
                return .{ .stream = stream };
            }
        };

        g.accept_done.store(false, .release);
        g.accept_task = g.io.async(Ctx.run, .{.{ .server = &g.listener.? }});
        return;
    }

    if (!g.accept_done.load(.acquire)) return; // still in flight

    var task = g.accept_task.?;
    g.accept_task = null;

    const result = task.await(g.io);

    if (result.err) |err| {
        std.log.err("[netling] accept failed: {}", .{err});
    } else if (result.stream) |stream| {
        const id = try addConnection(stream);
        try g.connected.append(g.allocator, id);
    }
}

/// Returns UserIds that connected since the last call to this function.
/// (Server-side only — a client only ever has one connection, its
/// UserId is the return value of startClient().)
pub fn takeConnected(allocator: std.mem.Allocator) ![]UserId {
    if (!g_initialized) return NetError.NotInitialized;

    const result = try allocator.dupe(UserId, g.connected.items);
    g.connected.clearRetainingCapacity();

    return result;
}

/// Returns UserIds that disconnected since the last call to this function.
/// Use this to know when to tell everyone else a player left.
pub fn takeDisconnected(allocator: std.mem.Allocator) ![]UserId {
    if (!g_initialized) return NetError.NotInitialized;

    const result = try allocator.dupe(UserId, g.disconnected.items);
    g.disconnected.clearRetainingCapacity();

    return result;
}

/// Currently connected UserIds. Caller owns the returned slice.
pub fn connectedUsers(allocator: std.mem.Allocator) ![]UserId {
    if (!g_initialized) return NetError.NotInitialized;

    var result: std.ArrayList(UserId) = .empty;
    var it = g.connections.keyIterator();
    while (it.next()) |id| try result.append(allocator, id.*);

    return try result.toOwnedSlice(allocator);
}

/// Whether `user_identifier` currently has a live connection. Use this to
/// tell "we got disconnected" apart from a genuine error before sending.
pub fn isConnected(user_identifier: UserId) bool {
    if (!g_initialized) return false;

    return g.connections.contains(user_identifier);
}

// =====================================================================
// Public Event API — the ONLY thing consumers should ever hold a
// reference to.
// =====================================================================

/// Register a new event type. Call once (e.g. as a global/comptime-adjacent
/// `const` binding) — this is the only netling API surface a feature file
/// should ever need to import.
///
/// The event id is derived at comptime from the call site (file:line:column)
/// so `registerEvent` can be assigned straight to a `pub const` without
/// needing any mutable global comptime state.
pub fn registerEvent(
    comptime T: type,
    method: CompressionMethod,
    comptime src: std.builtin.SourceLocation = @src(),
) Event(T) {
    const id: u16 = comptime blk: {
        const location = src.file ++ ":" ++ std.fmt.comptimePrint("{d}:{d}", .{ src.line, src.column });
        break :blk @truncate(std.hash.Wyhash.hash(0, location));
    };

    return .{ .event_identifier = id, .compression_method = method };
}

pub fn Event(comptime T: type) type {
    return struct {
        event_identifier: u16,
        compression_method: CompressionMethod,

        const Self = @This();

        pub fn sendTo(self: Self, target: UserId, value: T) !void {
            if (!g_initialized) return NetError.NotInitialized;

            const conn = g.connections.getPtr(target) orelse return NetError.UnknownUser;

            try conn.queueSend(self.event_identifier, T, value, self.compression_method);
        }

        pub fn broadcastAll(self: Self, value: T) !void {
            if (!g_initialized) return NetError.NotInitialized;

            var it = g.connections.valueIterator();
            while (it.next()) |conn| {
                try conn.queueSend(self.event_identifier, T, value, self.compression_method);
            }
        }

        pub fn broadcastExcept(self: Self, excluded: UserId, value: T) !void {
            if (!g_initialized) return NetError.NotInitialized;

            var it = g.connections.valueIterator();
            while (it.next()) |conn| {
                if (conn.user_identifier == excluded) continue;
                try conn.queueSend(self.event_identifier, T, value, self.compression_method);
            }
        }

        /// Returns every value received for this event from `from_user` since
        /// the last call. Valid until the next pollEvent call for this event.
        /// Automatically deserialized + decompressed — nothing else to do.
        pub fn pollEvent(self: Self, from_user: UserId) ![]T {
            if (!g_initialized) return NetError.NotInitialized;

            const list_ptr = g.incoming.getPtr(from_user) orelse return &.{};

            var result: std.ArrayList(T) = .empty;
            var write_index: usize = 0;

            for (list_ptr.items) |*packet| {
                if (packet.event_identifier != self.event_identifier) {
                    list_ptr.items[write_index] = packet.*;
                    write_index += 1;
                    continue;
                }

                var reader: std.Io.Reader = .fixed(packet.payload);
                const value = try deserializeValue(T, &reader, g.allocator);

                try result.append(g.allocator, value);

                packet.deinit();
            }

            list_ptr.shrinkRetainingCapacity(write_index);

            return try result.toOwnedSlice(g.allocator);
        }
    };
}