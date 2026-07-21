const connection = @import("connection.zig");
const root = @import("root.zig");
const std = @import("std");

var global_state: GlobalState = undefined;
var is_initialized = false;

const NetworkRole = enum {
    client,
    server,
};

const AcceptContext = struct {
    server: *std.Io.net.Server,

    fn run(context: @This()) GlobalState.AcceptOutcome {
        defer global_state.accept_done.store(true, .release);

        const stream = context.server.accept(global_state.io) catch |error_value| return .{
            .error_value = error_value,
        };

        return .{ .stream = stream };
    }
};

const GlobalState = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    role: NetworkRole,

    listener: ?std.Io.net.Server = null,
    accept_task: ?std.Io.Future(AcceptOutcome) = null,
    accept_done: std.atomic.Value(bool) = .init(false),

    connections: std.AutoHashMap(root.UserId, connection.PeerConnection) = undefined,
    next_user_identifier: root.UserId = 1,

    incoming: std.AutoHashMap(root.UserId, std.ArrayList(connection.RawPacket)) = undefined,

    disconnected_users: std.ArrayList(root.UserId) = .empty,
    connected_users: std.ArrayList(root.UserId) = .empty,

    const AcceptOutcome = struct {
        stream: ?std.Io.net.Stream = null,
        error_value: ?anyerror = null,
    };
};

fn ensureInitialized(io: std.Io, allocator: std.mem.Allocator, role: NetworkRole) !void {
    if (is_initialized) return root.NetworkError.AlreadyInitialized;

    global_state = .{
        .io = io,
        .allocator = allocator,
        .role = role,
        .connections = .init(allocator),
        .incoming = .init(allocator),
    };

    is_initialized = true;
}

fn addConnection(stream: std.Io.net.Stream) !root.UserId {
    const assigned_identifier = global_state.next_user_identifier;

    global_state.next_user_identifier += 1;

    try global_state.connections.put(assigned_identifier, .{
        .stream = stream,
        .user_identifier = assigned_identifier,
    });

    try global_state.incoming.put(assigned_identifier, .empty);

    return assigned_identifier;
}

fn removeConnection(user_identifier: root.UserId) void {
    if (global_state.connections.fetchRemove(user_identifier)) |entry| {
        var value = entry.value;

        value.close(global_state.io, global_state.allocator);
    }

    if (global_state.incoming.fetchRemove(user_identifier)) |entry| {
        var packet_list = entry.value;

        for (packet_list.items) |*packet| packet.deinit(global_state.allocator);

        packet_list.deinit(global_state.allocator);
    }
}

fn pollAccept() !void {
    if (global_state.accept_task == null) {
        global_state.accept_done.store(false, .release);
        global_state.accept_task = global_state.io.async(AcceptContext.run, .{.{ .server = &global_state.listener.? }});

        return;
    }

    if (!global_state.accept_done.load(.acquire)) return;

    var task = global_state.accept_task.?;

    global_state.accept_task = null;

    const outcome = task.await(global_state.io);

    if (outcome.error_value) |error_value| {
        std.log.err("[netling] accept failed: {}", .{error_value});
    } else if (outcome.stream) |stream| {
        const user_identifier = try addConnection(stream);

        try global_state.connected_users.append(global_state.allocator, user_identifier);
    }
}

pub fn initServer(io: std.Io, allocator: std.mem.Allocator, bind_address: std.Io.net.IpAddress) !void {
    try ensureInitialized(io, allocator, .server);

    global_state.listener = try bind_address.listen(io, .{ .reuse_address = true });
}

pub fn initClient(io: std.Io, allocator: std.mem.Allocator, server_address: std.Io.net.IpAddress) !root.UserId {
    try ensureInitialized(io, allocator, .client);

    const stream = try server_address.connect(io, .{ .mode = .stream });

    return try addConnection(stream);
}

pub fn shutdown() void {
    if (!is_initialized) return;

    var connection_iterator = global_state.connections.valueIterator();

    while (connection_iterator.next()) |value| value.close(global_state.io, global_state.allocator);

    global_state.connections.deinit();

    var incoming_iterator = global_state.incoming.valueIterator();

    while (incoming_iterator.next()) |packet_list| {
        for (packet_list.items) |*packet| packet.deinit(global_state.allocator);

        packet_list.deinit(global_state.allocator);
    }

    global_state.incoming.deinit();

    if (global_state.accept_task) |*task| _ = task.cancel(global_state.io);
    if (global_state.listener) |*listener| listener.deinit(global_state.io);

    global_state.disconnected_users.deinit(global_state.allocator);
    global_state.connected_users.deinit(global_state.allocator);

    is_initialized = false;
}

pub fn poll() !void {
    if (!is_initialized) return root.NetworkError.NotInitialized;
    if (global_state.role == .server) try pollAccept();

    var connection_iterator = global_state.connections.iterator();

    var dead_users: std.ArrayList(root.UserId) = .empty;
    var read_results: std.ArrayList(connection.ReadOutcome) = .empty;

    defer dead_users.deinit(global_state.allocator);
    defer read_results.deinit(global_state.allocator);

    while (connection_iterator.next()) |entry| {
        const user_identifier = entry.key_ptr.*;
        const value = entry.value_ptr;

        value.startReceiveIfIdle(global_state.io, global_state.allocator);
        value.pumpWrites(global_state.io, global_state.allocator);
        read_results.clearRetainingCapacity();

        try value.drainReadTasks(global_state.io, global_state.allocator, &read_results);

        for (read_results.items) |result| {
            if (result.error_value) |_| {
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

pub fn takeConnectedUsers(allocator: std.mem.Allocator) ![]root.UserId {
    if (!is_initialized) return root.NetworkError.NotInitialized;

    const result = try allocator.dupe(root.UserId, global_state.connected_users.items);

    global_state.connected_users.clearRetainingCapacity();

    return result;
}

pub fn takeDisconnectedUsers(allocator: std.mem.Allocator) ![]root.UserId {
    if (!is_initialized) return root.NetworkError.NotInitialized;

    const result = try allocator.dupe(root.UserId, global_state.disconnected_users.items);

    global_state.disconnected_users.clearRetainingCapacity();

    return result;
}

pub fn connectedUsers(allocator: std.mem.Allocator) ![]root.UserId {
    if (!is_initialized) return root.NetworkError.NotInitialized;

    var result: std.ArrayList(root.UserId) = .empty;
    var key_iterator = global_state.connections.keyIterator();

    while (key_iterator.next()) |user_identifier| try result.append(allocator, user_identifier.*);

    return try result.toOwnedSlice(allocator);
}

pub fn isConnected(user_identifier: root.UserId) bool {
    if (!is_initialized) return false;

    return global_state.connections.contains(user_identifier);
}

pub fn isServerRole() bool {
    return is_initialized and global_state.role == .server;
}

pub fn isClientRole() bool {
    return is_initialized and global_state.role == .client;
}

pub fn requireInitialized() !void {
    if (!is_initialized) return root.NetworkError.NotInitialized;
}

pub fn sharedAllocator() std.mem.Allocator {
    return global_state.allocator;
}

pub fn findConnection(user_identifier: root.UserId) ?*connection.PeerConnection {
    return global_state.connections.getPtr(user_identifier);
}

pub fn getConnectedCount() usize {
    return global_state.connected_users.items.len;
}

pub fn connectionValues() std.AutoHashMap(root.UserId, connection.PeerConnection).ValueIterator {
    return global_state.connections.valueIterator();
}

pub fn findIncoming(user_identifier: root.UserId) ?*std.ArrayList(connection.RawPacket) {
    return global_state.incoming.getPtr(user_identifier);
}
