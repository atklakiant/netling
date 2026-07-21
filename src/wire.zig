const std = @import("std");
const zstd = @import("zstd");

pub const maximum_payload_size: usize = 65536;
pub const zstd_level: i32 = zstd.DEFAULT_COMPRESSION_LEVEL;

pub const WireHeader = extern struct {
    event_identifier: u16,
    payload_length: u32,
};

pub fn compress(input: []const u8, output: []u8) usize {
    const result = zstd.compress(output, input, zstd_level) catch return 0;

    return result.len;
}

pub fn decompress(input: []const u8, output: []u8) usize {
    const result = zstd.decompress(output, input) catch return 0;

    return result.len;
}

pub fn serializeValue(comptime ValueType: type, value: ValueType, writer: *std.Io.Writer) !void {
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
            if (pointer_info.size != .slice) std.debug.panic("[netling] unsupported pointer type: {}", .{@typeName(ValueType)});

            try writer.writeInt(u32, @intCast(value.len), .little);

            if (pointer_info.child == u8) {
                try writer.writeAll(value);
            } else {
                for (value) |element| try serializeValue(pointer_info.child, element, writer);
            }
        },
        .vector => |vector_info| {
            inline for (0..vector_info.len) |element_index| {
                try serializeValue(vector_info.child, value[element_index], writer);
            }
        },
        .optional => |optional_info| {
            if (value) |inner_value| {
                try writer.writeByte(1);
                try serializeValue(optional_info.child, inner_value, writer);
            } else try writer.writeByte(0);
        },
        else => std.debug.panic("[netling] unsupported type: {}", .{@typeName(ValueType)}),
    }
}

pub fn deserializeValue(comptime ValueType: type, reader: *std.Io.Reader, allocator: std.mem.Allocator) !ValueType {
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
        .vector => |vector_info| {
            var result: ValueType = undefined;

            inline for (0..vector_info.len) |element_index| {
                result[element_index] = try deserializeValue(vector_info.child, reader, allocator);
            }

            return result;
        },
        .pointer => |pointer_info| {
            if (pointer_info.size != .slice) std.debug.panic("[netling] unsupported pointer type: {}", .{@typeName(ValueType)});

            const slice_length = try reader.takeInt(u32, .little);

            if (pointer_info.child == u8) {
                const buffer = try allocator.alloc(u8, slice_length);

                try reader.readSliceAll(buffer);

                return buffer;
            }

            const buffer = try allocator.alloc(pointer_info.child, slice_length);

            for (buffer) |*element| element.* = try deserializeValue(pointer_info.child, reader, allocator);

            return buffer;
        },
        .optional => |optional_info| {
            if ((try reader.takeByte()) != 0) return try deserializeValue(optional_info.child, reader, allocator);

            return null;
        },
        else => std.debug.panic("[netling] unsupported type: {}", .{@typeName(ValueType)}),
    }
}
