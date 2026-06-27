const std = @import("std");

pub fn serializeValue(comptime ValueType: type, value: ValueType, writer: *std.Io.Writer) !void {
    switch (@typeInfo(ValueType)) {
        .void => {},
        .bool => try writer.writeByte(if (value) 1 else 0),
        .int => try writer.writeInt(ValueType, value, .little),
        .float => try writer.writeAll(std.mem.asBytes(&value)),
        .@"struct" => |struct_info| {
            if (@hasDecl(ValueType, "compress")) {
                try value.compress(writer);
            } else {
                inline for (struct_info.fields) |field| {
                    try serializeValue(field.type, @field(value, field.name), writer);
                }
            }
        },
        .@"enum" => |enum_info| {
            try writer.writeInt(enum_info.tag_type, @intFromEnum(value), .little);
        },
        .array => |array_info| {
            for (value) |element| try serializeValue(array_info.child, element, writer);
        },
        .pointer => |pointer_info| {
            if (pointer_info.size == .Slice and pointer_info.child == u8) {
                try writer.writeInt(u32, @intCast(value.len), .little);
                try writer.writeAll(value);
            } else @compileError("netling serialize does not support pointer type: " ++ @typeName(ValueType));
        },
        .optional => |optional_info| {
            if (value) |inner_value| {
                try writer.writeByte(1);
                try serializeValue(optional_info.child, inner_value, writer);
            } else {
                try writer.writeByte(0);
            }
        },
        else => @compileError("[netling] serialize does not support type: " ++ @typeName(ValueType)),
    }
}

pub fn deserializeValue(comptime ValueType: type, reader: *std.Io.Reader, allocator: std.mem.Allocator) !ValueType {
    switch (@typeInfo(ValueType)) {
        .void => return {},
        .bool => return (try reader.takeByte()) != 0,
        .int => return reader.takeInt(ValueType, .little),
        .float => {
            const raw_bytes = try reader.takeArray(@sizeOf(ValueType));

            return std.mem.bytesToValue(ValueType, raw_bytes);
        },
        .@"struct" => |struct_info| {
            var result: ValueType = undefined;

            if (@hasDecl(ValueType, "decompress")) {
                return ValueType.decompress(reader);
            }

            inline for (struct_info.fields) |field| {
                @field(result, field.name) = try deserializeValue(field.type, reader, allocator);
            }

            return result;
        },
        .@"enum" => |enum_info| {
            const raw_tag = try reader.takeInt(enum_info.tag_type, .little);

            return @enumFromInt(raw_tag);
        },
        .array => |array_info| {
            var result: ValueType = undefined;

            for (&result) |*element| {
                element.* = try deserializeValue(array_info.child, reader, allocator);
            }

            return result;
        },
        .pointer => |pointer_info| {
            if (pointer_info.size == .Slice and pointer_info.child == u8) {
                const slice_length = try reader.takeInt(u32, .little);
                const slice_buffer = try allocator.alloc(u8, slice_length);

                try reader.readSliceAll(slice_buffer);

                return slice_buffer;
            } else @compileError("[netling] deserialize does not support pointer type: " ++ @typeName(ValueType));
        },
        .optional => |optional_info| {
            const has_value = (try reader.takeByte()) != 0;

            if (has_value) {
                return try deserializeValue(optional_info.child, reader, allocator);
            }

            return null;
        },
        else => @compileError("[netling] deserialize does not support type: " ++ @typeName(ValueType)),
    }
}
