const std = @import("std");

pub const Method = enum {
    none,
    rle,
    bitpack,
    rle_then_bitpack,
};

pub const RunLength = struct {
    pub fn encode(input: []const u8, output: []u8) usize {
        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index < input.len) {
            const current_byte = input[input_index];
            var run_length: usize = 1;

            while (input_index + run_length < input.len and
                input[input_index + run_length] == current_byte and
                run_length < 255)
            {
                run_length += 1;
            }

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

    pub fn decode(input: []const u8, output: []u8) usize {
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

pub const Bitpack = struct {
    pub fn encode(input: []const u8, output: []u8) usize {
        if (input.len % 4 != 0) {
            const copy_length = @min(input.len, output.len);

            @memcpy(output[0..copy_length], input[0..copy_length]);

            return copy_length;
        }

        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index + 4 <= input.len) {
            const value = std.mem.readInt(u32, input[input_index..][0..4], .little);
            const bytes_needed: u8 = if (value <= 0xFF) 1 else if (value <= 0xFFFF) 2 else if (value <= 0xFFFFFF) 3 else 4;

            if (output_index + 1 + bytes_needed > output.len) break;

            output[output_index] = bytes_needed;
            output_index += 1;

            for (0..bytes_needed) |byte_index| {
                output[output_index] = @intCast((value >> @intCast(byte_index * 8)) & 0xFF);
                output_index += 1;
            }

            input_index += 4;
        }

        return output_index;
    }

    pub fn decode(input: []const u8, output: []u8) usize {
        var input_index: usize = 0;
        var output_index: usize = 0;

        while (input_index < input.len) {
            if (input_index + 1 > input.len) break;

            const bytes_needed = input[input_index];
            var value: u32 = 0;

            input_index += 1;

            if (bytes_needed == 0 or bytes_needed > 4) break;
            if (input_index + bytes_needed > input.len) break;
            if (output_index + 4 > output.len) break;

            for (0..bytes_needed) |byte_index| {
                value |= @as(u32, input[input_index + byte_index]) << @intCast(byte_index * 8);
            }

            input_index += bytes_needed;

            std.mem.writeInt(u32, output[output_index..][0..4], value, .little);

            output_index += 4;
        }

        return output_index;
    }
};

pub fn compressWithMethod(method: Method, input: []const u8, output: []u8) usize {
    switch (method) {
        .none => {
            const copy_length = @min(input.len, output.len);
            @memcpy(output[0..copy_length], input[0..copy_length]);
            return copy_length;
        },
        .rle => {
            return RunLength.encode(input, output);
        },
        .bitpack => {
            return Bitpack.encode(input, output);
        },
        .rle_then_bitpack => {
            var intermediate_buffer: [8192]u8 = undefined;
            const rle_length = RunLength.encode(input, &intermediate_buffer);

            return Bitpack.encode(intermediate_buffer[0..rle_length], output);
        },
    }
}

pub fn decompressWithMethod(method: Method, input: []const u8, output: []u8) usize {
    switch (method) {
        .none => {
            const copy_length = @min(input.len, output.len);

            @memcpy(output[0..copy_length], input[0..copy_length]);

            return copy_length;
        },
        .rle => {
            return RunLength.decode(input, output);
        },
        .bitpack => {
            return Bitpack.decode(input, output);
        },
        .rle_then_bitpack => {
            var intermediate_buffer: [8192]u8 = undefined;
            const bitpack_length = Bitpack.decode(input, &intermediate_buffer);

            return RunLength.decode(intermediate_buffer[0..bitpack_length], output);
        },
    }
}
