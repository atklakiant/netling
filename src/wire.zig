const compress = @import("compress.zig");
const std = @import("std");

pub const PacketFlags = packed struct(u8) {
    rle_compressed: bool = false,
    bitpacked: bool = false,
    directed: bool = false,
    padding: u5 = 0,
};

pub const WireHeader = extern struct {
    event_identifier: u16,
    payload_length: u32,
    flags: PacketFlags,
};

pub const maximum_packet_payload_size: usize = 65536;
pub const wire_header_size: usize = @sizeOf(WireHeader);

pub fn methodToFlags(method: compress.Method) PacketFlags {
    return switch (method) {
        .none => PacketFlags{},
        .rle => PacketFlags{ .rle_compressed = true },
        .bitpack => PacketFlags{ .bitpacked = true },
        .rle_then_bitpack => PacketFlags{ .rle_compressed = true, .bitpacked = true },
    };
}

pub fn flagsToMethod(flags: PacketFlags) compress.Method {
    if (flags.rle_compressed and flags.bitpacked) return .rle_then_bitpack;
    if (flags.rle_compressed) return .rle;
    if (flags.bitpacked) return .bitpack;

    return .none;
}
