const std = @import("std");

pub fn build(builder: *std.Build) void {
    const target = builder.standardTargetOptions(.{});
    const optimize = builder.standardOptimizeOption(.{});

    const zstd = builder.dependency("zstd", .{
        .target = target,
        .optimize = optimize,
    });

    const netling = builder.addModule("netling", .{
        .optimize = optimize,
        .target = target,

        .root_source_file = builder.path("src/root.zig"),
    });

    netling.linkLibrary(zstd.artifact("zstd"));
}
