const std = @import("std");
const zig_libressl = @import("lib/zig-libressl/build.zig");
const Pkg = std.build.Pkg;

pub const pkgs = struct {
    pub const libressl = Pkg{
        .name = "libressl",
        .source = std.build.FileSource.relative("lib/zig-libressl/src/main.zig"),
    };

    pub const nats = Pkg{
        .name = "nats",
        .source = .{ .path = "src/conn.zig" },
    };

    pub const nats_sync = Pkg{
        .name = "nats-sync",
        .source = .{ .path = "src/sync.zig" },
        .dependencies = &[_]Pkg{
            libressl,
        },
    };
};

pub fn build(b: *std.build.Builder) !void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();
    const target = b.standardTargetOptions(.{});

    const lib = b.addStaticLibrary("nats", "./src/conn.zig");
    lib.setBuildMode(mode);
    lib.install();

    const sync_lib = b.addStaticLibrary("nats-sync", "./src/sync.zig");
    sync_lib.setBuildMode(mode);
    sync_lib.install();

    // var main_tests = b.addTest("conn.zig");
    // main_tests.setBuildMode(mode);
    // const test_step = b.step("test", "Run library tests");
    // test_step.dependOn(&main_tests.step);

    const example_step = b.step("examples", "Build examples");
    inline for (.{
        "pub",
        "sub",
        "test",
        "ssl_connect",
        "sync_pub",
        "sync_sub",
        "ngs",
    }) |example_name| {
        const example = b.addExecutable(example_name, "examples/" ++ example_name ++ ".zig");
        try zig_libressl.useLibreSslForStep(b, target, mode, "lib/zig-libressl/libressl", example, false);
        example.addPackage(pkgs.nats);
        example.addPackage(pkgs.nats_sync);
        example.addPackage(pkgs.libressl);
        example.setBuildMode(mode);
        example.setTarget(target);
        example.install();
        example_step.dependOn(&example.step);
    }
}
