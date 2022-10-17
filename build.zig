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
    const use_system_libressl = b.option(bool, "use-system-libressl", "Link and build from the system installed copy of LibreSSL instead of building it from source") orelse false;

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();
    const target = b.standardTargetOptions(.{});

    // const lib = b.addStaticLibrary("nats", "./src/conn.zig");
    // lib.setBuildMode(mode);
    // lib.install();

    // const sync_lib = b.addStaticLibrary("nats-sync", "./src/sync.zig");
    // sync_lib.setBuildMode(mode);
    // sync_lib.install();

    // var main_tests = b.addTest("conn.zig");
    // main_tests.setBuildMode(mode);
    // const test_step = b.step("test", "Run library tests");
    // test_step.dependOn(&main_tests.step);
    //
    const exe = b.addExecutable("ssl_connect", "examples/ssl_connect.zig");
    exe.linkLibC();
    exe.setTarget(target);
    exe.setBuildMode(mode);
    exe.addPackage(pkgs.libressl);
    exe.addIncludePath("/opt/homebrew/opt/libressl/include");
    exe.addLibraryPath("/opt/homebrew/opt/libressl/lib");
    exe.install();
    try zig_libressl.useLibreSslForStep(b, target, mode, "lib/zig-libressl/libressl", exe, use_system_libressl);

    const example_step = b.step("examples", "Build examples");
    inline for (.{
        //"pub",
        //"sub",
        //"test",
        //"ssl_connect",
        "sync_pub",
        "sync_sub",
        "ngs",
    }) |example_name| {
        const example = b.addExecutable(example_name, "examples/" ++ example_name ++ ".zig");
        //example.addPackage(pkgs.nats);
        example.addPackage(pkgs.nats_sync);
        try zig_libressl.useLibreSslForStep(b, target, mode, "lib/zig-libressl/libressl", example, use_system_libressl);
        example.addPackage(pkgs.libressl);
        example.addIncludePath("/opt/homebrew/opt/libressl/include");
        example.addLibraryPath("/opt/homebrew/opt/libressl/lib");
        example.setBuildMode(mode);
        example.setTarget(target);
        example.install();
        example_step.dependOn(&example.step);
    }
}
