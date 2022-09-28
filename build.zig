const std = @import("std");

pub fn build(b: *std.build.Builder) void {
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

    var main_tests = b.addTest("conn.zig");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    const example_step = b.step("examples", "Build examples");
    inline for (.{
        "pub",
        "sub",
        "test",
        //        "connect",
        "sync_pub",
        "sync_sub",
    }) |example_name| {
        const example = b.addExecutable(example_name, "examples/" ++ example_name ++ ".zig");
        example.addPackagePath("nats", "./src/conn.zig");
        example.addPackagePath("nats-sync", "./src/sync.zig");
        example.setBuildMode(mode);
        example.setTarget(target);
        example.install();
        example_step.dependOn(&example.step);
    }
}
