const std = @import("std");
const Mpsc = @import("channel.zig").Mpsc;
const Thread = std.Thread;

const EventTag = enum {
    foo,
    bar,
    signal,
};

const Event = union(EventTag) {
    foo: usize,
    bar: usize,
    signal: usize,
};

const ch = struct {
    var channel = Mpsc(Event, 16){};
    var threads: usize = 0;
    var mut: Thread.Mutex = Thread.Mutex{};

    pub fn aa() void {}

    pub fn sendFoo(i: usize) !void {
        try channel.send(.{ .foo = i });
    }
    pub fn sendBar(i: usize) !void {
        try channel.send(.{ .bar = i });
    }
    pub fn sendSignal(i: usize) !void {
        try channel.send(.{ .signal = i });
    }
    pub fn done() void {
        mut.lock();
        defer mut.unlock();
        threads -= 1;
        if (threads == 0) {
            channel.close();
        }
    }
};

pub fn main() !void {
    try setSignalHandler();

    var trd1 = try Thread.spawn(.{}, count2, .{ ch.sendFoo, ch.done, 100 });
    //var trd2 = try Thread.spawn(.{}, count2, .{ ch.sendBar, ch.done, 200 });
    var trd3 = try Thread.spawn(.{}, count3, .{ struct {
        const send = ch.sendBar;
        const done = ch.done;
    }, 200 });
    ch.threads = 2;

    while (ch.channel.recv()) |event| {
        switch (event) {
            .foo => |i| std.debug.print("foo {d}\n", .{i}),
            .bar => |i| std.debug.print("bar {d}\n", .{i}),
            .signal => |i| {
                std.debug.print("signal {d}\n", .{i});
                ch.channel.close();
            },
        }
    }

    Thread.join(trd1);
    //Thread.join(trd2);
    Thread.join(trd3);
}

fn count2(comptime send: anytype, comptime done: anytype, sleep: usize) void {
    defer done();
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        send(i) catch {
            break;
        };
        std.time.sleep(sleep * std.time.ns_per_ms);
    }
}

fn count3(c: anytype, sleep: usize) void {
    defer c.done();
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        c.send(i) catch {
            break;
        };
        std.time.sleep(sleep * std.time.ns_per_ms);
    }
}

// fn count(comptime tag: EventTag, sleep: usize) void {
//     var i: u8 = 0;
//     while (i < 10) : (i += 1) {
//         var e: Event = .{ ."@tagName(tag)" = i};
//         @field(e, @tagName(tag)) = i;
//         channel.send(e);
//         //std.debug.print("{s} {d}\n", .{ name, i });
//         std.time.sleep(sleep * std.time.ns_per_ms);
//     }
// }

// TODO: how to to this without global var signal_target
fn setSignalHandler() !void {
    var act = std.os.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask = std.os.empty_sigset,
        .flags = 0,
    };
    try std.os.sigaction(std.os.SIG.INT, &act, null);
    try std.os.sigaction(std.os.SIG.TERM, &act, null);
    try std.os.sigaction(std.os.SIG.USR1, &act, null);
    try std.os.sigaction(std.os.SIG.USR2, &act, null);
}

fn sigHandler(sig: c_int) callconv(.C) void {
    ch.sendSignal(@intCast(usize, sig)) catch {};
}
