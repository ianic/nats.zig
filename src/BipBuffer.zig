const std = @import("std");
const Thread = std.Thread;

// references:
//   https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist
//   https://ferrous-systems.com/blog/lock-free-ring-buffer/

buffer: []u8,
buflen: usize,

w: usize = 0, // writer position
r: usize = 0, // reader position
h: usize, // high water mark

const Self = @This();

pub fn init(buf: []u8) Self {
    return .{
        .buffer = buf,
        .buflen = buf.len,
        .h = buf.len,
    };
}

// returns writable part of the buffer
// can be greater or lower than the required len size
// it's up to the caller to check that
pub fn writable(b: *Self, len: usize) []u8 {
    var r = @atomicLoad(usize, &b.r, .SeqCst);
    var w = @atomicLoad(usize, &b.w, .SeqCst);

    if (r <= w) {
        if ((w + len < b.buflen) // is there enough space at the end of the buffer
        or (b.buflen - w >= r) // or wrapping is useless
        ) {
            return b.buffer[w..];
        }
        // wrap writer position
        @atomicStore(usize, &b.h, w, .SeqCst); // b.h = b.w; // set hwm
        w = 0;
        @atomicStore(usize, &b.w, w, .SeqCst); // b.w = 0;
    }
    return b.buffer[w .. r - 1];
}

// confirmation that len part of the writable buffer is written
pub fn written(b: *Self, len: usize) void {
    _ = @atomicRmw(usize, &b.w, .Add, len, .SeqCst); // b.w += len
}

// returns readable buffer part, zero size if there is nothing new
pub fn readable(b: *Self) []u8 {
    var r = @atomicLoad(usize, &b.r, .SeqCst);
    var w = @atomicLoad(usize, &b.w, .SeqCst);
    var h = @atomicLoad(usize, &b.h, .SeqCst);

    if (r == h and r > w) {
        // wrap reader position
        // b.h = buflen // reset hvm
        if (@cmpxchgWeak(usize, &b.h, h, b.buflen, .SeqCst, .SeqCst)) |v| {
            _ = v;
            return b.buffer[0..0];
        }
        r = 0;
        @atomicStore(usize, &b.r, r, .SeqCst); // b.r = 0; // wrap reader position
    }
    if (r <= w) {
        return b.buffer[r..w];
    }
    if (r <= h) {
        return b.buffer[r..h];
    }
    return b.buffer[0..0];
}

pub fn empty(b: *Self) bool {
    var r = @atomicLoad(usize, &b.r, .SeqCst);
    var w = @atomicLoad(usize, &b.w, .SeqCst);
    var h = @atomicLoad(usize, &b.h, .SeqCst);

    return r == w and h == b.buflen;
}

pub fn copy(dst: *Self, src: *Self) void {
    while (!src.empty()) {
        var src_buf = src.readable();
        var ln = src_buf.len;
        var dst_buf = dst.writable(ln);
        if (dst_buf.len < ln) {
            unreachable;
        }
        std.mem.copy(u8, dst_buf, src_buf);
        dst.written(ln);
        src.read(ln);
    }
}

// confirmation that the len part of the readable buffer is processed
pub fn read(b: *Self, len: usize) void {
    _ = @atomicRmw(usize, &b.r, .Add, len, .SeqCst); //b.r += len;
}

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

test "init" {
    const len = 128;
    var buf: [len]u8 = undefined;
    var b = init(&buf);

    try expect(b.w == 0);
    try expect(b.r == 0);
    try expect(b.buflen == len);
    try expect(b.h == len);

    try expect(b.writable(0).len == 128);
    try expect(b.writable(10).len == 128);
    try expect(b.writable(256).len == 128);
    try expect(b.readable().len == 0);
}

test "w>r" {
    const len = 128;
    var buf: [len]u8 = undefined;
    var b = init(&buf);

    b.written(10);
    try expect(b.w == 10);
    try expect(b.r == 0);
    try expect(b.h == len);
    try expect(b.writable(10).len == 118);
    try expect(b.writable(0).len == 118);
    try expect(b.writable(256).len == 118);
    try expect(b.readable().len == 10);

    b.written(55);
    try expect(b.w == 65);
    try expect(b.r == 0);
    try expect(b.h == len);
    try expect(b.writable(10).len == 63);
    try expect(b.readable().len == 65);

    try expect(b.writable(64).len == 63);
    try expect(b.writable(100).len == 63);
    try expect(b.readable().len == 65);
    b.read(15);
    try expect(b.w == 65);
    try expect(b.r == 15);
    try expect(b.h == len);

    try expect(b.readable().len == 50);
    try expect(b.writable(100).len == 63);
    b.read(50);
    try expect(b.w == 65);
    try expect(b.r == 65);
    try expect(b.h == len);
}

test "r>w" {
    const len = 128;
    var buf: [len]u8 = undefined;
    var b = init(&buf);
    b.w = 10;
    b.r = 80;
    b.h = 100;

    try expect(b.writable(50).len == 70 - 1);
    try expect(b.writable(100).len == 70 - 1);
    try expect(b.readable().len == 20);

    b.written(20);
    try expect(b.w == 30);
    try expect(b.r == 80);
    try expect(b.h == 100);
    try expect(b.writable(40).len == 50 - 1);
    try expect(b.writable(100).len == 50 - 1);
    try expect(b.readable().len == 20);

    b.read(10);
    try expect(b.w == 30);
    try expect(b.r == 90);
    try expect(b.h == 100);
    try expect(b.readable().len == 10);

    b.read(10);
    try expect(b.w == 30);
    try expect(b.r == 100);
    try expect(b.h == 100);

    // warapping reader
    try expect(b.readable().len == 30);
    try expect(b.w == 30);
    try expect(b.r == 0);
    try expect(b.h == 128);
}

test "wrapping" {
    const len = 128;
    var buf: [len]u8 = undefined;
    var b = init(&buf);
    b.w = 65;
    b.r = 65;
    b.h = len;

    // writer wrap, requested more then there is at the right of the w
    try expect(b.writable(64).len == 65 - 1);
    try expect(b.w == 0);
    try expect(b.r == 65);
    try expect(b.h == 65);

    // reader wrap
    try expect(b.readable().len == 0);
    try expect(b.r == 0);

    b.written(10);
    try expect(b.w == 10);
    try expect(b.r == 0);
    try expect(b.h == 128);
    try expect(b.readable().len == 10);
}

test "using with different buffer sizes" {
    const alloc = std.testing.allocator;

    var i: usize = 6;
    while (i < 20) : (i += 1) {
        var buf = try alloc.alloc(u8, i);
        var b = init(buf);
        defer alloc.free(buf);

        var j: usize = 0;
        while (j < 21) : (j += 1) {
            //_ = b.readable();
            //std.debug.print("w:{d} r:{d} h:{d}\n", .{ b.w, b.r, b.h });
            var wb = b.writable(3);
            try expect(wb.len >= 3);
            std.mem.copy(u8, wb, "123");
            b.written(3);
            try expect(b.readable().len == 3);

            //std.debug.print("w:{d} r:{d} h:{d}\n", .{ b.w, b.r, b.h });
            wb = b.writable(2);
            try expect(wb.len >= 2);
            std.mem.copy(u8, wb, "45");
            b.written(2);

            wb = b.readable();
            //std.debug.print("w:{d} r:{d} h:{d}\n", .{ b.w, b.r, b.h });
            //std.debug.print("i:{d} j:{d} wb:{d}:{s}\n", .{ i, j, wb.len, wb });
            if (wb.len == 5) {
                try expect(wb.len == 5);
                try expect(std.mem.eql(u8, wb, "12345"));
                b.read(5);
            } else {
                try expect(wb.len == 3);
                try expect(std.mem.eql(u8, wb, "123"));
                b.read(3);
                wb = b.readable();
                try expect(wb.len == 2);
                try expect(std.mem.eql(u8, wb, "45"));
                b.read(2);
            }
        }
    }
}

test "writer/reader thread synchonization" {
    const Context = struct {
        done: bool = false,
        event: std.Thread.AutoResetEvent = std.Thread.AutoResetEvent{},
        writes: u16 = 0,
        reads: u16 = 0,

        const RndGen = std.rand.DefaultPrng;
        const Context = @This();

        fn writer(self: *Context) !void {
            var rnd = RndGen.init(@intCast(u64, std.time.timestamp()));

            var i: u16 = 0;
            while (i < 1024) : (i += 1) {
                var rn = rnd.random().uintLessThan(u64, 128);
                std.time.sleep(rn);
                //std.debug.print("+\n", .{});
                self.writes += 1;
                self.event.set();
            }
            @atomicStore(bool, &self.done, true, .SeqCst);
        }

        fn reader(self: *Context) !void {
            var rnd = RndGen.init(@intCast(u64, std.time.timestamp()));

            while (true) {
                self.event.wait();
                //std.debug.print("-\n", .{});
                self.reads += 1;
                var rn = rnd.random().uintLessThan(u64, 128);
                std.time.sleep(rn);
                var done = @atomicLoad(bool, &self.done, .SeqCst);
                if (done) {
                    break;
                }
            }
        }
    };

    var context = Context{};
    const writer_thread = try std.Thread.spawn(.{}, Context.reader, .{&context});
    const reader_thread = try std.Thread.spawn(.{}, Context.writer, .{&context});

    writer_thread.join();
    reader_thread.join();

    std.debug.print("writes: {d}, reads: {d}\n", .{ context.writes, context.reads });
}

fn bug_r_w_on_sam_pos() !void {
    const len = 4;
    var buf: [len]u8 = undefined;
    var b = init(&buf);

    try expect(b.w == 0);
    try expect(b.r == 0);
    try expect(b.h == 4);
    try expect(b.writable(2).len == 4);
    try expect(b.readable().len == 0);

    b.written(2);
    try expect(b.writable(2).len == 2);
    try expect(b.readable().len == 2);
    b.written(2);
    try expect(b.writable(2).len == 0);
    try expect(b.readable().len == 4);
    b.read(2);
    try expect(b.writable(2).len == 2);
    try expect(b.readable().len == 2);
    b.written(2);

    try expect(b.w == 2);
    try expect(b.r == 2);
    try expect(b.h == 4);
    try expect(b.readable().len == 0);
}

test "buf fix" {
    try std.testing.expectError(error.TestUnexpectedResult, bug_r_w_on_sam_pos());
}

test "copy" {
    var buf: [4]u8 = undefined;
    var b = init(&buf);
    var wb = b.writable(3);
    std.mem.copy(u8, wb, "123");
    b.written(3);

    var buf2: [8]u8 = undefined;
    var b2 = init(&buf2);
    b2.copy(&b);

    var wb2 = b2.readable();
    try expect(wb2.len == 3);
    try expect(std.mem.eql(u8, wb2, "123"));
}
