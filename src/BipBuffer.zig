const std = @import("std");
const Thread = std.Thread;

// references:
//   https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist
//   https://ferrous-systems.com/blog/lock-free-ring-buffer/

pub fn BipBuffer(buflen: usize) type {
    return struct {
        buffer: [buflen]u8 = undefined,
        buflen: usize = buflen,

        w: usize = 0, // writer position
        r: usize = 0, // reader position
        h: usize = buflen, // high water mark

        const Self = @This();

        // returns writable part of the buffer
        // can be greater or lower than the required len size
        // it's up to the caller to check that
        pub fn writable(b: *Self, len: usize) []const u8 {
            if (b.r <= b.w) {
                if ((b.w + len < b.buflen) // is there enough space at the end of the buffer
                or (b.buflen - b.w >= b.r) // or wrapping is useless
                ) {
                    return b.buffer[b.w..];
                }
                // wrap writer position
                b.h = b.w; // set hwm
                b.w = 0;
            }
            return b.buffer[b.w..b.r];
        }

        // confirmation that len part of the writable buffer is written
        pub fn written(b: *Self, len: usize) void {
            b.w += len;
        }

        // returns readable buffer part, zero size if there is nothing new
        pub fn readable(b: *Self) []const u8 {
            if (b.w >= b.r) {
                return b.buffer[b.r..b.w];
            }
            if (b.r == b.h) {
                b.r = 0; // wrap reader position
                return b.buffer[b.r..b.w];
            }
            return b.buffer[b.r..b.h];
        }

        // confirmation that the len part of the readable buffer is processed
        pub fn read(b: *Self, len: usize) void {
            b.r += len;
        }
    };
}

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

test "w>r" {
    const len = 128;
    var b = BipBuffer(len){};
    try expect(b.w == 0);
    try expect(b.r == 0);
    try expect(b.buflen == len);
    try expect(b.h == len);

    try expect(b.writable(10).len == 128);
    try expect(b.writable(0).len == 128);
    try expect(b.writable(256).len == 128);
    try expect(b.readable().len == 0);

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

    // wrapping requested more then is free at the end
    try expect(b.writable(64).len == 65);
    try expect(b.w == 0);
    try expect(b.r == 65);
    try expect(b.h == 65);
    try expect(b.readable().len == 0);
    try expect(b.r == 0);

    b.written(10);
    try expect(b.w == 10);
    try expect(b.r == 0);
    try expect(b.h == 65);
    try expect(b.readable().len == 10);
}
