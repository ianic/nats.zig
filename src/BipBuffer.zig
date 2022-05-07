const std = @import("std");
const Thread = std.Thread;

// references:
//   https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist
//   https://ferrous-systems.com/blog/lock-free-ring-buffer/

pub fn BipBuffer(buflen: usize) type {
    return struct {
        buffer: [buflen]u8 = undefined,
        buflen: usize = buflen,

        // a buffer
        a_pos: usize = 0,
        a_len: usize = 0,

        // b buffer - alway start at the head
        b_len: usize = 0,

        // reserved buffer part
        r_pos: usize = 0,
        r_len: usize = 0,

        pub fn writeReserve(self: *Self, len: usize) Error![]const u8 {
            if (self.r_len > 0) {
                return Error.NestedReserve; // another reserve before commit of the previous
            }
            if (len > self.buflen) {
                return Error.Overflow;
            }
            if (self.b_len > 0) {
                // reserve on top of b
                if (len + self.b_len >= self.a.pos) {
                    return Error.Overflow; // no space to allocate
                }
                self.r_pos = self.b_len;
                self.r_len = len;
                return self.reservedBuf();
            }

            if (self.a_pos > 0) {
                if (len + self.a_pos + self.a_len > self.buflen) {
                    return Error.Overflow;
                }
                // reserve on top of a
                self.r_pos = self.a_pos + self.a_len;
                self.r_len = len;
                return self.reservedBuf();
            }
            // reserve from the head
            self.r_pos = 0;
            self.r_len = len;
            return self.reservedBuf();
        }

        fn reservedBuf(self: *Self) []const u8 {
            return self.buffer[self.r_pos .. self.r_pos + self.r_len];
        }

        fn resetReserved(self: *Self) void {
            self.r_pos = 0;
            self.r_len = 0;
        }

        pub fn writeCommit(len: usize) Error!void {
            if (self.r_len < len) {
                return Error.BadRequest;
            }
            defer self.resetReserved();

            if (self.b_len > 0) {
                self.b_len += len;
                return;
            }
            self.a_len += len;
        }

        pub fn read() ?[]const u8 {
            if (self.a_len == 0) {
                return null;
            }
            return self.buffer[self.a_pos .. self.a_pos + self.a_len];
        }

        pub fn readCommit(len: usize) !void {
            if (self.a_len < len) {
                return Error.BadRequest;
            }
            self.a_pos += len;
            self.a_len -= len;
            if (self.a_len == 0) {
                self.a_pos = 0;
                if (self.b_len > 0) {
                    // promote b to a
                    self.a_len = self.b_len;
                    self.b_len = 0;
                }
            }
        }
    };
}
