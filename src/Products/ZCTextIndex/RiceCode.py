##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""Rice coding (a variation of Golomb coding)

Based on a Java implementation by Glen McCluskey described in a Usenix
 ;login: article at
http://www.usenix.org/publications/login/2000-4/features/java.html

McCluskey's article explains the approach as follows.  The encoding
for a value x is represented as a unary part and a binary part.  The
unary part is a sequence of 1 bits followed by a 0 bit.  The binary
part encodes some of the lower bits of x-1.

The encoding is parameterized by a value m that describes how many
bits to store in the binary part.  If most of the values are smaller
than 2**m then they can be stored in only m+1 bits.

Compute the length of the unary part, q, where
   q = math.floor((x-1)/ 2 ** m)

   Emit q 1 bits followed by a 0 bit.

Emit the lower m bits of x-1, treating x-1 as a binary value.
"""

import array


class BitArray:

    def __init__(self, buf=None):
        self.bytes = array.array('B')
        self.nbits = 0
        self.bitsleft = 0
        self.tostring = self.bytes.tostring

    def __getitem__(self, i):
        byte, offset = divmod(i, 8)
        mask = 2 ** offset
        if self.bytes[byte] & mask:
            return 1
        else:
            return 0

    def __setitem__(self, i, val):
        byte, offset = divmod(i, 8)
        mask = 2 ** offset
        if val:
            self.bytes[byte] |= mask
        else:
            self.bytes[byte] &= ~mask

    def __len__(self):
        return self.nbits

    def append(self, bit):
        """Append a 1 if bit is true or 1 if it is false."""
        if self.bitsleft == 0:
            self.bytes.append(0)
            self.bitsleft = 8
        self.__setitem__(self.nbits, bit)
        self.nbits += 1
        self.bitsleft -= 1

    def __getstate__(self):
        return (self.nbits, self.bitsleft, self.tostring())

    def __setstate__(self, value):
        nbits, bitsleft, s = value
        self.bytes = array.array('B', s)
        self.nbits = nbits
        self.bitsleft = bitsleft


class RiceCode:

    def __init__(self, m):
        """Constructor a RiceCode for m-bit values."""
        if not (0 <= m <= 16):
            raise ValueError("m must be between 0 and 16")
        self.init(m)
        self.bits = BitArray()
        self.len = 0

    def init(self, m):
        self.m = m
        self.lower = (1 << m) - 1
        self.mask = 1 << (m - 1)

    def append(self, val):
        """Append an item to the list."""
        if val < 1:
            raise ValueError("value >= 1 expected, got %r" % val)
        val -= 1
        # emit the unary part of the code
        q = val >> self.m
        for i in range(q):
            self.bits.append(1)
        self.bits.append(0)
        # emit the binary part
        r = val & self.lower
        mask = self.mask
        while mask:
            self.bits.append(r & mask)
            mask >>= 1
        self.len += 1

    def __len__(self):
        return self.len

    def tolist(self):
        """Return the items as a list."""
        result = []
        i = 0  # bit offset
        binary_range = range(self.m)
        for j in range(self.len):
            unary = 0
            while self.bits[i] == 1:
                unary += 1
                i += 1
            assert self.bits[i] == 0
            i += 1
            binary = 0
            for k in binary_range:
                binary = (binary << 1) | self.bits[i]
                i += 1
            result.append((unary << self.m) + (binary + 1))
        return result

    def tostring(self):
        """Return a binary string containing the encoded data.

        The binary string may contain some extra zeros at the end.
        """
        return self.bits.tostring()

    def __getstate__(self):
        return (self.m, self.bits)

    def __setstate__(self, value):
        m, bits = value
        self.init(m)
        self.bits = bits


def encode(m, l):  # noqa: E741
    c = RiceCode(m)
    for elt in l:
        c.append(elt)
    assert c.tolist() == l
    return c


def encode_deltas(l):  # noqa: E741
    if len(l) == 1:
        return l[0], []
    deltas = RiceCode(6)
    deltas.append(l[1] - l[0])
    for i in range(2, len(l)):
        deltas.append(l[i] - l[i - 1])
    return l[0], deltas


def decode_deltas(start, enc_deltas):
    deltas = enc_deltas.tolist()
    result = [start]
    for i in range(1, len(deltas)):
        result.append(result[i - 1] + deltas[i])
    result.append(result[-1] + deltas[-1])
    return result
