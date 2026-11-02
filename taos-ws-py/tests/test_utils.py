# Copyright (c) 2026 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from utils import *


def test_generate_totp_seed():
    seed = generate_totp_seed(16)
    assert len(seed) == 16
    assert any(c.islower() for c in seed)
    assert any(c.isupper() for c in seed)
    assert any(c.isdigit() for c in seed)


def test_generate_totp_code():
    secret = generate_totp_secret(b"12345678901234567890")
    code = generate_totp_code(secret)
    assert len(code) == 6
    assert code.isdigit()


def test_hotp():
    counter = 1765854733 // 30
    digits = 6
    assert hotp(generate_totp_secret(b"12345678901234567890"), counter, digits) == 383089
    assert hotp(generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz"), counter, digits) == 269095
    assert hotp(generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~"), counter, digits) == 203356


def test_generate_totp_secret():
    secret1 = generate_totp_secret(b"12345678901234567890")
    totp_secret1 = totp_secret_encode(secret1)
    assert totp_secret1 == "VR62SA7EK3RP7MRTH7QXSIVZXXS57OY2SRUMGLKDJPREZ62OHFEQ"
    assert totp_secret_decode(totp_secret1) == secret1

    secret2 = generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz")
    totp_secret2 = totp_secret_encode(secret2)
    assert totp_secret2 == "OMYPD744HIZB2KZPAUNLEWUFBNRQBILEPWD2FGPUDYCZMFTCRFXQ"
    assert totp_secret_decode(totp_secret2) == secret2

    secret3 = generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~")
    totp_secret3 = totp_secret_encode(secret3)
    assert totp_secret3 == "FURKOZ6REIGLQHP5OMKLZZFUQNOCRDJPCOSDP5ESH2VR3IU7NAYA"
    assert totp_secret_decode(totp_secret3) == secret3
