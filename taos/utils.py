import time
import uuid
import os
from threading import Lock

tUUIDHashId = 0
tUUIDSerialNo = 0


def gen_req_id():
    global tUUIDHashId
    global tUUIDSerialNo
    if tUUIDHashId == 0:
        uid = str(uuid.uuid4()).encode('utf-8')
        tUUIDHashId = murmurhash3_32(uid, len(uid))

    while True:
        ts = int(time.time()) >> 8
        pid = get_pid()
        with Lock():
            tUUIDSerialNo += 1
            val = tUUIDSerialNo

        req_id = ((tUUIDHashId & 0x07FF) << 52) | ((pid & 0x0F) << 48) | ((ts & 0x3FFFFFF) << 20) | (val & 0xFFFFF)
        if req_id:
            break

    return req_id


def murmurhash3_32(key, length):
    data = key
    nblocks = length >> 2
    h1 = 0x12345678
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    blocks = data
    for i in range(-nblocks, 0):
        k1 = blocks[i]
        k1 *= c1
        k1 = rotl32(k1, 15)
        k1 *= c2
        h1 ^= k1
        h1 = rotl32(h1, 13)
        h1 = h1 * 5 + 0xe6546b64
    tail = data[nblocks * 4:]
    k1 = 0
    # print('length', length, length & 3)
    if length & 3 == 3:
        k1 ^= tail[2] << 16
    if length & 3 == 2:
        k1 ^= tail[1] << 8
    if length & 3 == 1:
        k1 ^= tail[0]
        k1 *= c1
        k1 = rotl32(k1, 15)
        k1 *= c2
        h1 ^= k1
    h1 ^= length
    fmix32(h1)
    return h1


def rotl32(x, r):
    return (x << r) | (x >> (32 - r))


def fmix32(h):
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    return h


def get_pid():
    return os.getpid()
