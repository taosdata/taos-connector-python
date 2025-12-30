import base64
import hashlib
import hmac
import os
import random
import string
import struct
import subprocess
import time
import toml


def get_connector_info():
    cargo_toml = toml.load(os.path.join(os.path.dirname(__file__), "../Cargo.toml"))
    version = cargo_toml["package"]["version"]

    git_commit_id = os.getenv("GIT_COMMIT_ID")
    if not git_commit_id:
        git_commit_id = (
            subprocess.check_output(
                ["git", "rev-parse", "--short=7", "HEAD"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
            .decode()
            .strip()
        )
    if not git_commit_id:
        git_commit_id = "ncid000"

    return f"python-ws-v{version}-{git_commit_id}"


LOWERCASE = string.ascii_lowercase
UPPERCASE = string.ascii_uppercase
DIGITS = string.digits
ALL_CHARS = LOWERCASE + UPPERCASE + DIGITS


def generate_totp_seed(length: int) -> str:
    assert length >= 3
    buf = [
        random.choice(LOWERCASE),
        random.choice(UPPERCASE),
        random.choice(DIGITS),
    ]
    buf += [random.choice(ALL_CHARS) for _ in range(3, length)]
    random.shuffle(buf)
    return "".join(buf)


def generate_totp_secret(seed: bytes) -> bytes:
    return hmac.new(b"", seed, hashlib.sha256).digest()


def generate_totp_code(secret: bytes) -> str:
    now = int(time.time())
    counter = now // 30
    code = hotp(secret, counter, 6)
    return f"{code:06d}"


def hotp(secret: bytes, counter: int, digits: int) -> int:
    counter_bytes = struct.pack(">Q", counter)
    mac = hmac.new(secret, counter_bytes, hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    code = struct.unpack(">I", mac[offset : offset + 4])[0] & 0x7FFFFFFF
    return code % (10 ** min(digits, 8))


def totp_secret_encode(secret: bytes) -> str:
    return base64.b32encode(secret).decode("utf-8").rstrip("=")


def totp_secret_decode(secret: str) -> bytes:
    missing_padding = len(secret) % 8
    if missing_padding:
        secret += "=" * (8 - missing_padding)
    return base64.b32decode(secret, casefold=True)


# Test functions below


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
