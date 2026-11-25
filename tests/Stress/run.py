#!/usr/bin/env python3
"""Bounded Linux loopback stress test; independent WebSocket wire implementation."""

import argparse
import json
import os
from pathlib import Path
import re
import selectors
import socket
import struct
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
ACCEPT = b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def frame(opcode, payload=b"", final=True):
    first = (0x80 if final else 0) | opcode
    size = len(payload)
    if size < 126:
        header = bytes((first, 0x80 | size))
    elif size <= 65535:
        header = bytes((first, 0xFE)) + struct.pack("!H", size)
    else:
        header = bytes((first, 0xFF)) + struct.pack("!Q", size)
    mask = os.urandom(4)
    masked = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
    return header + mask + masked


class Client:
    def __init__(self, address):
        self.sock = socket.create_connection(address, timeout=5)
        self.reader = self.sock.makefile("rb")
        try:
            host, port = address
            self.sock.sendall(
                f"GET /ws HTTP/1.1\r\nHost: {host}:{port}\r\n"
                "Connection: Upgrade\r\nUpgrade: websocket\r\n"
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                "Sec-WebSocket-Version: 13\r\n\r\n".encode("ascii")
            )
            status = self.reader.readline(8193)
            require(status.startswith(b"HTTP/1.1 101 "), f"Upgrade failed: {status!r}")
            headers = {}
            total = len(status)
            while True:
                line = self.reader.readline(8193)
                total += len(line)
                require(line and total <= 8192, "Invalid upgrade response headers")
                if line == b"\r\n":
                    break
                name, value = line.split(b":", 1)
                headers[name.lower()] = value.strip()
            require(headers.get(b"sec-websocket-accept") == ACCEPT, "Invalid accept digest")
        except BaseException:
            self.abort()
            raise

    def exact(self, size):
        data = self.reader.read(size)
        require(len(data) == size, f"Short server read: {len(data)} of {size}")
        return data

    def receive(self, opcode, expected):
        first, second = self.exact(2)
        require(first == (0x80 | opcode) and not second & 0x80, "Invalid server frame header")
        size = second & 0x7F
        if size == 126:
            size = struct.unpack("!H", self.exact(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self.exact(8))[0]
        require(size == len(expected), "Unexpected server frame size")
        require(self.exact(size) == expected, "Unexpected server frame payload")

    def send(self, opcode, data=b"", final=True):
        self.sock.sendall(frame(opcode, data, final))

    def close(self):
        try:
            self.send(8, b"\x03\xe8")
            self.receive(8, b"\x03\xe8")
            require(self.reader.read(1) == b"", "Server did not close the TCP stream")
        finally:
            self.abort()

    def abort(self):
        self.reader.close()
        self.sock.close()


def process_sample(pid):
    status = Path(f"/proc/{pid}/status").read_text()
    rss = re.search(r"^VmRSS:\s+(\d+)\s+kB$", status, re.M)
    require(rss is not None, "Linux RSS measurement unavailable")
    return {"rss_kib": int(rss.group(1)), "fds": len(list(Path(f"/proc/{pid}/fd").iterdir()))}


def run(seconds, php):
    require(seconds >= 1, "Duration must be positive")
    with tempfile.TemporaryDirectory(prefix="websocket-stress-") as tmp:
        stats_path = Path(tmp) / "stats.json"
        env = os.environ.copy()
        env["WEBSOCKET_STRESS_STATS"] = str(stats_path)
        with (Path(tmp) / "server.log").open("w+") as errors:
            process = subprocess.Popen(
                [php, "-d", "memory_limit=256M", str(ROOT / "tests/Support/echo-server.php")],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=errors, text=True,
            )
            persistent = []
            churn = []
            connected = messages = clean = abnormal = cycles = 0
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    require(selector.select(timeout=10), "Server readiness timed out")
                ready = process.stdout.readline().strip()
                match = re.fullmatch(r"READY (127\.0\.0\.1):(\d+)", ready)
                require(match is not None, f"Unexpected readiness: {ready!r}")
                address = (match.group(1), int(match.group(2)))
                idle = process_sample(process.pid)
                for _ in range(64):
                    persistent.append(Client(address))
                    connected += 1

                def cycle(index):
                    nonlocal connected, messages, clean, abnormal
                    for round_id in range(4):
                        payloads = [
                            struct.pack("!III", index, round_id, i)
                            + bytes([i]) * (8192 if round_id == 3 else 64)
                            for i in range(len(persistent))
                        ]
                        for client, payload in zip(persistent, payloads):
                            client.send(2, payload)
                        for client, payload in zip(persistent, payloads):
                            client.receive(2, payload)
                            messages += 1
                    for _ in range(16):
                        churn.append(Client(address))
                        connected += 1
                    for i, client in enumerate(churn):
                        payload = struct.pack("!II", index, i) + b"fragment" * 128
                        client.send(2, payload[:23], False)
                        client.send(9, b"check")
                        client.send(0, payload[23:])
                        client.receive(10, b"check")
                        client.receive(2, payload)
                        messages += 1
                    for i, client in enumerate(churn):
                        if i % 4 == 0:
                            client.abort()
                            abnormal += 1
                        else:
                            client.close()
                            clean += 1
                    churn.clear()
                    # A persistent client confirms the event loop is still responsive.
                    persistent[0].send(1, b"barrier")
                    persistent[0].receive(1, b"barrier")
                    messages += 1

                for i in range(5):
                    cycle(i)
                warm = process_sample(process.pid)
                samples = [warm]
                started = time.monotonic()
                while time.monotonic() - started < seconds or cycles < 50:
                    require(process.poll() is None, "Server exited during stress")
                    require(time.monotonic() - started < seconds + 90, "Stress failed to reach minimum cycles")
                    cycle(cycles + 5)
                    cycles += 1
                    if cycles % 10 == 0:
                        samples.append(process_sample(process.pid))
                    # Avoid exhausting the local ephemeral port range during connection churn.
                    time.sleep(0.05)

                elapsed = time.monotonic() - started
                for client in persistent:
                    client.close()
                    clean += 1
                persistent.clear()

                deadline = time.monotonic() + 5
                while True:
                    final = process_sample(process.pid)
                    if final["fds"] <= idle["fds"] or time.monotonic() >= deadline:
                        break
                    time.sleep(0.02)

                process.terminate()
                require(process.wait(timeout=10) == 0, "Server did not exit cleanly")
                actual = json.loads(stats_path.read_text())
                expected = {
                    "connected": connected, "disconnected": connected, "clean": clean,
                    "abnormal": abnormal, "received": messages, "failures": 0, "registry_count": 0,
                }
                for name, value in expected.items():
                    require(actual.get(name) == value, f"{name}: expected {value}, got {actual.get(name)}")
                peak = max(sample["rss_kib"] for sample in samples + [final])
                peak_fds = max(sample["fds"] for sample in samples)
                result = {
                    "duration_seconds": round(elapsed, 3), "measured_cycles": cycles,
                    "persistent_connections": 64, "churn_per_cycle": 16, "warmup_cycles": 5,
                    "idle": idle, "after_warmup": warm, "final": final,
                    "sampled_peak_rss_kib": peak, "sampled_peak_fds": peak_fds,
                    "server": actual,
                }
                print(json.dumps(result, indent=2), flush=True)
                require(final["fds"] <= idle["fds"], "File descriptors leaked")
                require(peak_fds <= idle["fds"] + 64 + 4, "File descriptors accumulated after churn")
                require(peak - warm["rss_kib"] <= 16 * 1024, "RSS grew more than 16 MiB after warmup")
            except BaseException:
                errors.flush()
                errors.seek(0)
                print(errors.read(), flush=True)
                raise
            finally:
                for client in persistent + churn:
                    client.abort()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
                if process.stdout is not None:
                    process.stdout.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--php", default="php")
    arguments = parser.parse_args()
    run(arguments.seconds, arguments.php)
