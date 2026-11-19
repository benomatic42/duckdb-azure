#!/usr/bin/env python3
"""Reads of an open file are pinned to the version (ETag) seen when it was opened.

Usage: check_read_etag.py <azure.duckdb_extension> [--azurite <connection string> <container>]

A file read in several requests (a Parquet footer, then row groups) must not mix two versions when another writer
replaces it in between: the second read fails with "ETag ... changed after it was opened", unless
azure_unsafe_disable_etag_checks is set. Uses the C++ driver and the DFS/Blob mock of test/null_if_exists: the driver
opens the file with DIRECT_IO (one request per read), reads the first bytes, waits, then reads the rest; the file is
replaced while it waits (in the mock, or through a second driver process against Azurite).
"""

import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import http.server
import urllib.parse
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "null_if_exists"))
import check_null_if_exists as nie  # noqa: E402


def read_across_replace(runner, url, replace, extra_setup=""):
    """Read url's first 4 bytes, call replace(), read the rest; returns the driver's output."""
    setup = runner.setup
    if extra_setup:
        setup = pathlib.Path(str(runner.setup) + ".extra.sql")
        setup.write_text(runner.setup.read_text() + extra_setup)
    proc = subprocess.Popen(
        [runner.driver, runner.extension, str(setup), "read2", url, "4"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    lines = []
    for line in proc.stdout:
        lines.append(line.strip())
        if line.strip() == "ready" or line.startswith("error"):
            break
    if lines and lines[-1] == "ready":
        replace()
        proc.stdin.write("\n")
        proc.stdin.flush()
    out, err = proc.communicate(timeout=120)
    lines += [line.strip() for line in out.splitlines()]
    got = " ".join(line for line in lines if line != "ready")
    return got + (f" | exit={proc.returncode}" if proc.returncode else "")


def cases(run, runner, base, replace_fn):
    out = []

    def check(name, got, want):
        ok = want(got) if callable(want) else got == want
        out.append((name, got, ok))

    changed = lambda got: "changed after it was opened" in got and "exit=1" in got  # noqa: E731

    url = base + "same.txt"
    run("put", url, "AAAA1111")
    check("unchanged: reads one version", read_across_replace(runner, url, lambda: None), "first=AAAA rest=1111")

    url = base + "replaced.txt"
    run("put", url, "AAAA1111")
    got = read_across_replace(runner, url, lambda: replace_fn(url, "BBBB2222"))
    check("replaced mid-read: fails", got, changed)
    check("replaced mid-read: the new version is there", run("cat", url), "size=8 content=BBBB2222")

    url = base + "unsafe.txt"
    run("put", url, "AAAA1111")
    got = read_across_replace(
        runner, url, lambda: replace_fn(url, "BBBB2222"), "SET azure_unsafe_disable_etag_checks = true;\n"
    )
    check("replaced mid-read, checks disabled: mixes versions", got, "first=AAAA rest=2222")
    return out


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    extension, azurite = args[0], None
    if len(args) >= 4 and args[1] == "--azurite":
        azurite = (args[2], args[3])

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), nie.Mock)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    endpoint = f"http://127.0.0.1:{server.server_address[1]}/{nie.ACCT}"
    mock_cs = (
        f"DefaultEndpointsProtocol=http;AccountName={nie.ACCT};AccountKey=a2V5;"
        f"BlobEndpoint={endpoint};DfsEndpoint={endpoint}"
    )

    def mock_replace(url, data):
        nie.STORE.replace(urllib.parse.unquote(urllib.parse.urlsplit(url).path).lstrip("/"), data.encode())

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        driver = nie.build_driver(extension, tmp)
        run = nie.Runner(driver, extension, mock_cs, tmp)
        for prefix in (f"abfss://fs@{nie.ACCT}.dfs.core.windows.net/", "abfss://fs/", "az://fs/"):
            base = prefix + f"t-{uuid.uuid4().hex[:8]}/"
            results += [(f"mock {prefix}: {n}", g, ok) for n, g, ok in cases(run, run, base, mock_replace)]
        if azurite:
            connection_string, container = azurite
            run = nie.Runner(driver, extension, connection_string, tmp)
            base = f"az://{container}/read-etag-{uuid.uuid4().hex[:8]}/"
            replace = lambda url, data: run("put", url, data)  # noqa: E731
            results += [(f"azurite az://: {n}", g, ok) for n, g, ok in cases(run, run, base, replace)]
    server.shutdown()

    failures = 0
    for name, got, ok in results:
        print(f"{'ok  ' if ok else 'FAIL'} {name}" + ("" if ok else f"\n  got: {got!r}"))
        failures += not ok
    print(f"{len(results) - failures}/{len(results)} passed" + ("" if azurite else "; Azurite not run (no --azurite)"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
