#!/usr/bin/env python3
"""Check selected historical regressions in isolated source copies, never edit src/."""

import argparse
import json
import re
from pathlib import Path
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
DEFINITIONS = Path(__file__).with_name("mutants.json")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def php_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_case(php, directory, mutant, variant):
    path = directory / "src" / mutant["file"]
    class_name = "Componenta\\WebSocket\\" + mutant["file"][:-4].replace("/", "\\")
    bootstrap = directory / "bootstrap.php"
    bootstrap.write_text(
        "<?php\n"
        f"$loader = require {php_string(ROOT / 'vendor/autoload.php')};\n"
        f"$loader->setPsr4('Componenta\\\\WebSocket\\\\', [{php_string(directory / 'src')}]);\n"
        f"$actual = (new ReflectionClass({php_string(class_name)}))->getFileName();\n"
        f"if ($actual !== {php_string(path)}) {{ throw new RuntimeException('Wrong source loaded'); }}\n"
    )
    junit = directory / f"{variant}.xml"
    result = subprocess.run(
        [php, str(ROOT / "vendor/bin/pest"), str(ROOT / "tests" / mutant["test"]),
         "--bootstrap=" + str(bootstrap), "--filter=" + re.escape(mutant["filter"]),
         "--log-junit=" + str(junit), "--colors=never"],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    require(junit.is_file(), f"{mutant['id']}: missing JUnit report ({variant})\n{result.stdout}\n{result.stderr}")
    tests = ET.parse(junit).getroot().findall(".//testcase")
    require(tests, f"{mutant['id']}: no tests matched")
    require(all(not t.findall("skipped") for t in tests), f"{mutant['id']}: test skipped")
    names = [(t.get("classname"), t.get("name")) for t in tests]
    require(all(mutant["filter"].lower() in (name or "").lower() for _, name in names),
            f"{mutant['id']}: unrelated tests matched: {names}")
    failed = [t for t in tests if t.find("failure") is not None or t.find("error") is not None]
    if variant == "control":
        require(result.returncode == 0 and not failed,
                f"{mutant['id']}: control failed\n{result.stdout}\n{result.stderr}")
    else:
        require((result.returncode == 0) == (not failed),
                f"{mutant['id']}: runner failed outside the selected tests\n{result.stdout}\n{result.stderr}")
        if failed:
            print(result.stdout, flush=True)
    return names, len(failed)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--php", default="php")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    mutants = json.loads(DEFINITIONS.read_text())
    require(len({m["id"] for m in mutants}) == len(mutants), "Duplicate mutation id")
    results = []
    for mutant in mutants:
        with tempfile.TemporaryDirectory(prefix="websocket-mutation-") as temporary:
            directory = Path(temporary)
            shutil.copytree(ROOT / "src", directory / "src")
            source = directory / "src" / mutant["file"]
            original = source.read_text()
            require(original.count(mutant["original"]) == 1,
                    f"{mutant['id']}: mutation anchor absent or ambiguous; review the mutation")
            control_names, _ = run_case(args.php, directory, mutant, "control")
            source.write_text(original.replace(mutant["original"], mutant["replacement"], 1))
            lint = subprocess.run([args.php, "-l", str(source)], capture_output=True, text=True, timeout=10)
            require(lint.returncode == 0, f"{mutant['id']}: invalid PHP mutation\n{lint.stdout}\n{lint.stderr}")
            names, failed = run_case(args.php, directory, mutant, "mutant")
            require(names == control_names, f"{mutant['id']}: control and mutant selected different tests")
            status = "killed" if failed else "survived"
            results.append({"id": mutant["id"], "status": status, "tests": len(names), "failed": failed})
            print(f"{mutant['id']}: {status} ({failed}/{len(names)} failed)", flush=True)
    summary = {
        "total": len(results),
        "killed": sum(r["status"] == "killed" for r in results),
        "survived": sum(r["status"] == "survived" for r in results),
        "mutations": results,
    }
    print(json.dumps(summary, indent=2), flush=True)
    if args.report:
        args.report.write_text(json.dumps(summary, indent=2) + "\n")
    require(not summary["survived"], "Selected mutations survived; review test coverage")


if __name__ == "__main__":
    main()
