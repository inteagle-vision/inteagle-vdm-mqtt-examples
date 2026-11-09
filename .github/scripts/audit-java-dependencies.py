#!/usr/bin/env python3
"""SDK 维护者依赖审计：只发送公开 Maven 坐标到 OSV，不读取源码或认证配置。"""
import argparse
import datetime
import json
from pathlib import Path
import re
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", default="target/dependency-audit.json")
parser.add_argument(
    "--extra", action="append", default=[], help="额外的 group:artifact:version，例如升级前版本"
)
args = parser.parse_args()
root = Path(__file__).resolve().parents[2] / "java"
packages = set()
for listing in root.glob("*/target/runtime-dependencies.txt"):
    for line in listing.read_text().splitlines():
        match = re.match(
            r"\s+([^\s:]+):([^:]+):jar:([^:]+):(compile|runtime)(?:\s|$)", line
        )
        if match and not match[1].startswith("com.inteagle"):
            packages.add((match[1] + ":" + match[2], match[3]))
for extra in args.extra:
    group, artifact, version = extra.split(":")
    packages.add((group + ":" + artifact, version))
if not packages:
    raise SystemExit("没有依赖清单，请先运行 mvn -f java/pom.xml dependency:list -DincludeScope=runtime -DoutputFile=target/runtime-dependencies.txt")
ordered = sorted(packages)
body = {
    "queries": [
        {"package": {"ecosystem": "Maven", "name": name}, "version": version}
        for name, version in ordered
    ]
}
request = urllib.request.Request(
    "https://api.osv.dev/v1/querybatch",
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json"},
)
# 请求失败时直接失败，不能把无法联网误报为没有漏洞。
with urllib.request.urlopen(request, timeout=60) as response:
    results = json.load(response)["results"]
if len(results) != len(ordered):
    raise SystemExit("OSV 返回数量不匹配，审计未完成")
report = {
    "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "source": "https://api.osv.dev/v1/querybatch",
    "scope": "resolved Maven runtime dependencies; excludes build plugins, OS and broker images",
    "packages": [],
}
count = 0
for (name, version), result in zip(ordered, results):
    vulnerabilities = result.get("vulns", [])
    report["packages"].append(
        {"name": name, "version": version, "advisories": vulnerabilities}
    )
    if vulnerabilities:
        count += len(vulnerabilities)
        print(name, version, ", ".join(v["id"] for v in vulnerabilities))
output = Path(args.output)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
print(
    f"Checked {len(ordered)} package versions; {count} advisory matches; report: {output}"
)
raise SystemExit(1 if count else 0)
