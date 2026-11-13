"""Print the locked hgraph version and hg-oap git revision as GITHUB_OUTPUT lines."""

import tomllib
from pathlib import Path

lock = tomllib.loads(Path("uv.lock").read_text())
packages = {p["name"]: p for p in lock["package"]}

hgraph = packages["hgraph"]["version"]
hg_oap = packages["hg-oap"]
git_url = hg_oap.get("source", {}).get("git", "")
hg_oap_rev = git_url.rpartition("#")[2][:10] if "#" in git_url else hg_oap["version"]

print(f"hgraph={hgraph}")
print(f"hg_oap={hg_oap['version']}@{hg_oap_rev}")
