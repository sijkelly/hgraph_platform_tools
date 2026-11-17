"""Print the hgraph version pinned in uv.lock as a GITHUB_OUTPUT line.

hg-oap is not pinned here: pyproject.toml installs it from the sibling ../hg_oap checkout,
and the hg_oap fork check workflow tracks it instead.
"""

import tomllib
from pathlib import Path

lock = tomllib.loads(Path("uv.lock").read_text())
packages = {p["name"]: p for p in lock["package"]}

print(f"hgraph={packages['hgraph']['version']}")
