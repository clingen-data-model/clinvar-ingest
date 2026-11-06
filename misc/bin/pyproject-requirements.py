"""
Print the dependencies declared in pyproject.toml, one requirement per line, for
installing them before the package source is copied into an image.

Usage: python pyproject-requirements.py [EXTRAS] [PYPROJECT]
  EXTRAS     optional dependency groups in pip's bracket form, e.g. "[vrsify]" (default: none)
  PYPROJECT  path to pyproject.toml (default: ./pyproject.toml)
"""

import sys
import tomllib

extras_arg = sys.argv[1] if len(sys.argv) > 1 else ""
pyproject_path = sys.argv[2] if len(sys.argv) > 2 else "pyproject.toml"  # noqa: PLR2004

with open(pyproject_path, "rb") as f:
    project = tomllib.load(f)["project"]

extras = [name.strip() for name in extras_arg.strip().strip("[]").split(",") if name.strip()]
optional = project.get("optional-dependencies", {})
unknown = [name for name in extras if name not in optional]
if unknown:
    sys.exit(f"{pyproject_path} has no optional dependencies named {', '.join(unknown)}")

requirements = list(project.get("dependencies", []))
for name in extras:
    requirements.extend(optional[name])
print("\n".join(requirements))
