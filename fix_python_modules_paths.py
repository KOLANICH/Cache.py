import sys
import os
from pathlib import Path
import sysconfig
import re

"""This tool makes available packages installed via apt to python installed in the original Docker image in /usr/local"""

stdlibDir = Path(sysconfig.get_paths()["platstdlib"])

cextNameRx = re.compile("^(?P<name>.+)\\.(?P<impl>cpython)-(?P<major>\\d)(?P<minor>\\d)m?-(?P<arch>x86_64)-(?P<abi>linux-gnu)\\.so$")


def genNewName(name, impl, major, minor, arch, abi):
	return name + "." + impl + "-" + str(major) + str(minor) + ("m" if (major, minor) < (3, 8) else "") + "-" + arch + "-" + abi + ".so"


def symlink(f, to):
	print(f, "->", to)
	os.symlink(f, to)


def genNewSoPath(parent, name):
	m = cextNameRx.match(name)
	if m:
		d = m.groupdict()
		d["major"] = sys.version_info[0]
		d["minor"] = sys.version_info[1]
		return parent / genNewName(**d)
	else:
		return parent / name


def fixSoPath(f):
	to = genNewSoPath(f.parent, f.name)
	if not to.exists():
		symlink(f, to)


def fixPaths(fromAptPackagesInstalledPythonModulesDir):
	for f in fromAptPackagesInstalledPythonModulesDir.glob("*"):
		to = stdlibDir / f.name
		if f.is_dir():
			for l in f.glob("**/*.so"):
				fixSoPath(l)
		elif f.suffix == ".so":
			to = genNewSoPath(stdlibDir, f.name)
		if not to.exists():
			symlink(f, to)


if __name__ == "__main__":
	print("stdlibDir:", stdlibDir)
	fixPaths(Path("/usr/lib/python3/dist-packages/"))
