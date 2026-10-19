#!/usr/bin/env bash
# Build on a RHEL 9 builder. No network downloads or privilege escalation here.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
cd "$root"
source /etc/os-release
if [[ ${VERSION_ID%%.*} != 9 ]]; then
    echo 'Build this RPM on RHEL 9 (or a compatible EL9 builder).' >&2; exit 2
fi
version=${VERSION:-0.1.0}
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'VERSION must be X.Y.Z' >&2; exit 2; }
for tool in git rpmbuild python3; do command -v "$tool" >/dev/null; done
cmake_bin=$(command -v "${CMAKE:-cmake}")
ctest_bin=$(dirname "$cmake_bin")/ctest
[[ -x $ctest_bin ]] || { echo 'ctest must be next to cmake' >&2; exit 2; }
python3 - "$cmake_bin" <<'PY'
import re, subprocess, sys
version = tuple(map(int, re.search(r'(\d+)\.(\d+)\.(\d+)', subprocess.check_output([sys.argv[1], '--version'], text=True)).groups()))
if version < (3, 27, 0):
    raise SystemExit('CMake >= 3.27 is required. Set CMAKE to an approved newer installation.')
PY
# git archive deliberately packages committed source only.
git diff --quiet HEAD -- || { echo 'Commit tracked changes before packaging.' >&2; exit 2; }
[[ -z $(git ls-files --others --exclude-standard) ]] || { echo 'Commit or ignore untracked files before packaging.' >&2; exit 2; }
revision=$(git rev-parse --short=12 HEAD)
top="$root/build/rpmbuild"
mkdir -p "$top"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
git archive --format=tar.gz --prefix="amqpprox-rewrite-$version/" HEAD > "$top/SOURCES/amqpprox-rewrite-$version.tar.gz"
cp packaging/rhel9/amqpprox-rewrite.spec "$top/SPECS/"
rpmbuild -ba "$top/SPECS/amqpprox-rewrite.spec" \
    --define "_topdir $top" --define "package_version $version" \
    --define "source_revision $revision" --define "cmake_binary $cmake_bin" \
    --define "ctest_binary $ctest_bin"
printf 'RPM and SRPM output: %s/RPMS and %s/SRPMS\n' "$top" "$top"
