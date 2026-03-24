"""One-shot helper to run datalad save via labpy env."""
import os
import subprocess
import sys

LABPY_BIN = "/storage/share/python/environments/Anaconda3/envs/labpy/bin"
DATALAD = os.path.join(LABPY_BIN, "datalad")
REPO = "/storage2/arash/projects/projio/packages/pipeio"
MSG = "pipeio: Phase 4 notebook lifecycle, docs collect, mod discovery, BidsResolver, contracts CLI"

env = os.environ.copy()
env["PATH"] = LABPY_BIN + ":" + env.get("PATH", "")

r = subprocess.run([DATALAD, "save", "-d", REPO, "-m", MSG], capture_output=True, text=True, env=env)
print(r.stdout)
if r.stderr:
    print(r.stderr, file=sys.stderr)
sys.exit(r.returncode)
