"""Launcher for the ingest job.

Databricks runs a spark_python_task by executing this file directly, so the
relative imports inside the ingest package are unavailable to it. Putting src/
on sys.path and delegating to the real entry point means the job executes the
same code, through the same argparse interface, as

    uv run python -m ingest.main --landing-root ...

does locally. There is deliberately no logic here beyond that.
"""

import sys
from pathlib import Path

# Databricks runs a spark_python_task via exec(compile(...)) inside an ipykernel,
# where __file__ is never bound. sys.argv[0] is populated there - it has to be,
# since the task's parameters reach argparse through sys.argv - so it gives the
# script's location in both contexts. globals().get() avoids the NameError that
# referencing __file__ directly would raise.
_src = Path(globals().get("__file__", sys.argv[0])).resolve().parent
sys.path.insert(0, str(_src))

from ingest.main import main  # noqa: E402  - must follow the sys.path edit


from ingest.main import main  # noqa: E402  - must follow the sys.path edit

if __name__ == "__main__":
    main()
