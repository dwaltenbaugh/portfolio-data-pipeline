"""
Local command-line entry point for the Open Food Facts raw pipeline.

The actual workflow lives in the installable pipeline package so the
same code can be invoked from the CLI, Airflow, tests, or another
containerized runtime.
"""

from pipeline.jobs.openfoodfacts_raw import (
    run_openfoodfacts_raw,
)


if __name__ == "__main__":
    run_openfoodfacts_raw()