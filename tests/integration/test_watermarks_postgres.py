import os
from datetime import UTC, datetime

import psycopg
import pytest
from dotenv import load_dotenv

from pipeline.state.watermarks import (
    WatermarkAdvanceError,
    WatermarkRepository,
)

# Load the local database settings from .env.
#
# This integration test talks to the REAL pipeline-db container,
# so unlike our unit tests, it needs actual connection information.
load_dotenv()

def get_connection() -> psycopg.Connection:
    """
    Create a real connection to our local pipeline PostgreSQL database.

    We keep connection creation in one helper so individual tests
    don't need to repeat all of the environment-variable plumbing.
    """

    return psycopg.connect(
        dbname=os.environ["PIPELINE_DB_NAME"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
        host=os.environ["PIPELINE_DB_HOST"],
        port=os.environ["PIPELINE_DB_PORT"],
    )


def test_watermark_repository_against_postgres() -> None:
    """
    Verify the complete watermark lifecycle against real PostgreSQL.

    This test proves that:
      1. Our SQL is valid PostgreSQL.
      2. A brand-new source can be inserted.
      3. The transaction actually commits.
      4. ON CONFLICT updates an existing source.
      5. EXCLUDED.watermark_ts behaves as expected.
      6. A watermark cannot move backward.
      7. The stored run_id changes with the successful watermark.
    """

    # Use a test-only source name so this integration test never
    # modifies the real "openfoodfacts" pipeline state.
    source_name = "integration_test_watermark"

    first_watermark = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    second_watermark = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    try:
        # ----------------------------------------------------------
        # SETUP
        # ----------------------------------------------------------
        # Remove any leftover row from a previous test execution.
        #
        # Using a connection as a context manager means psycopg will:
        #   - commit if the block exits successfully
        #   - roll back if an exception escapes the block
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM control.source_watermarks
                    WHERE source_name = %s
                    """,
                    (source_name,),
                )


        # ----------------------------------------------------------
        # TEST 1: BRAND-NEW SOURCE
        # ----------------------------------------------------------
        with get_connection() as connection:
            repository = WatermarkRepository(
                connection=connection
            )

            # Because we deleted the test row above, the source should
            # not have any stored watermark yet.
            assert (
                repository.get_watermark(source_name)
                is None
            )

            # This should execute the INSERT portion of our UPSERT.
            repository.advance_watermark(
                source_name=source_name,
                new_watermark=first_watermark,
                run_id="run-001",
            )

        # Exiting the connection context above should COMMIT.
        #
        # We intentionally open a brand-new connection below.
        # If the previous transaction wasn't committed, this new
        # connection would not see the inserted row.
        with get_connection() as connection:
            repository = WatermarkRepository(
                connection=connection
            )

            assert (
                repository.get_watermark(source_name)
                == first_watermark
            )


        # ----------------------------------------------------------
        # TEST 2: ADVANCE AN EXISTING WATERMARK
        # ----------------------------------------------------------
        with get_connection() as connection:
            repository = WatermarkRepository(
                connection=connection
            )

            # This source already exists now, so PostgreSQL should hit:
            #
            #   ON CONFLICT (source_name)
            #   DO UPDATE ...
            #
            # Because Aug 20 > Aug 19, the WHERE condition should allow
            # the update.
            repository.advance_watermark(
                source_name=source_name,
                new_watermark=second_watermark,
                run_id="run-002",
            )

        # Again, use another connection to prove the update committed.
        with get_connection() as connection:
            repository = WatermarkRepository(
                connection=connection
            )

            assert (
                repository.get_watermark(source_name)
                == second_watermark
            )

            # Query the table directly so we also verify that the
            # successful update stored the new run_id.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT watermark_ts, run_id
                    FROM control.source_watermarks
                    WHERE source_name = %s
                    """,
                    (source_name,),
                )

                row = cursor.fetchone()

            assert row is not None
            assert row[0] == second_watermark
            assert row[1] == "run-002"


        # ----------------------------------------------------------
        # TEST 3: PREVENT BACKWARD MOVEMENT
        # ----------------------------------------------------------
        with get_connection() as connection:
            repository = WatermarkRepository(
                connection=connection
            )

            # Try to move the watermark backward from Aug 20 to Aug 19.
            #
            # PostgreSQL should evaluate:
            #
            # EXCLUDED.watermark_ts > existing watermark_ts
            #
            # as:
            #
            # Aug 19 > Aug 20
            #
            # which is FALSE.
            #
            # That means zero rows should be changed, which our
            # repository translates into WatermarkAdvanceError.
            with pytest.raises(
                WatermarkAdvanceError
            ):
                repository.advance_watermark(
                    source_name=source_name,
                    new_watermark=first_watermark,
                    run_id="run-old",
                )


        # ----------------------------------------------------------
        # TEST 4: VERIFY FAILED ADVANCE CHANGED NOTHING
        # ----------------------------------------------------------
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT watermark_ts, run_id
                    FROM control.source_watermarks
                    WHERE source_name = %s
                    """,
                    (source_name,),
                )

                row = cursor.fetchone()

            assert row is not None

            # The database should still contain the most recent
            # successful values.
            assert row[0] == second_watermark
            assert row[1] == "run-002"

    finally:
        # ----------------------------------------------------------
        # CLEANUP
        # ----------------------------------------------------------
        # Always remove the test row, even if one of the assertions
        # above fails.
        #
        # This keeps repeated test runs deterministic and prevents
        # test data from accumulating in the control table.
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM control.source_watermarks
                    WHERE source_name = %s
                    """,
                    (source_name,),
                )