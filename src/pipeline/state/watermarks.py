from datetime import datetime
from typing import cast

from psycopg import Connection


class WatermarkAdvanceError(Exception):
    """
    Raised when a watermark cannot be advanced.

    In particular, we do not allow the watermark to move backward
    or remain at the same timestamp.
    """


class WatermarkRepository:
    """
    Read and update source watermarks stored in PostgreSQL.

    The repository pattern keeps database-specific SQL in one place.
    Other parts of the pipeline can work with Python methods such as
    get_watermark() and advance_watermark() without needing to know
    how the control table is implemented.
    """

    def __init__(
        self,
        connection: Connection,
    ) -> None:
        # Store an already-open PostgreSQL connection.
        #
        # We inject the connection instead of creating it inside every
        # method. This makes transaction handling clearer and also makes
        # this class much easier to unit test.
        self.connection = connection

    def get_watermark(
        self,
        source_name: str,
    ) -> datetime | None:
        """
        Return the current successful watermark for a source.

        If the source has never successfully completed an extraction,
        there will be no row in control.source_watermarks, so we return
        None instead of raising an error.
        """

        # Use a parameterized query rather than building SQL with an
        # f-string. psycopg safely sends source_name as a parameter,
        # avoiding SQL injection and quoting problems.
        query = """
            SELECT watermark_ts
            FROM control.source_watermarks
            WHERE source_name = %s
        """

        # cursor() gives us an object used to execute SQL and retrieve
        # query results.
        #
        # Using "with" ensures the cursor is closed automatically.
        with self.connection.cursor() as cursor:
            cursor.execute(
                query,
                (source_name,),
            )

            # fetchone() returns:
            #
            #   (datetime(...),)
            #
            # if a row exists, or:
            #
            #   None
            #
            # if no matching source exists.
            row = cursor.fetchone()

            if row is None:
                return None

            # The SELECT only requested one column, so the timestamp is the
            # first element of the returned tuple.
            return cast(datetime, row[0])

    def advance_watermark(
        self,
        source_name: str,
        new_watermark: datetime,
        run_id: str,
    ) -> None:
        """
        Insert or advance a source's successful watermark.

        For a brand-new source:
            INSERT a new control row.

        For an existing source:
            UPDATE the row only when new_watermark is GREATER than the
            currently stored watermark.

        This prevents accidental backward movement of pipeline state.
        """

        query = """
            INSERT INTO control.source_watermarks (
                source_name,
                watermark_ts,
                updated_at,
                run_id
            )
            VALUES (
                %s,
                %s,
                CURRENT_TIMESTAMP,
                %s
            )

            ON CONFLICT (source_name)
            DO UPDATE SET
                watermark_ts = EXCLUDED.watermark_ts,
                updated_at = CURRENT_TIMESTAMP,
                run_id = EXCLUDED.run_id

            WHERE
                EXCLUDED.watermark_ts
                > control.source_watermarks.watermark_ts
        """

        with self.connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    source_name,
                    new_watermark,
                    run_id,
                ),
            )

            # cursor.rowcount tells us how many database rows were
            # inserted or updated.
            #
            # Brand-new source:
            #     INSERT succeeds -> rowcount = 1
            #
            # Existing source with later timestamp:
            #     UPDATE succeeds -> rowcount = 1
            #
            # Existing source with same/older timestamp:
            #     WHERE condition blocks UPDATE -> rowcount = 0
            rows_changed = cursor.rowcount

        if rows_changed == 0:
            raise WatermarkAdvanceError(
                "Watermark was not advanced because the new "
                "timestamp was not later than the existing watermark."
            )


def reconcile_watermark(
    repository: WatermarkRepository,
    source_name: str,
    committed_watermark: datetime,
    run_id: str,
) -> bool:
    """
    Reconcile PostgreSQL watermark state with a committed raw run.

    Returns True if PostgreSQL had to be advanced.
    Returns False if PostgreSQL already matched or was ahead.
    """

    # Read the current high-water mark from PostgreSQL.
    current_watermark = repository.get_watermark(
        source_name
    )

    # If this source has no watermark yet, initialize it
    # from the successfully committed raw run.
    if current_watermark is None:
        repository.advance_watermark(
            source_name=source_name,
            new_watermark=committed_watermark,
            run_id=run_id,
        )
        return True

    # If PostgreSQL is behind raw storage, catch it up.
    if current_watermark < committed_watermark:
        repository.advance_watermark(
            source_name=source_name,
            new_watermark=committed_watermark,
            run_id=run_id,
        )
        return True

    # If PostgreSQL already matches or is ahead, do nothing.
    # Never move a high-water mark backward.
    return False