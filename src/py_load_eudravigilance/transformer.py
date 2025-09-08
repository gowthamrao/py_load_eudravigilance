"""
Handles the Transformation phase of the ETL process.

This module takes the parsed XML data from the Parser and transforms it
into a relational format suitable for bulk loading into the target database.
"""

import csv
import io
from typing import Any, Dict, Generator, IO


def transform_to_csv_buffer(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> io.StringIO:
    """
    Transforms a generator of ICSR dictionaries into an in-memory CSV buffer.

    This function creates an intermediate representation of the data that is
    optimized for native bulk loading utilities like PostgreSQL's COPY command.

    Args:
        icsr_generator: A generator that yields dictionaries, each representing
                        a parsed ICSR.

    Returns:
        An `io.StringIO` object containing the data in CSV format with a header.
        If the generator is empty, the buffer will also be empty.
    """
    buffer = io.StringIO()
    writer = None

    for icsr_dict in icsr_generator:
        if writer is None:
            # First item: Create the DictWriter and write the header.
            # The fieldnames are derived from the keys of the first dictionary.
            fieldnames = list(icsr_dict.keys())
            writer = csv.DictWriter(buffer, fieldnames=fieldnames)
            writer.writeheader()

        # Write the current row to the buffer.
        writer.writerow(icsr_dict)

    # Ensure the buffer's position is at the beginning before it's read.
    buffer.seek(0)
    return buffer
