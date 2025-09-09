"""
This module contains the main orchestration logic for the ETL process.

It handles file discovery, parallel processing, and the overall workflow,
separating these concerns from the CLI interface.
"""
import logging
from .config import Settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_etl(settings: Settings, mode: str, max_workers: int | None = None):
    """
    The main entry point for the ETL orchestration.

    This function discovers files, filters out already processed ones (in delta mode),
    and processes the remaining files in parallel.

    Args:
        settings: The application configuration.
        mode: The load mode ('delta' or 'full').
        max_workers: The maximum number of processes to use. Defaults to CPU count.
    """
    logger.info(f"Starting ETL process in '{mode}' mode...")
    logger.info(f"Schema type: {settings.schema_type}")
    logger.info(f"Source URI: {settings.source_uri}")

    # Step 1: Discover files using fsspec
    all_files = discover_files(settings.source_uri)
    logger.info(f"Found {len(all_files)} files at source.")

    # Step 2: Filter out completed files based on their hash (only in delta mode)
    if mode == "delta":
        files_to_process_map = filter_completed_files(all_files, settings)
    elif mode == "full":
        logger.info("Full load mode: all discovered files will be processed.")
        # In full mode, we still need the file hashes for logging.
        files_to_process_map = {
            file_path: _calculate_file_hash(file_path) for file_path in all_files
        }
    else:
        raise ValueError(f"Unknown load mode: {mode}")

    logger.info(f"{len(files_to_process_map)} new files to process.")

    # Step 3: Process files in parallel
    if files_to_process_map:
        process_files_parallel(files_to_process_map, settings, mode, max_workers)

    logger.info("ETL process finished.")


import fsspec
import hashlib
from . import loader

def _calculate_file_hash(file_path: str) -> str:
    """Calculates the SHA-256 hash of a file's content."""
    hasher = hashlib.sha256()
    with fsspec.open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def discover_files(uri: str) -> list[str]:
    """
    Discovers files from a given URI, supporting local paths, glob patterns,
    and cloud storage URIs.
    """
    if not uri:
        logger.warning("No source_uri provided. No files to process.")
        return []
    try:
        # open_files is the correct fsspec function for handling globs
        # and returning file-like objects. We just need their paths.
        file_objects = fsspec.open_files(uri)
        return [f.path for f in file_objects]
    except Exception as e:
        logger.error(f"Failed to discover files at URI: {uri}. Error: {e}")
        # In case of a fatal error during discovery, raise it to stop the CLI
        raise e


def filter_completed_files(files: list[str], settings: Settings) -> dict[str, str]:
    """
    Filters a list of files, removing those that have already been processed.

    Args:
        files: A list of file paths to check.
        settings: The application configuration.

    Returns:
        A dictionary mapping the file paths of files that need to be processed
        to their corresponding SHA-256 hash.
    """
    if not files:
        return {}

    try:
        db_loader = loader.get_loader(settings.database.dsn)
        completed_hashes = db_loader.get_completed_file_hashes()
        logger.info(f"Found {len(completed_hashes)} completed files in the database.")
    except Exception as e:
        logger.error(f"Could not connect to the database to get completed files. Aborting. Error: {e}")
        return {}

    files_to_process = {}
    for file_path in files:
        try:
            logger.info(f"Calculating hash for {file_path}...")
            file_hash = _calculate_file_hash(file_path)
            if file_hash not in completed_hashes:
                files_to_process[file_path] = file_hash
            else:
                logger.info(f"Skipping '{file_path}' as it has already been processed (hash: {file_hash[:7]}...).")
        except FileNotFoundError:
            logger.error(f"File not found during hashing: {file_path}. Skipping.")
        except Exception as e:
            logger.error(f"Error hashing file {file_path}: {e}. Skipping.")

    return files_to_process


import concurrent.futures
from . import parser, transformer, loader
from typing import Tuple

def process_files_parallel(
    files_map: dict[str, str], settings: Settings, mode: str, max_workers: int | None = None
):
    """
    Processes a dictionary of files in parallel using a process pool.
    """
    logger.info(f"Starting parallel processing with max_workers={max_workers or 'default'}.")
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks to the executor
        future_to_file = {
            executor.submit(process_file, path, hash_val, settings, mode): path
            for path, hash_val in files_map.items()
        }

        success_count = 0
        failure_count = 0
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result, message = future.result()
                if result:
                    logger.info(f"Successfully processed {file_path}: {message}")
                    success_count += 1
                else:
                    logger.error(f"Failed to process {file_path}: {message}")
                    failure_count += 1
            except Exception as exc:
                logger.error(f"{file_path} generated an exception: {exc}", exc_info=True)
                failure_count += 1

    logger.info(f"Parallel processing finished. Success: {success_count}, Failures: {failure_count}.")

    if failure_count > 0:
        raise RuntimeError(f"{failure_count} file(s) failed to process.")


import os
import json
from datetime import datetime


def _process_normalized_file(
    f, db_loader, file_path, file_hash, mode
) -> Tuple[bool, str]:
    """Helper to process a file for the 'normalized' schema."""
    parsed_stream = parser.parse_icsr_xml(f)
    buffers, counts, errors = transformer.transform_and_normalize(parsed_stream)

    if errors:
        logger.warning(
            f"Encountered {len(errors)} parsing errors in {file_path}. See quarantine."
        )
        # Future enhancement: send errors to a quarantine queue/location

    db_loader.load_normalized_data(
        buffers=buffers,
        row_counts=counts,
        load_mode=mode,
        file_path=file_path,
        file_hash=file_hash,
    )
    total_rows = sum(counts.values())
    return True, f"Loaded {total_rows} rows into normalized schema."


def _process_audit_file(
    f, db_loader, file_path, file_hash, mode
) -> Tuple[bool, str]:
    """Helper to process a file for the 'audit' schema."""
    parsed_stream = parser.parse_icsr_xml_for_audit(f)
    buffer, count = transformer.transform_for_audit(parsed_stream)
    db_loader.load_audit_data(
        buffer=buffer,
        row_count=count,
        load_mode=mode,
        file_path=file_path,
        file_hash=file_hash,
    )
    return True, f"Loaded {count} records into audit schema."


def process_file(
    file_path: str, file_hash: str, settings: Settings, mode: str
) -> Tuple[bool, str]:
    """
    Processes a single file: opens, parses, transforms, and loads its data.
    This function is designed to be run in a separate process. It acts as a
    dispatcher to schema-specific processing functions.
    """
    logger.info(f"Worker started for file: {file_path}")
    try:
        # The loader must be instantiated within the worker process
        db_loader = loader.get_loader(settings.database.dsn)

        with fsspec.open(file_path, "rb") as f:
            if settings.schema_type == "normalized":
                return _process_normalized_file(f, db_loader, file_path, file_hash, mode)
            elif settings.schema_type == "audit":
                return _process_audit_file(f, db_loader, file_path, file_hash, mode)
            else:
                # This case should ideally be caught earlier, but serves as a safeguard
                raise ValueError(
                    f"Invalid schema_type in worker: {settings.schema_type}"
                )

    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}", exc_info=True)
        if settings.quarantine_uri:
            try:
                protocol = fsspec.utils.get_protocol(file_path)
                fs = fsspec.filesystem(protocol)
                base_filename = os.path.basename(file_path)
                dest_path = os.path.join(settings.quarantine_uri, base_filename)
                meta_path = dest_path + ".meta.json"

                # Ensure the quarantine directory exists.
                fs.makedirs(settings.quarantine_uri, exist_ok=True)

                # Create metadata content
                error_meta = {
                    "failed_at": datetime.utcnow().isoformat(),
                    "source_file": file_path,
                    "file_hash": file_hash,
                    "error_message": str(e),
                }
                # Write metadata file to quarantine
                with fs.open(meta_path, "w") as meta_f:
                    json.dump(error_meta, meta_f, indent=2)
                logger.info(f"Wrote failure metadata to: {meta_path}")

                # Move the actual failed file
                fs.mv(file_path, dest_path)
                logger.info(f"Moved failed file to quarantine: {dest_path}")

            except Exception as q_exc:
                logger.error(f"Could not move file to quarantine. Error: {q_exc}", exc_info=True)
                raise q_exc

        return False, str(e)
