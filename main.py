"""
Email Processing System - Main Orchestrator
Processes 170M+ email records with GPU acceleration

Usage:
    python main.py --use-gpu --workers 16
    python main.py --test  # Process only first 5 files as test
"""
# Suppress CuPy CUDA path warning before any import that might load cupy
import warnings
warnings.filterwarnings("ignore", message="CUDA path could not be detected", module="cupy._environment")

import sys
import os
import argparse
import logging
import tempfile
import pickle
import uuid
import signal
import atexit
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from tqdm import tqdm
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import threading

# Global executor reference for cleanup
_executor = None
_shutdown_requested = False

def _cleanup_handler(signum=None, frame=None):
    """Handle Ctrl+C by shutting down executor and exiting cleanly."""
    global _shutdown_requested, _executor
    if _shutdown_requested:
        print("\n[FORCE EXIT] Killing all processes...")
        os._exit(1)
    _shutdown_requested = True
    print("\n[SHUTDOWN] Ctrl+C received, stopping workers...")
    if _executor:
        _executor.shutdown(wait=False, cancel_futures=True)
    sys.exit(1)

# Register signal handlers
signal.signal(signal.SIGINT, _cleanup_handler)
signal.signal(signal.SIGTERM, _cleanup_handler)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    BASE_DIR, LOG_DIR, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT,
    CPU_WORKERS, USE_GPU, BATCH_SIZE_LOAD, WORKER_TEMP_DIR
)

# Checkpoint file for resume support
CHECKPOINT_FILE = Path(__file__).parent / '.processed_files.txt'

def load_checkpoint() -> set:
    """Load list of already processed files."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_checkpoint(filename: str):
    """Add a file to the checkpoint list."""
    with open(CHECKPOINT_FILE, 'a') as f:
        f.write(filename + '\n')

def clear_checkpoint():
    """Clear the checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
from database import (
    initialize_database, create_indexes, drop_indexes,
    get_email_count, get_category_counts, get_source_counts,
    get_provider_counts, get_brand_counts, get_quality_distribution,
    close_connection_pool
)
from parser import discover_files
from categorizer import get_category_stats
from deduplicator import IncrementalDeduplicator
from loader import load_records_batch, log_final_stats, LoaderProgress

# Check GPU availability
try:
    import cupy as cp
    GPU_AVAILABLE = True
    GPU_NAME = "RTX 5080"  # You can detect this dynamically if needed
except ImportError:
    GPU_AVAILABLE = False
    GPU_NAME = "N/A"


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    
    # Create log directory if needed
    LOG_DIR.mkdir(exist_ok=True)
    
    level = logging.DEBUG if verbose else logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from other libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('chardet').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# =============================================================================
# PARALLEL FILE PROCESSING
# =============================================================================

def process_single_file(
    file_info: Dict[str, Any],
    use_temp_file: bool = False,
    status_dict: Any = None,
) -> Tuple[Any, Dict[str, int], str]:
    """
    Process a single file. Can run in main process or in a worker.

    When use_temp_file=True (multiprocessing): writes records to a temp pickle file
    and returns (path_str, rejection_counts, filename). Avoids sending huge lists
    through the process queue, which was causing _recv_bytes() crashes.

    When use_temp_file=False (sequential): returns (records, rejection_counts, filename).

    When status_dict is provided (Manager().dict()), worker writes current filename
    so the main process can show "which file each core is processing".
    """
    from parser import read_file, transform_dataframe
    from cleaner import clean_and_validate_records, score_records
    from categorizer import categorize_records

    filename = file_info["filename"]
    if use_temp_file and status_dict is not None:
        try:
            status_dict[os.getpid()] = filename
        except Exception:
            pass

    try:
        df = read_file(file_info)
        if df is None or len(df) == 0:
            if use_temp_file:
                return "", {"empty": 1}, filename
            return [], {"empty": 1}, filename

        records = transform_dataframe(df, file_info)
        del df

        valid_records, rejection_counts = clean_and_validate_records(records)
        del records

        valid_records = categorize_records(valid_records)
        valid_records = score_records(valid_records)

        if use_temp_file:
            WORKER_TEMP_DIR.mkdir(parents=True, exist_ok=True)
            path = WORKER_TEMP_DIR / f"{uuid.uuid4().hex}.pkl"
            with open(path, "wb") as f:
                pickle.dump(valid_records, f, protocol=pickle.HIGHEST_PROTOCOL)
            return str(path), rejection_counts, filename

        return valid_records, rejection_counts, filename

    except Exception as e:
        if use_temp_file:
            return "", {"error": str(e)[:500]}, filename
        return [], {"error": str(e)}, filename
    finally:
        if use_temp_file and status_dict is not None:
            try:
                status_dict.pop(os.getpid(), None)
            except Exception:
                pass


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================

def _format_record_line(r: Dict[str, Any]) -> str:
    """One-line summary of a normalized record for live output."""
    email = r.get("email") or ""
    cat = r.get("email_category") or ""
    prov = r.get("email_provider") or ""
    q = r.get("quality_score")
    qs = str(q) if q is not None else ""
    src = r.get("data_source") or ""
    return f"  {email} | {cat} | {prov} | q={qs} | {src}"


def process_pipeline(
    use_gpu: bool = False,
    workers: int = CPU_WORKERS,
    test_mode: bool = False,
    max_files: int = None,
    show_records: bool = True,
    show_records_limit: int = 50,
    resume: bool = False,
    reset: bool = False,
):
    """
    Main processing pipeline.
    
    Args:
        use_gpu: Whether to use GPU acceleration
        workers: Number of CPU workers
        test_mode: Process only a few files for testing
        max_files: Maximum number of files to process
        show_records: Print normalized records to screen in real time
        show_records_limit: Max records to print per file (0 = all)
        resume: Resume from checkpoint (skip already processed files)
        reset: Clear checkpoint and start fresh
    """
    start_time = datetime.now()
    
    # Print banner
    print("\n" + "=" * 70)
    print("  EMAIL PROCESSING SYSTEM")
    print("  GPU-Accelerated Pipeline for 170M+ Records")
    print("=" * 70)
    print(f"\n  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  GPU: {'[YES] ' + GPU_NAME if GPU_AVAILABLE and use_gpu else '[NO] CPU Only'}")
    print(f"  CPU Workers: {workers}")
    print(f"  Mode: {'TEST' if test_mode else 'FULL'}")
    print("=" * 70 + "\n")
    
    # Statistics
    stats = {
        'files_processed': 0,
        'total_records_read': 0,
        'records_cleaned': 0,
        'records_loaded': 0,
        'duplicates_found': 0,
        'invalid_emails': 0,
        'role_emails': 0,
        'country_tld': 0,
    }
    
    # =========================================================================
    # STEP 1: Initialize Database
    # =========================================================================
    logger.info("Step 1: Initializing database...")
    
    if not initialize_database():
        logger.error("Failed to initialize database!")
        return
    
    logger.info("[OK] Database initialized")
    
    # Drop indexes for faster bulk loading
    logger.info("Dropping indexes for faster loading...")
    drop_indexes()
    
    # =========================================================================
    # STEP 2: Discover Files
    # =========================================================================
    logger.info("\nStep 2: Discovering files...")
    
    files = discover_files(BASE_DIR)
    
    # Filter out archives for now (handle separately)
    data_files = [f for f in files if not f['is_archive']]
    archive_files = [f for f in files if f['is_archive']]
    
    logger.info(f"[OK] Found {len(data_files)} data files, {len(archive_files)} archives")
    
    # Limit files in test mode - use medium-sized files with actual data
    if test_mode:
        # Filter to files between 100KB and 10MB (more likely to have real data)
        medium_files = [f for f in data_files if 100_000 < f['size_bytes'] < 10_000_000]
        # Skip DNC and seed files
        medium_files = [f for f in medium_files if 'dnc' not in f['filename'].lower() 
                        and 'seed' not in f['filename'].lower()]
        # Sort by size ascending
        medium_files = sorted(medium_files, key=lambda x: x['size_bytes'])
        data_files = medium_files[:5] if medium_files else data_files[:5]
        logger.info(f"  TEST MODE: Processing {len(data_files)} medium-sized files")
    elif max_files:
        data_files = data_files[:max_files]
        logger.info(f"  Limited to {len(data_files)} files")
    
    # Handle checkpoint reset
    if reset:
        clear_checkpoint()
        logger.info("[OK] Checkpoint cleared - starting fresh")
    
    # Handle resume - skip already processed files
    if resume:
        processed = load_checkpoint()
        if processed:
            original_count = len(data_files)
            data_files = [f for f in data_files if f['filename'] not in processed]
            skipped = original_count - len(data_files)
            logger.info(f"[RESUME] Skipping {skipped} already processed files, {len(data_files)} remaining")
    
    # =========================================================================
    # STEP 3: Process Files (parallel or sequential)
    # =========================================================================
    total_files = len(data_files)
    if workers == 1:
        logger.info(f"\nStep 3: Processing {total_files} files sequentially (--workers 1)...")
    else:
        logger.info(f"\nStep 3: Processing {total_files} files with {workers} parallel workers (up to {workers} files at once)...")
        WORKER_TEMP_DIR.mkdir(parents=True, exist_ok=True)
        print(f"\n  Queue: {total_files} files | Workers: {workers} (up to {workers} files in progress at a time)", flush=True)
        print("  Each line below = one file finished: [completed/total = %] Done: filename -> records loaded (total in DB)\n", flush=True)

    deduplicator = IncrementalDeduplicator(use_gpu=use_gpu and GPU_AVAILABLE)

    def handle_result(valid_records: List[Dict], rejection_counts: Dict[str, int], filename: str, file_info: Dict):
        """Dedupe, load to DB, update stats. Shared by sequential and parallel paths."""
        if not valid_records:
            if "empty" in rejection_counts:
                logger.debug(f"Empty: {filename}")
            elif "error" in rejection_counts:
                logger.error(f"Error processing {filename}: {rejection_counts['error']}")
            return
        # Real-time output: normalized records to screen
        if show_records:
            n = len(valid_records) if show_records_limit <= 0 else min(len(valid_records), show_records_limit)
            print(f"\n--- {filename} ({len(valid_records):,} records) ---", flush=True)
            for r in valid_records[:n]:
                print(_format_record_line(r), flush=True)
            if show_records_limit > 0 and len(valid_records) > show_records_limit:
                print(f"  ... and {len(valid_records) - show_records_limit:,} more", flush=True)
        total_from_file = len(valid_records) + sum(rejection_counts.values())
        n_valid_this_file = len(valid_records)
        stats["total_records_read"] += total_from_file
        stats["invalid_emails"] += rejection_counts.get("invalid_format", 0) + rejection_counts.get("missing_email", 0)
        stats["role_emails"] += rejection_counts.get("role_email", 0)
        stats["country_tld"] += rejection_counts.get("country_tld", 0)
        unique_records = deduplicator.process_batch(valid_records)
        n_unique = len(unique_records)
        n_loaded_this_file = 0
        if unique_records:
            n_loaded_this_file = load_records_batch(unique_records, BATCH_SIZE_LOAD, on_conflict="skip")
            stats["records_loaded"] += n_loaded_this_file
        stats["files_processed"] += 1
        stats["records_cleaned"] += n_valid_this_file
        del valid_records, unique_records
        gc.collect()
        # Per-file breakdown: read -> valid (passed filters) -> new (after dedup this run) -> loaded (actually inserted; rest were already in DB)
        pct = 100.0 * stats["files_processed"] / total_files
        print(
            f"  [{stats['files_processed']}/{total_files} = {pct:.1f}%] Done: {filename}",
            f" -> read {total_from_file:,} | valid {n_valid_this_file:,} | new {n_unique:,} | loaded {n_loaded_this_file:,} (total in DB: {stats['records_loaded']:,})",
            flush=True,
        )
        if stats["files_processed"] % 10 == 0:
            logger.info(
                f"Progress: {stats['files_processed']} files, "
                f"{stats['records_loaded']:,} loaded, "
                f"{deduplicator.total_duplicates:,} duplicates"
            )
        
        # Save checkpoint for resume support
        save_checkpoint(filename)

    if workers == 1:
        # Sequential: no multiprocessing, no pickling of huge lists, clear tracebacks
        for file_info in tqdm(data_files, desc="Processing files", unit="file"):
            valid_records, rejection_counts, filename = process_single_file(file_info, use_temp_file=False)
            handle_result(valid_records, rejection_counts, filename, file_info)
    else:
        # Parallel: workers write to temp files, return only path (small payload)
        # Shared status so we can show "which file each core is processing"
        global _executor
        manager = multiprocessing.Manager()
        status_dict = manager.dict()
        executor = ProcessPoolExecutor(max_workers=workers)
        _executor = executor  # Store reference for signal handler
        future_to_file = {
            executor.submit(process_single_file, file_info, True, status_dict): file_info
            for file_info in data_files
        }
        print(f"  Submitted {total_files} files. Up to {workers} workers (cores) running (largest files first).", flush=True)
        print(f"  Tip: If CPU is under 90%%, try --workers 24 or 32 to oversubscribe (helps with I/O wait).", flush=True)
        print(f"  First 'Done' line may take 1-5 minutes for big files...\n", flush=True)
        stop_heartbeat = threading.Event()
        def heartbeat():
            while True:
                if stop_heartbeat.wait(timeout=10):
                    break
                n = stats["files_processed"]
                # Show which file each worker (core) is currently processing
                current = list(status_dict.items())
                current.sort(key=lambda x: x[0])
                if current:
                    parts = [f"  Core {i+1}: {fn[:35]}..." if len(fn) > 35 else f"  Core {i+1}: {fn}" for i, (_, fn) in enumerate(current)]
                    line = " | ".join(parts)
                    print(f"  --- In progress ({n}/{total_files} = {100*n/total_files:.1f}% completed) ---", flush=True)
                    print(line, flush=True)
                else:
                    print(f"  ... still working ({n}/{total_files} = {100*n/total_files:.1f}% so far)", flush=True)
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()
        pbar = tqdm(total=len(data_files), desc="Files", unit="file", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} ({percentage:5.1f}%) {postfix}")
        try:
            for future in as_completed(future_to_file):
                # Check if shutdown was requested
                if _shutdown_requested:
                    logger.info("Shutdown requested, stopping...")
                    break
                file_info = future_to_file[future]
                try:
                    result_path_or_records, rejection_counts, filename = future.result()
                    pbar.set_postfix_str(f"last: {filename[:25]}...")
                    pbar.update(1)
                    if result_path_or_records == "":
                        stats["files_processed"] += 1
                        pct = 100.0 * stats["files_processed"] / total_files
                        if "empty" in rejection_counts:
                            print(f"  [{stats['files_processed']}/{total_files} = {pct:.1f}%] Done: {filename} (empty)", flush=True)
                            logger.debug(f"Empty: {filename}")
                        elif "error" in rejection_counts:
                            print(f"  [{stats['files_processed']}/{total_files} = {pct:.1f}%] Done: {filename} (error: {rejection_counts['error'][:50]}...)", flush=True)
                            logger.error(f"Error processing {filename}: {rejection_counts['error']}")
                        else:
                            print(f"  [{stats['files_processed']}/{total_files} = {pct:.1f}%] Done: {filename} (no records)", flush=True)
                        # Save checkpoint even for empty/error files
                        save_checkpoint(filename)
                        continue
                    # Load from temp file
                    path = Path(result_path_or_records)
                    if path.exists():
                        with open(path, "rb") as f:
                            valid_records = pickle.load(f)
                        try:
                            path.unlink()
                        except OSError:
                            pass
                        handle_result(valid_records, rejection_counts, filename, file_info)
                except Exception as e:
                    logger.error(f"Error with {file_info['filename']}: {e}")
        except KeyboardInterrupt:
            logger.info("Ctrl+C: shutting down workers (may take a few seconds)...")
            raise
        finally:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=2)
            pbar.close()
            # Shutdown without waiting for workers to finish (avoids OSError spam on Ctrl+C)
            kwargs = {"wait": False}
            if sys.version_info >= (3, 9):
                kwargs["cancel_futures"] = True
            executor.shutdown(**kwargs)
    
    # Get deduplicator stats
    dedup_stats = deduplicator.get_stats()
    stats['duplicates_found'] = dedup_stats['total_duplicates']
    
    # =========================================================================
    # STEP 4: Create Indexes
    # =========================================================================
    logger.info("\nStep 4: Creating indexes...")
    create_indexes()
    logger.info("[OK] Indexes created")
    
    # =========================================================================
    # STEP 5: Final Statistics
    # =========================================================================
    end_time = datetime.now()
    processing_time = int((end_time - start_time).total_seconds())
    
    # Get database counts
    total_in_db = get_email_count()
    category_counts = get_category_counts()
    source_counts = get_source_counts()
    provider_counts = get_provider_counts()
    quality_dist = get_quality_distribution()
    
    # Log to database
    log_final_stats(
        files_processed=stats['files_processed'],
        total_records=stats['total_records_read'],
        records_loaded=stats['records_loaded'],
        duplicates=stats['duplicates_found'],
        invalid_emails=stats['invalid_emails'],
        role_emails=stats['role_emails'],
        country_tld=stats['country_tld'],
        processing_time=processing_time,
        notes=f"GPU: {use_gpu and GPU_AVAILABLE}, Workers: {workers}"
    )
    
    # Print summary
    print("\n" + "=" * 70)
    print("  PROCESSING COMPLETE")
    print("=" * 70)
    print(f"\n  Duration: {processing_time // 3600}h {(processing_time % 3600) // 60}m {processing_time % 60}s")
    print(f"\n  Files Processed: {stats['files_processed']:,}")
    print(f"  Total Records Read: {stats['total_records_read']:,}")
    print(f"  Records Loaded: {stats['records_loaded']:,}")
    print(f"  Duplicates Removed: {stats['duplicates_found']:,}")
    print(f"  Invalid Emails: {stats['invalid_emails']:,}")
    print(f"  Role Emails Filtered: {stats['role_emails']:,}")
    print(f"  Country TLD Filtered: {stats['country_tld']:,}")
    print(f"\n  Total in Database: {total_in_db:,}")
    
    print(f"\n  By Category:")
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        pct = count / total_in_db * 100 if total_in_db > 0 else 0
        print(f"    {category}: {count:,} ({pct:.1f}%)")
    
    print(f"\n  By Provider (mail server host):")
    for provider, count in list(provider_counts.items())[:10]:
        if provider:
            pct = count / total_in_db * 100 if total_in_db > 0 else 0
            print(f"    {provider}: {count:,} ({pct:.1f}%)")
    
    print(f"\n  By Quality Score:")
    for quality_range, count in quality_dist.items():
        pct = count / total_in_db * 100 if total_in_db > 0 else 0
        print(f"    {quality_range}: {count:,} ({pct:.1f}%)")
    
    print(f"\n  Top Data Sources:")
    for source, count in list(source_counts.items())[:10]:
        print(f"    {source}: {count:,}")
    
    print("\n" + "=" * 70)
    print("  Ready for queries! Use export_tools.py to export segments.")
    print("=" * 70 + "\n")
    
    # Cleanup
    close_connection_pool()


# =============================================================================
# CLI
# =============================================================================

def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(
        description='Email Processing System - GPU-Accelerated Pipeline'
    )
    
    parser.add_argument(
        '--use-gpu', 
        action='store_true',
        default=False,
        help='Use GPU acceleration (default: False on Windows)'
    )
    
    parser.add_argument(
        '--no-gpu', 
        action='store_true',
        help='Disable GPU acceleration'
    )
    
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=CPU_WORKERS,
        help=f'Number of CPU workers (default: {CPU_WORKERS}). Use 1 for sequential. Try 24 or 32 if CPU is under 90%% (oversubscribe for I/O).'
    )
    
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='Test mode: process only 5 files'
    )
    
    parser.add_argument(
        '--max-files', '-m',
        type=int,
        help='Maximum number of files to process'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose logging'
    )
    
    parser.add_argument(
        '--no-live',
        action='store_true',
        help='Disable live output of normalized records (default: print up to --live-limit per file)'
    )
    parser.add_argument(
        '--live-limit',
        type=int,
        default=50,
        metavar='N',
        help='Max records to print per file (0 = all). Default: 50'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last checkpoint (skip already processed files)'
    )
    
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Clear checkpoint and start fresh'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Determine GPU usage
    use_gpu = args.use_gpu and not args.no_gpu
    
    # Run pipeline
    try:
        process_pipeline(
            use_gpu=use_gpu,
            workers=args.workers,
            test_mode=args.test,
            max_files=args.max_files,
            show_records=not args.no_live,
            show_records_limit=args.live_limit,
            resume=args.resume,
            reset=args.reset,
        )
    except KeyboardInterrupt:
        logger.info("\nProcessing interrupted by user")
        close_connection_pool()
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        close_connection_pool()
        sys.exit(1)


if __name__ == '__main__':
    # Required for Windows multiprocessing
    multiprocessing.freeze_support()
    main()
