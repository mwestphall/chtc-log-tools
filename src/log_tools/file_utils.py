import os
import gzip
import magic
from pathlib import Path
from typing import Iterator, Any
import io
from datetime import datetime, timezone
from dataclasses import dataclass
from collections import defaultdict
from .log_utils import safe_parse_line
from .common_args import CHUNK_SIZE, TIME_FIELD

def open_possibly_compressed_file(file_path: Path) -> io.BytesIO:
    """ Using python-magic, expose a plaintext or compressed file in 
    read-binary mode via a unified interface
    """
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    is_compressed = 'gzip' in file_type
    
    open_func = gzip.open if is_compressed else open
    
    return open_func(file_path, 'rb')


def read_file_reverse(file_path: Path, chunk_size=CHUNK_SIZE) -> Iterator[str]:
    """ Reads a regular or compressed (.gz) text file line by line in reverse 
    order using chunk-based processing.
    """
    with open_possibly_compressed_file(file_path) as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        buffer = None
        position = file_size

        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size

            f.seek(position)
            chunk = f.read(read_size)

            lines = chunk.split(b'\n')

            if buffer is not None:
                if chunk[-1] == b'\n':
                    yield buffer  # Yield previous buffer since it's complete
                else:
                    lines[-1] += buffer  # Merge buffer with last line of current chunk

            buffer = lines.pop(0) if position != 0 else None  # Save first line for next chunk

            # Yield non-empty lines in reverse order
            for line in reversed(lines):
                if line.strip():
                    yield line.decode()

        # Yield the last buffered line if it's valid
        if buffer and buffer.strip():
            yield buffer

def _is_structured_logs(file_path: Path) -> tuple[bool, dict[str, Any]]:
    """ 
    Check whether a given file (probably) contains structured logs by checking whether
    its first line is JSON-deserializable
    """
    with open_possibly_compressed_file(file_path) as f:
        # TODO handle/skip headers?
        line = f.readline().decode()
        return safe_parse_line(line)

@dataclass
class DateRangedLogFile:
    path: str
    first_record: dict[str, Any]
    start_time: datetime
    end_time: datetime = datetime.max.replace(tzinfo=timezone.utc)


    def contains_logs_for(self, start_time: datetime, end_time: datetime):
        """ Return whether this log's time range overlaps with the given time range"""
        start_time_tz = start_time.replace(tzinfo=start_time.tzinfo or self.start_time.tzinfo)
        end_time_tz = end_time.replace(tzinfo=end_time.tzinfo or self.end_time.tzinfo)
        latest_start = max(self.start_time, start_time_tz)
        earliest_end = min(self.end_time, end_time_tz)
        return earliest_end > latest_start


def find_log_files(log_paths: list[Path], max_depth = 999) -> Iterator[Path]:
    """
    Given a set of log paths or directories containing logs, and a max search depth, yield
    all individual files in those paths
    """
    for p in log_paths:
        if p.is_file():
            yield p
            continue
        dirs: list[tuple[Path, int]] = [(p, 0)]
        while len(dirs) and (dir_tuple := dirs.pop()):
            cur_dir, cur_depth = dir_tuple
            for f in cur_dir.iterdir():
                if f.is_file():
                    yield f
                elif f.is_dir() and cur_depth < max_depth:
                    dirs.append([f, cur_depth + 1])
            
        

def find_log_files_in_date_range(
        log_paths: list[Path], 
        start_date: datetime = datetime.min,
        end_date: datetime = datetime.max,
        time_key: str = TIME_FIELD, 
        partition_key: str = "") -> Iterator[tuple[str, list[DateRangedLogFile]]]:
    sorted_files : dict[str, list[DateRangedLogFile]] = defaultdict(lambda: [])
    # Find all newline-delimited JSON files in the given directory(s)
    for file_path in find_log_files(log_paths):
        parsed, fields = _is_structured_logs(file_path)
        # Filter out ndjson objects that don't contain the expected time key
        if not parsed or not time_key in fields:
            continue
        sorted_files[fields.get(partition_key, "")].append(DateRangedLogFile(file_path, fields, datetime.fromisoformat(fields[time_key])))


    for key, files in sorted_files.items():
        files.sort(key = lambda file: file.start_time, reverse = True)

        # set the end of each file to the start of the next
        for file, next_file in zip(files[1:], files):
            file.end_time = next_file.start_time

        # Filter down to the list of files containing records in the date range
        in_range_files = [f for f in files if f.contains_logs_for(start_date, end_date)]

        yield (key, in_range_files)


def read_files_reverse(files: list[DateRangedLogFile], chunk_size: int = CHUNK_SIZE) -> Iterator[str]:
    for file in files:
        fname = file.path
        for line in read_file_reverse(fname, chunk_size):
            yield line

def aggregate_log_files(
        log_paths: list[Path], 
        start_date: datetime = datetime.min,
        end_date: datetime = datetime.max,
        time_key: str = TIME_FIELD, 
        partition_key: str = "", 
        chunk_size: int = CHUNK_SIZE) -> Iterator[str]:
    """ Given a log file path, run read_file_reverse over all files matching 
    that pattern.
    """
    for _, files in find_log_files_in_date_range(log_paths, start_date, end_date, time_key, partition_key):
        for line in read_files_reverse(files, chunk_size):
            yield line
