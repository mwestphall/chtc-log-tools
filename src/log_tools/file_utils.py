import os
import gzip
import magic
from pathlib import Path
from typing import Iterator, Any
import io
from .log_utils import safe_parse_line
from datetime import datetime, timezone
from dataclasses import dataclass
from common_args import CHUNK_SIZE

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
    start_time: datetime
    end_time: datetime = datetime.max.replace(tzinfo=timezone.utc)


    def contains_logs_for(self, start_time: datetime, end_time: datetime):
        """ Return whether this log's time range overlaps with the given time range"""
        start_time_tz = start_time.replace(tzinfo=start_time.tzinfo or self.start_time.tzinfo)
        end_time_tz = end_time.replace(tzinfo=end_time.tzinfo or self.end_time.tzinfo)
        latest_start = max(self.start_time, start_time_tz)
        earliest_end = min(self.end_time, end_time_tz)
        return earliest_end > latest_start

def aggregate_log_files(
        log_path: Path, 
        start_date: datetime = datetime.min,
        end_date: datetime = datetime.max,
        time_key: str = "time", 
        chunk_size: int = CHUNK_SIZE) -> Iterator[str]:
    """ Given a log file path, run read_file_reverse over all files matching 
    that pattern.
    """
    sorted_files : list[DateRangedLogFile] = []
    # Find all newline-delimited JSON files in the given directory
    all_log_files = [log_path] if log_path.is_file() else [f for f in log_path.iterdir() if f.is_file()]
    for file_path in all_log_files:
        parsed, fields = _is_structured_logs(file_path)
        # Filter out ndjson objects that don't contain the expected time key
        if not parsed or not time_key in fields:
            continue
        sorted_files.append(DateRangedLogFile(file_path, datetime.fromisoformat(fields[time_key])))
    sorted_files.sort(key = lambda file: file.start_time, reverse = True)

    # set the end of each file to the start of the next
    for file, next_file in zip(sorted_files[1:], sorted_files):
        file.end_time = next_file.start_time

    for file in sorted_files:
        fname = file.path
        if not file.contains_logs_for(start_date, end_date):
            continue
        for l in read_file_reverse(fname, chunk_size):
            yield l
