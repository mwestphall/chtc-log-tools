import os
import gzip
import magic
from pathlib import Path
from typing import Iterator, Any
import io
import glob
from .log_utils import safe_parse_line
from datetime import datetime

def open_possibly_compressed_file(file_path: Path) -> io.BytesIO:
    """ Using python-magic, expose a plaintext or compressed file in 
    read-binary mode via a unified interface
    """
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    is_compressed = 'gzip' in file_type
    
    open_func = gzip.open if is_compressed else open
    
    return open_func(file_path, 'rb')


def read_file_reverse(file_path: Path, chunk_size=40960) -> Iterator[str]:
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
        line = f.readline()
        return safe_parse_line(line)

def aggregate_log_files(log_path: Path, time_key: str, chunk_size: int = 40960) -> Iterator[str]:
    """ Given a log file path, run read_file_reverse over all files matching 
    that pattern.
    """
    sorted_files : list[tuple[str,datetime]] = []
    all_log_files = [log_path] if log_path.is_file() else [f for f in log_path.iterdir() if f.is_file()]
    for file_path in all_log_files:
        parsed, fields = _is_structured_logs(file_path)
        if not parsed or not time_key in fields:
            continue
        sorted_files.append((file_path, fields[time_key]))
    
    sorted_files.sort(key = lambda pair: pair[1], reverse = True)
    print(sorted_files)
    for fname, _ in sorted_files:
        for l in read_file_reverse(fname, chunk_size):
            yield l