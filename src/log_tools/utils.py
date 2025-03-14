import typing
import io 
import json

def readlines_reverse(f: typing.BinaryIO, buf_size = 4096):
    ''' Read a file line-by-line in reverse order'''
    segment:str = None
    offset = 0
    f.seek(0, io.SEEK_END)
    file_size = f.tell()
    remaining_size = file_size
    while remaining_size > 0:
        offset = min(file_size, offset + buf_size)
        f.seek(file_size - offset)
        buffer = f.read(min(remaining_size, buf_size))
        if remaining_size == file_size and buffer[-1] == '\n':
            buffer = buffer[:-1]
        remaining_size -= buf_size
        lines = buffer.decode().split("\n")
        # Add the last partial line from the previous chunk to the last line of this chunk
        if segment is not None:
            lines[-1] += segment
        segment = lines[0]
        lines = lines[1:]
        for line in reversed(lines):
            yield line
    # yield the last partial segment after reading through the file
    yield segment



import os
import gzip
import magic

def read_file_reverse(file_path, chunk_size=40960):
    """Reads a regular or compressed (.gz) text file line by line in reverse order using chunk-based processing."""
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    is_compressed = 'gzip' in file_type
    
    open_func = gzip.open if is_compressed else open
    
    with open_func(file_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        buffer = None
        position = file_size

        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size

            f.seek(position)
            chunk = f.read(read_size).decode()

            lines = chunk.split('\n')

            if buffer is not None:
                if chunk[-1] == '\n':
                    yield buffer  # Yield previous buffer since it's complete
                else:
                    lines[-1] += buffer  # Merge buffer with last line of current chunk

            buffer = lines.pop(0) if position != 0 else None  # Save first line for next chunk

            # Yield non-empty lines in reverse order
            for line in reversed(lines):
                if line.strip():
                    yield line

        # Yield the last buffered line if it's valid
        if buffer and buffer.strip():
            yield buffer