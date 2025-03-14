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
    