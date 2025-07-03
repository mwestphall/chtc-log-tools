import typer
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from datetime import datetime
from typing import Annotated

load_dotenv(find_dotenv(usecwd=True))

TIME_FIELD = "time"
MSG_FIELD = "msg"
# Want to read a lot of the file into memory at once since decompression is expensive time-wise
CHUNK_SIZE = 4 * 1024 * 1024
EXCLUDE_KEYS = "level,sequence_info"

LogPathOpt = Annotated[list[Path], typer.Argument(help="Path to the log file(s) to parse")]
StartDateArg = Annotated[datetime, typer.Option(help="First date/time from which to return logs")]
EndDateArg = Annotated[datetime, typer.Option(help="Last date/time from which to return logs")]
MaxLinesArg = Annotated[int, typer.Option(help="Max number of lines to seek backwards")]
TimeFieldArg = Annotated[str, typer.Option(help="Structured log field to parse timestamps from", envvar="TIME_KEY")]
MsgFieldArg = Annotated[str, typer.Option(help="Structured log field containing the main body of the message", envvar="MESSAGE_KEY")]
ExcludeKeysArg = Annotated[str, typer.Option(help="Comma-separated structured log fields to omit from output", envvar="EXCLUDE_KEYS")]
PartitionKeyArg = Annotated[str, typer.Option(help="Comma-separated fields on which logs are partitioned, in addition to time", envvar="PARTITION_KEYS")]
ChunkSizeArg = Annotated[int, typer.Option(help="Maximum chunk size of a file to read at once", envvar="CHUNK_SIZE")]
