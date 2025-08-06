import typer

from .sequence_check import sequence
from .log_tools import filterer
from .partition_checker import partition_checker
from .stats import stats


app = typer.Typer()
app.add_typer(filterer, name="filter")
app.add_typer(partition_checker, name="times")
app.add_typer(stats, name="stats")



if __name__ == '__main__':
    app()
