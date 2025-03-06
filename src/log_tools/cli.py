import typer

from .log_tools import filter_logs_by_date
from .common_args import common_args


app = typer.Typer()
app.callback()(common_args)
app.command("filter")(filter_logs_by_date)



if __name__ == '__main__':
    app()