import logging
import re
import sys

import click
from engine import Engine
from intellisense import Intellisense
from output_format import OutputFormat
from output_writer import OutputWriter
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer
from sqlalchemy import create_engine
from state_machine import SolrSQLState
from state_machine import SolrSQLStateMachine
from util import Util

keywords = [
    "and",
    "as",
    "asc",
    "between",
    "by",
    "desc",
    "distinct",
    "fetch",
    "from",
    "group",
    "having",
    "in",
    "is",
    "like",
    "limit",
    "not",
    "null",
    "offset",
    "or",
    "order",
    "select",
    "where",
    "approx_count_distinct",
    "avg",
    "count",
    "min",
    "max",
    "sum",
]

logger = logging.getLogger(__name__)


class SQLCompleter(Completer):

    def __init__(self):
        self.state_machine = SolrSQLStateMachine()
        self.intellisense = Intellisense()

    def reset_state_machine(self):
        self.state_machine.reset()

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        tokens = list(filter(None, map(str.strip, re.findall(r"\w+|\W", text))))
        last_token = tokens[-1] if tokens else ""
        last_char = text[-1]

        logger.debug(tokens)

        if (
            last_char == " "
            or last_token == ","
            or last_token == "("
            or last_token == ")"
        ):
            if last_token == "(" or last_token == ")":
                last_token = tokens[-2]
            elif last_token.lower() == "by":
                last_token = " ".join(tokens[-2:])

            logger.debug("-" + self.state_machine.current_state.name + "-")
            self.state_machine.process_token(last_token)
            logger.debug("+" + self.state_machine.current_state.name + "+")

        # print("*" + last_token + "*", "+" + last_char + "+")

        if (
            last_char == " "
            and self.state_machine.current_state == SolrSQLState.EXPRESSION
        ):
            return []

        completions = self.intellisense.get_context_items(
            last_token, last_char, self.state_machine
        )
        return completions


sql_completer = SQLCompleter()

style = Style.from_dict(
    {
        # 'completion-menu.completion': 'bg:#008888 #ffffff',
        "completion-menu.completion.current": "bg:#e45a3a #000000",
        # 'scrollbar.background': 'bg:#88aaaa',
        # 'scrollbar.button': 'bg:#222222',
    }
)


@click.command()
@click.option("-l", "--protocol", default="http")
@click.option("--host", default="solr")
@click.option("-p", "--port", default="8983")
@click.option("--path", default="solr")
@click.option("-c", "--collection")
@click.option("-o", "--output-file")
@click.option("-f", "--output-format")
@click.option("-s", "--statement")
def prompt(
    protocol, host, port, path, collection, output_file, output_format, statement
):
    logging.basicConfig(filename="prompt.log", encoding="utf-8", level=logging.WARNING)

    # Setting command-line default values
    if not collection:
        raise ValueError("Missing collection name")
    if not output_file:
        output = sys.stdout
    else:
        output = open(output_file, "w", newline="")
    if not output_format:
        output_format = OutputFormat.TABULAR
    elif output_format.lower() == "csv":
        output_format = OutputFormat.CSV

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer), completer=sql_completer, style=style
    )

    Util.print_banner()

    Engine.set_engine(create_engine(f"solr://{host}:{port}/{path}/{collection}"))

    if statement:
        execute_statement(output_format, statement, output)

    else:
        display_prompt(output, output_format, session)

    if output_file:
        output.close()


def display_prompt(output_file, output_format, session):
    while True:
        try:
            text = session.prompt("> ")
        except KeyboardInterrupt:
            continue
        except EOFError:
            break
        else:
            with Engine.get_engine().connect() as connection:
                result = connection.execute(text)

                output_writer = OutputWriter(output_format, output_file)
                output_writer.write(result)

            sql_completer.reset_state_machine()

    print("GoodBye!")


def execute_statement(output_format, statement, output):
    with Engine.get_engine().connect() as connection:
        result = connection.execute(statement)

        output_writer = OutputWriter(output_format, output)
        output_writer.write(result)


# pylint: disable=no-value-for-parameter
if __name__ == "__main__":
    prompt()
