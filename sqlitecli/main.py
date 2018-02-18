from __future__ import unicode_literals
from __future__ import print_function

import os
import sys
import traceback
import logging
import threading
from time import time
from datetime import datetime
from random import choice
from io import open

from cli_helpers.tabular_output import TabularOutputFormatter
from cli_helpers.tabular_output import preprocessors
import click
import sqlparse
from prompt_toolkit import CommandLineInterface, Application, AbortAction
from prompt_toolkit.interface import AcceptAction
from prompt_toolkit.enums import DEFAULT_BUFFER, EditingMode
from prompt_toolkit.shortcuts import create_prompt_layout, create_eventloop
from prompt_toolkit.styles.from_pygments import style_from_pygments
from prompt_toolkit.document import Document
from prompt_toolkit.filters import Always, HasFocus, IsDone
from prompt_toolkit.layout.processors import (HighlightMatchingBracketProcessor,
                                              ConditionalProcessor)
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from pygments.token import Token

from .packages.special.main import NO_QUERY
from .packages.prompt_utils import confirm, confirm_destructive_query, prompt
from .packages.tabular_output import sql_format
import packages.special as special
from .sqlcompleter import SQLCompleter
from .clitoolbar import create_toolbar_tokens_func
from .clistyle import style_factory
from .sqlexecute import FIELD_TYPES, SQLExecute
from .clibuffer import CLIBuffer
from .completion_refresher import CompletionRefresher
from .config import (write_default_config, get_mylogin_cnf_path,
                     open_mylogin_cnf, read_config_files, str_to_bool)
from .key_bindings import cli_bindings
from .encodingutils import utf8tounicode, text_type
from .lexer import Lexer
from .__init__ import __version__
from .compat import WIN
from .packages.filepaths import dir_path_exists

import itertools

click.disable_unicode_literals_warning = True

try:
    from urlparse import urlparse
    FileNotFoundError = OSError
except ImportError:
    from urllib.parse import urlparse
from pymysql import OperationalError

from collections import namedtuple

# Query tuples are used for maintaining history
Query = namedtuple('Query', ['query', 'successful', 'mutating'])

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))


class SQLiteCli(object):
    DEFAULT_PROMPT = 'sqlite> '
    MAX_LEN_PROMPT = 45

    def __init__(self, prompt=None, sqliteclirc=None):
        self.config = self.init_config(sqliteclirc)
        self.prompt = prompt or self.config['main']['prompt'] or self.DEFAULT_PROMPT
        self.prompt_continuation = self.config['main']['prompt_continuation']
        self.multi_line = self.config['main'].as_bool('multi_line')
        self.key_bindings = self.config['main']['key_bindings']

        self.explicit_pager = False
        self.logfile = None

        # Init formatter
        self.formatter = TabularOutputFormatter(
            format_name=self.config['main']['table_format']
        )
        self.formatter.cli = self
        sql_format.register_new_formatter(self.formatter)

        # Init style
        self.syntax_style = self.config['main']['syntax_style']
        self.cli_style = self.config['colors']
        self.output_style = style_factory(self.syntax_style, self.cli_style)

        # Init completer.
        self.smart_completion = self.config['main'].as_bool('smart_completion')
        self.completer = SQLCompleter(
            self.smart_completion,
            supported_formats=self.formatter.supported_formats,
            keyword_casing=self.config['main'].get('keyword_casing', 'auto')
        )
        self._completer_lock = threading.Lock()
        self.completion_refresher = CompletionRefresher()

        # Register custom special commands
        self.register_special_commands()

        self.cli = None

    def init_config(self, sqliteclirc):
        # Order matters, the settings in later file will override those from
        # previously file
        config_files = [
            os.path.join(PACKAGE_ROOT, 'sqliteclirc'),
            '/etc/sqliteclirc',
            sqliteclirc
        ]
        return read_config_files(config_files)

    def connect(self, filename=None):
        self.sqlexecute = SQLExecute(filename)

    def run_cli(self):
        self.iterations = 0
        self.refresh_completions()

        history_file = os.path.expanduser(
            os.environ.get('SQLiteCLI_HISTFILE', '~/.sqlitecli-history')
        )
        history = FileHistory(history_file)

        self.cli = self._build_cli(history)

        def one_iteration():
            document = self.cli.run()
            special.set_expanded_output(False)

            try:
                document = self.handle_editor_command(self.cli, document)
            except RuntimeError as e:
                self.echo(str(e), err=True, fg='red')
                return

            if not document.text.strip():
                return

            mutating = False

            try:
                special.write_tee(self.get_prompt(self.prompt) + document.text)
                successful = False
                start = time()
                res = self.sqlexecute.run(document.text)
                successful = True
                threshold = 1000
                result_count = 0

                for title, cur, headers, status in res:
                    if (is_select(status)
                        and cur and cur.rowcount > threshold):
                        self.echo(
                            'The result set has more than {} rows.'.forma(threshold),
                            fg='red'
                        )
                        if not confirm('Do you want to continue?'):
                            self.echo('Aborted!', err=True, fg='red')
                            break

                    formatted = self.format_output(
                        title, cur, headers, special.is_expanded_output(), None
                    )

                    t = time() - start
                    try:
                        if result_count > 0:
                            self.echo('')
                        try:
                            self.output(formatted, status)
                        except KeyboardInterrupt:
                            pass

                        if special.is_timing_enabled():
                            self.echo('Time: %0.03fs' % t)
                    except KeyboardInterrupt:
                        pass

                    start = time()
                    result_count += 1
                    mutating = mutating or is_mutating(status)
                special.unset_once_if_written()
            except EOFError as e:
                raise e
            except KeyboardInterrupt:
                pass
            except NotImplementedError:
                self.echo('Not Yet Implemented.', fg="yellow")

            query = Query(document.text, successful, mutating)

        try:
            while True:
                one_iteration()
                self.iterations += 1
        except EOFError:
            special.close_tee()

    def _build_cli(self, history):
        key_binding_manager = cli_bindings()

        def prompt_tokens(cli):
            prompt = self.get_prompt(self.prompt)
            if len(prompt) > self.MAX_LEN_PROMPT:
                prompt = self.get_prompt('\\d> ')
            return [(Token.Prompt, prompt)]

        def get_continuation_tokens(cli, width):
            prompt = self.get_prompt(self.prompt_continuation)
            token = (
                Token.Continuation,
                ' ' * (width - len(prompt)) + prompt
            )
            return [token]

        def show_suggestion_tip():
            return self.iterations < 2

        get_toolbar_tokens = create_toolbar_tokens_func(
            self.completion_refresher.is_refreshing,
            show_suggestion_tip)

        layout = create_prompt_layout(
            lexer=Lexer,
            multiline=True,
            get_prompt_tokens=prompt_tokens,
            get_continuation_tokens=get_continuation_tokens,
            get_bottom_toolbar_tokens=get_toolbar_tokens,
            display_completions_in_columns=self.config['main'].as_bool('wider_completion_menu'),
            extra_input_processors=[
                ConditionalProcessor(
                    processor=HighlightMatchingBracketProcessor(chars='[](){}'),
                    filter=HasFocus(DEFAULT_BUFFER) & ~IsDone())
            ],
            reserve_space_for_menu=self.get_reserved_space()
        )

        with self._completer_lock:
            buf = CLIBuffer(
                always_multiline=self.multi_line,
                completer=self.completer,
                history=history,
                auto_suggest=AutoSuggestFromHistory(),
                complete_while_typing=Always(),
                accept_action=AcceptAction.RETURN_DOCUMENT)

            if self.key_bindings == 'vi':
                editing_mode = EditingMode.VI
            else:
                editing_mode = EditingMode.EMACS

            application = Application(
                style=style_from_pygments(style_cls=self.output_style),
                layout=layout,
                buffer=buf,
                key_bindings_registry=key_binding_manager.registry,
                on_exit=AbortAction.RAISE_EXCEPTION,
                on_abort=AbortAction.RETRY,
                editing_mode=editing_mode,
                ignore_case=True)

            cli = CommandLineInterface(
                application=application,
                eventloop=create_eventloop())

            return cli

    def get_prompt(self, string):
        sqlexecute = self.sqlexecute
        now = datetime.now()
        string = string.replace('\\u', '(none)')
        string = string.replace('\\h', '(none)')
        string = string.replace('\\d', '(none)')
        string = string.replace('\\t', 'sqlite')
        string = string.replace('\\n', "\n")
        string = string.replace('\\D', now.strftime('%a %b %d %H:%M:%S %Y'))
        string = string.replace('\\m', now.strftime('%M'))
        string = string.replace('\\P', now.strftime('%p'))
        string = string.replace('\\R', now.strftime('%H'))
        string = string.replace('\\r', now.strftime('%I'))
        string = string.replace('\\s', now.strftime('%S'))
        string = string.replace('\\_', ' ')
        return string

    def get_reserved_space(self):
        """Get the number of lines to reserve for the completion menu."""
        reserved_space_ratio = .45
        max_reserved_space = 8
        _, height = click.get_terminal_size()
        return min(int(round(height * reserved_space_ratio)), max_reserved_space)


    def register_special_commands(self):
        pass

    def handle_editor_command(self, cli, document):
        """
        Editor command is any query that is prefixed or suffixed
        by a '\e'. The reason for a while loop is because a user
        might edit a query multiple times.
        For eg:
        "select * from \e"<enter> to edit it in vim, then come
        back to the prompt with the edited query "select * from
        blah where q = 'abc'\e" to edit it again.
        :param cli: CommandLineInterface
        :param document: Document
        :return: Document
        """
        # FIXME: using application.pre_run_callables like this here is not the best solution.
        # It's internal api of prompt_toolkit that may change. This was added to fix
        # https://github.com/dbcli/pgcli/issues/668. We may find a better way to do it in the future.
        saved_callables = cli.application.pre_run_callables
        while special.editor_command(document.text):
            filename = special.get_filename(document.text)
            query = (special.get_editor_query(document.text) or
                     self.get_last_query())
            sql, message = special.open_external_editor(filename, sql=query)
            if message:
                # Something went wrong. Raise an exception and bail.
                raise RuntimeError(message)
            cli.current_buffer.document = Document(sql, cursor_position=len(sql))
            cli.application.pre_run_callables = []
            document = cli.run()
            continue
        cli.application.pre_run_callables = saved_callables
        return document

    def log_output(self, output):
        """Log the output in the audit log, if it's enabled."""
        if self.logfile:
            click.echo(utf8tounicode(output), file=self.logfile)

    def echo(self, s, **kwargs):
        """Print a message to stdout.

        The message will be logged in the audit log, if enabled.

        All keyword arguments are passed to click.echo().

        """
        self.log_output(s)
        click.secho(s, **kwargs)

    def get_output_margin(self, status=None):
        """Get the output margin (number of rows for the prompt, footer and
        timing message."""
        margin = self.get_reserved_space() + self.get_prompt(self.prompt).count('\n') + 1
        if special.is_timing_enabled():
            margin += 1
        if status:
            margin += 1 + status.count('\n')

        return margin


    def output(self, output, status=None):
        """Output text to stdout or a pager command.

        The status text is not outputted to pager or files.

        The message will be logged in the audit log, if enabled. The
        message will be written to the tee file, if enabled. The
        message will be written to the output file, if enabled.

        """
        if output:
            size = self.cli.output.get_size()

            margin = self.get_output_margin(status)

            fits = True
            buf = []
            output_via_pager = self.explicit_pager and special.is_pager_enabled()
            for i, line in enumerate(output, 1):
                self.log_output(line)
                special.write_tee(line)
                special.write_once(line)

                if fits or output_via_pager:
                    # buffering
                    buf.append(line)
                    if len(line) > size.columns or i > (size.rows - margin):
                        fits = False
                        if not self.explicit_pager and special.is_pager_enabled():
                            # doesn't fit, use pager
                            output_via_pager = True

                        if not output_via_pager:
                            # doesn't fit, flush buffer
                            for line in buf:
                                click.secho(line)
                            buf = []
                else:
                    click.secho(line)

            if buf:
                if output_via_pager:
                    # sadly click.echo_via_pager doesn't accept generators
                    click.echo_via_pager("\n".join(buf))
                else:
                    for line in buf:
                        click.secho(line)

        if status:
            self.log_output(status)
            click.secho(status)

    def refresh_completions(self, reset=False):
        if reset:
            with self._completer_lock:
                self.completer.reset_completions()
        self.completion_refresher.refresh(
            self.sqlexecute, self._on_completions_refreshed,
            {'smart_completion': self.smart_completion,
             'supported_formats': self.formatter.supported_formats,
             'keyword_casing': self.completer.keyword_casing})

        return [(None, None, None,
                'Auto-completion refresh started in the background.')]

    def _on_completions_refreshed(self, new_completer):
        """Swap the completer object in cli with the newly created completer.
        """
        with self._completer_lock:
            self.completer = new_completer
            # When cli is first launched we call refresh_completions before
            # instantiating the cli object. So it is necessary to check if cli
            # exists before trying the replace the completer object in cli.
            if self.cli:
                self.cli.current_buffer.completer = new_completer

        if self.cli:
            # After refreshing, redraw the CLI to clear the statusbar
            # "Refreshing completions..." indicator
            self.cli.request_redraw()

    def get_completions(self, text, cursor_positition):
        with self._completer_lock:
            return self.completer.get_completions(
                Document(text=text, cursor_position=cursor_positition), None)

    def run_query(self, query, new_line=True):
        """Runs *query*."""
        results = self.sqlexecute.run(query)
        for result in results:
            title, cur, headers, status = result
            self.formatter.query = query
            output = self.format_output(title, cur, headers)
            for line in output:
                click.echo(line, nl=new_line)

    def format_output(self, title, cur, headers, expanded=False,
                      max_width=None):
        expanded = expanded or self.formatter.format_name == 'vertical'
        output = []

        output_kwargs = {
            'disable_numparse': True,
            'preserve_whitespace': True,
            'preprocessors': (preprocessors.align_decimals, ),
            'style': self.output_style
        }

        if title:  # Only print the title if it's not None.
            output = itertools.chain(output, [title])

        if cur:
            column_types = None
            if hasattr(cur, 'description'):
                def get_col_type(col):
                    col_type = FIELD_TYPES.get(col[1], text_type)
                    return col_type if type(col_type) is type else text_type
                column_types = [get_col_type(col) for col in cur.description]

            if max_width is not None:
                cur = list(cur)

            formatted = self.formatter.format_output(
                cur, headers, format_name='vertical' if expanded else None,
                column_types=column_types,
                **output_kwargs)

            if isinstance(formatted, (text_type)):
                formatted = formatted.splitlines()
            formatted = iter(formatted)

            first_line = next(formatted)
            formatted = itertools.chain([first_line], formatted)

            if (not expanded and max_width and headers and cur and
                    len(first_line) > max_width):
                formatted = self.formatter.format_output(
                    cur, headers, format_name='vertical', column_types=column_types, **output_kwargs)
                if isinstance(formatted, (text_type)):
                    formatted = iter(formatted.splitlines())

            output = itertools.chain(output, formatted)


        return output

    def get_reserved_space(self):
        """Get the number of lines to reserve for the completion menu."""
        reserved_space_ratio = .45
        max_reserved_space = 8
        _, height = click.get_terminal_size()
        return min(int(round(height * reserved_space_ratio)), max_reserved_space)

    def get_last_query(self):
        """Get the last query executed or None."""
        return self.query_history[-1][0] if self.query_history else None


def need_completion_refresh(queries):
    """Determines if the completion needs a refresh by checking if the sql
    statement is an alter, create, drop or change db."""
    for query in sqlparse.split(queries):
        try:
            first_token = query.split()[0]
            if first_token.lower() in ('alter', 'create', 'use', '\\r',
                    '\\u', 'connect', 'drop'):
                return True
        except Exception:
            return False


def is_dropping_database(queries, dbname):
    """Determine if the query is dropping a specific database."""
    if dbname is None:
        return False

    def normalize_db_name(db):
        return db.lower().strip('`"')

    dbname = normalize_db_name(dbname)

    for query in sqlparse.parse(queries):
        if query.get_name() is None:
            continue

        first_token = query.token_first(skip_cm=True)
        _, second_token = query.token_next(0, skip_cm=True)
        database_name = normalize_db_name(query.get_name())
        if (first_token.value.lower() == 'drop' and
                second_token.value.lower() in ('database', 'schema') and
                database_name == dbname):
            return True


def need_completion_reset(queries):
    """Determines if the statement is a database switch such as 'use' or '\\u'.
    When a database is changed the existing completions must be reset before we
    start the completion refresh for the new database.
    """
    for query in sqlparse.split(queries):
        try:
            first_token = query.split()[0]
            if first_token.lower() in ('use', '\\u'):
                return True
        except Exception:
            return False


def is_mutating(status):
    """Determines if the statement is mutating based on the status."""
    if not status:
        return False

    mutating = set(['insert', 'update', 'delete', 'alter', 'create', 'drop',
                    'replace', 'truncate', 'load'])
    return status.split(None, 1)[0].lower() in mutating

def is_select(status):
    """Returns true if the first word in status is 'select'."""
    if not status:
        return False
    return status.split(None, 1)[0].lower() == 'select'


def thanks_picker(files=()):
    for filename in files:
        with open(filename, encoding='utf-8') as f:
            contents = f.readlines()

    return choice([x.split('*')[1].strip() for x in contents if x.startswith('*')])


@click.command()
@click.option('-v', '--version',
              is_flag=True,
              help='Print version.')
@click.option('--prompt',
              help='Prompt format (Default: "{0}").'.format(
                  SQLiteCli.DEFAULT_PROMPT))
@click.option('--sqliteclirc',
              type=click.Path(),
              default="~/.sqliteclirc",
              help='Location of sqliteclirc file.')
@click.argument('filename',
                required=False)
def cli(version, prompt, sqliteclirc, filename):
    '''A SQLite terminal client with auto-competion and syntax highlighting.

    \b
    Examples:
        - sqlitecli
        - sqlitecli filename
    '''
    if version:
        print('Version: ', __version__)
        sys.exit(0)

    sqlitecli = SQLiteCli(
        prompt=prompt,
        sqliteclirc=sqliteclirc
    )

    sqlitecli.connect(filename)

    if sys.stdin.isatty():
        sqlitecli.run_cli()


if __name__ == "__main__":
    cli()
