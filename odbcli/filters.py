from prompt_toolkit.filters import Filter
from prompt_toolkit.application import get_app
from re import sub, findall

class SqlAppFilter(Filter):
    def __init__(self, sql_app: "sqlApp") -> None:
        self.my_app = sql_app

    def __call__(self) -> bool:
        raise NotImplementedError

class ShowSidebar(SqlAppFilter):
    def __call__(self) -> bool:
        return self.my_app.show_sidebar and not self.my_app.show_exit_confirmation

class ShowLoginPrompt(SqlAppFilter):
    def __call__(self) -> bool:
        return self.my_app.show_login_prompt

class ShowPreview(SqlAppFilter):
    def __call__(self) -> bool:
        return self.my_app.show_preview

class ShowDisconnectDialog(SqlAppFilter):
    def __call__(self) -> bool:
        return self.my_app.show_disconnect_dialog

class MultilineFilter(SqlAppFilter):
    def _is_open_quote(self, sql: str):
        """ To implement """
        return False
    def _is_query_executable(self, sql: str):
        # A complete command is an sql statement that ends with a 'GO', unless
        # there's an open quote surrounding it, as is common when writing a
        # CREATE FUNCTION command
        if sql is not None and sql != "":
            # remove comments
            #esql = sqlparse.format(sql, strip_comments=True)
            # check for open comments
            # remove all closed quotes to isolate instances of open comments
            sql_no_quotes = sub(r'".*?"|\'.*?\'', '', sql)
            is_open_comment = len(findall(r'\/\*', sql_no_quotes)) > 0
            # check that 'go' is only token on newline
            lines = sql.split('\n')
            lastline = lines[len(lines) - 1].lower().strip()
            is_valid_go_on_lastline = lastline == 'go'
            # check that 'go' is on last line, not in open quotes, and there's no open
            # comment with closed comments and quotes removed.
            # NOTE: this method fails when GO follows a closing '*/' block comment on the same line,
            # we've taken a dependency with sqlparse
            # (https://github.com/andialbrecht/sqlparse/issues/484)
            return not self._is_open_quote(sql) and not is_open_comment and is_valid_go_on_lastline
        return False


    def _multiline_exception(self, text: str):
        text = text.strip()
        return (
            text.startswith('\\') or  # Special Command
            text.endswith(r'\e') or  # Ended with \e which should launch the editor
            self._is_query_executable(text) or  # A complete SQL command
            (text.endswith(';')) or # GO doesn't work everywhere
            (text == 'exit') or  # Exit doesn't need semi-colon
            (text == 'quit') or  # Quit doesn't need semi-colon
            (text == ':q') or  # To all the vim fans out there
            (text == '')  # Just a plain enter without any text
        )

    def __call__(self) -> bool:
        doc = get_app().layout.get_buffer_by_name("defaultbuffer").document
        if not self.my_app.multiline:
            return False
        return not self._multiline_exception(doc.text)
