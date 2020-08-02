#!/usr/bin/env python
"""
A simple example of a calculator program.
This could be used as inspiration for a REPL.
"""
import os
from sys import stderr
from cyanodbc import DatabaseError, datasources
from click import echo_via_pager, secho
from prompt_toolkit.shortcuts import print_formatted_text
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.utils import get_cwidth
from prompt_toolkit.application import Application, get_app
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.bindings.focus import focus_next
from prompt_toolkit.styles import Style
from .app import sqlApp
from .layout import sqlAppLayout
from .conn import connStatus
from .executor import cmsg, commandStatus
from .odbcstyle import style_factory

my_app = sqlApp()

def main():
    sql_layout = sqlAppLayout(my_app = my_app)
    kb = KeyBindings()

    @kb.add("c-q")
    def _(event):
        " Pressing Ctrl-Q or Ctrl-C will exit the user interface. "
        event.app.exit(exception = EOFError, style="class:exiting")
    # Global key bindings.
    kb.add("tab")(focus_next)
    @kb.add("c-f")
    def _(event):
        " Toggle between Emacs and Vi mode. "
        my_app.vi_mode = not my_app.vi_mode
    # apparently ctrls does this
    @kb.add("c-t")
    def _(event):
        """
        Show/hide sidebar.
        """
        my_app.show_sidebar = not my_app.show_sidebar
    sidebar_visible = Condition(lambda: my_app.show_sidebar and not my_app.show_login_prompt and not my_app.show_preview)
    @kb.add("up", filter=sidebar_visible)
    @kb.add("c-p", filter=sidebar_visible)
    @kb.add("k", filter=sidebar_visible)
    def _(event):
        " Go to previous option. "
        my_app.obj_list[0].select_previous()

    @kb.add("down", filter=sidebar_visible)
    @kb.add("c-n", filter=sidebar_visible)
    @kb.add("j", filter=sidebar_visible)
    def _(event):
        " Go to next option. "
        my_app.obj_list[0].select_next()

    @kb.add("enter", filter=sidebar_visible)
    def _(event):
        " If connection, connect.  If table preview"
        obj = my_app.obj_list[0].selected_object
        if type(obj).__name__ == "myDBConn" and not obj.conn.connected():
            my_app.show_login_prompt = True
            event.app.layout.focus(sql_layout.lprompt)
        if type(obj).__name__ == "myDBConn" and obj.conn.connected():
            # OG: some thread locking may be needed here
            my_app.completer.reset_completions()
            my_app.active_conn = obj.conn
        elif type(obj).__name__ == "myDBTable":
            my_app.show_preview = True
            event.app.layout.focus(sql_layout.preview)

    @kb.add("right", filter=sidebar_visible)
    @kb.add("l", filter=sidebar_visible)
    @kb.add(" ", filter=sidebar_visible)
    def _(event):
        " Select next value for current option. "
        my_app.obj_list[0].selected_object.expand()

    @kb.add("left", filter=sidebar_visible)
    @kb.add("h", filter=sidebar_visible)
    def _(event):
        " Select next value for current option. "
        obj = my_app.obj_list[0].selected_object
        if obj is not None:
            obj.collapse()

    # Run application.
    application = Application(
        layout = sql_layout.layout,
        key_bindings = kb,
        enable_page_navigation_bindings = True,
        style = style_factory(my_app.syntax_style, my_app.cli_style),
        include_default_pygments_style = False,
        mouse_support = True,
        full_screen = False,
    )

#    with patch_stdout():
    while True:
        try:
            text = application.run()
        except EOFError:
            for i in range(len(my_app.obj_list) - 1):
                my_app.obj_list[i].conn.close()
            return
        else:
            sqlConn = my_app.active_conn
            if sqlConn is not None:
                #TODO also check that it is connected
                try:
                    res = sqlConn.async_execute(sql_layout.input_buffer.text)
                    sqlConn.status = connStatus.IDLE
                    if res.status == commandStatus.OKWRESULTS:
                        ht = application.output.get_size()[0]
                        formatted = sqlConn.formatted_fetch(ht - 4, my_app.table_format)
                        sqlConn.status = connStatus.FETCHING
                    else:
                        formatted = "No rows returned\n"
                    echo_via_pager(formatted)
                except BrokenPipeError:
                    my_app.logger.debug('BrokenPipeError caught. Recovering...', file = stderr)
                except KeyboardInterrupt:
                    secho("Cancelling query...", err = True, fg = 'red')
                    sqlConn.executor.terminate()
                    sqlConn.executor.join()
                    secho("Query cancelled.", err = True, fg='red')
                    #TODO: catch ConnectError
                    sqlConn.connect(start_executor = True)
                sqlConn.status = connStatus.IDLE
                # TODO check status of return
                sqlConn.async_fetchdone()
#                sqlConn.parent_chan.send(cmsg("fetchdone", None, None))
#                sqlConn.parent_chan.recv()
