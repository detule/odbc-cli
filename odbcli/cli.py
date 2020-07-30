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
os.environ["LESS"] = "-SRXF"

dsns = datasources()
my_app = sqlApp(dsns = list(dsns.keys()))

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

    # Style.
    style = Style(
        [
            ("preview-output-field", "bg:#000044 #ffffff"),
            ("preview-input-field", "bg:#000066 #ffffff"),
            ("preview-divider-line", "#000000"),
            ("completion-menu.completion.current", 'bg:#ffffff #000000'),
            ("completion-menu.completion", 'bg:#008888 #ffffff'),
            ("completion-menu.meta.completion.current", 'bg:#44aaaa #000000'),
            ("completion-menu.meta.completion", 'bg:#448888 #ffffff'),
            ("completion-menu.multi-column-meta", 'bg:#aaffff #000000'),
            ("status-toolbar", "bg:#222222 #aaaaaa"),
            ("status-toolbar.title", "underline"),
            ("status-toolbar.inputmode", "bg:#222222 #ffffaa"),
            ("status-toolbar.key", "bg:#000000 #888888"),
            ("status-toolbar.pastemodeon", "bg:#aa4444 #ffffff"),
            ("status-toolbar.pythonversion", "bg:#222222 #ffffff bold"),
            ("status-toolbar paste-mode-on", "bg:#aa4444 #ffffff"),
            ("record", "bg:#884444 white"),
            ("status-toolbar.input-mode", "#ffff44"),
            ("status-toolbar.conn-executing", "bg:red #ffff44"),
            ("status-toolbar.conn-fetching", "bg:yellow black"),
            ("status-toolbar.conn-idle", "bg:#668866 #ffffff"),
    # The options sidebar.
            ("sidebar", "bg:#bbbbbb #000000"),
            ("sidebar.title", "bg:#668866 fg:#ffffff"),
            ("sidebar.label", "bg:#bbbbbb fg:#222222"),
            ("sidebar.status", "bg:#dddddd #000011"),
            ("sidebar.label selected", "bg:#222222 #eeeeee bold"),
            ("sidebar.status selected", "bg:#444444 #ffffff bold"),
            ("sidebar.label active", "bg:#668866 #ffffff"),
            ("sidebar.status active", "bg:#88AA88 #ffffff"),
            ("sidebar.separator", "underline"),
            ("sidebar.key", "bg:#bbddbb #000000 bold"),
            ("sidebar.key.description", "bg:#bbbbbb #000000"),
            ("sidebar.helptext", "bg:#fdf6e3 #000011"),
        ]
    )

    # Run application.
    application = Application(
        layout = sql_layout.layout,
        key_bindings = kb,
        enable_page_navigation_bindings = True,
        style = style,
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
                        formatted = sqlConn.formatted_fetch(ht - 4)
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
