#!/usr/bin/env python
"""
A simple example of a calculator program.
This could be used as inspiration for a REPL.
"""
import os
from sys import stderr
from cyanodbc import DatabaseError, datasources
from click import echo_via_pager, secho
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.utils import get_cwidth
from .app import sqlApp
from .layout import sqlAppLayout
from .conn import connStatus
from .executor import cmsg, commandStatus


def main():

    my_app = sqlApp()
#    with patch_stdout():
    while True:
        try:
            text = my_app.application.run()
        except EOFError:
            for i in range(len(my_app.obj_list) - 1):
                my_app.obj_list[i].conn.close()
            return
        else:
            sqlConn = my_app.active_conn
            if sqlConn is not None:
                #TODO also check that it is connected
                try:
                    res = sqlConn.async_execute(my_app.sql_layout.input_buffer.text)
                    sqlConn.status = connStatus.IDLE
                    if res.status == commandStatus.OKWRESULTS:
                        ht = my_app.application.output.get_size()[0]
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
