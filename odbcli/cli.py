#!/usr/bin/env python
"""
A simple example of a calculator program.
This could be used as inspiration for a REPL.
"""
import os
from sys import stderr
from time import time
from cyanodbc import DatabaseError, datasources
from click import echo_via_pager, secho
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.utils import get_cwidth
from .app import sqlApp, ExitEX
from .layout import sqlAppLayout
from .conn import connStatus
from .executor import cmsg, commandStatus


def main():

    my_app = sqlApp()
#    with patch_stdout():
    while True:
        try:
            app_res = my_app.application.run()
        except ExitEX:
            for i in range(len(my_app.obj_list)):
                my_app.obj_list[i].conn.close()
            return
        else:
            # If it's a preview query we need an indication
            # of where to run the query
            if(app_res[0] == "preview"):
                sqlConn = my_app.selected_object.conn
            else:
                sqlConn = my_app.active_conn
            if sqlConn is not None:
                #TODO also check that it is connected
                try:
                    secho("Executing query...Ctrl-c to cancel", err = False)
                    start = time()
                    res = sqlConn.async_execute(app_res[1])
                    execution = time() - start
                    sqlConn.status = connStatus.IDLE
                    secho("Query execution...done", err = False)
                    if(app_res[0] == "preview"):
                        continue
                    if my_app.timing_enabled:
                        print("Time: %0.03fs" % execution)
                    if res.status == commandStatus.OKWRESULTS:
                        ht = my_app.application.output.get_size()[0]
                        formatted = sqlConn.formatted_fetch(ht - 3 - my_app.pager_reserve_lines, my_app.table_format)
                        sqlConn.status = connStatus.FETCHING
                        echo_via_pager(formatted)
                    elif res.status == commandStatus.OK:
                        secho("No rows returned\n", err = False)
                    else:
                        secho("Query error: %s\n" % res.payload, err = True, fg = "red")
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
