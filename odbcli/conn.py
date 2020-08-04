from enum import Enum
from cyanodbc import connect, Connection, SQLGetInfo, Cursor, DatabaseError, ConnectError
from typing import Optional
from functools import partial
from cli_helpers.tabular_output import TabularOutputFormatter
from multiprocessing import Process, Pipe
from logging import getLogger
from .executor import executor_process, cmsg, commandStatus

formatter = TabularOutputFormatter()

class connStatus(Enum):
    DISCONNECTED = 0
    IDLE = 1
    EXECUTING = 2
    FETCHING = 3

class sqlConnection:
    def __init__(
        self,
        dsn: str,
        conn: Optional[Connection] = Connection(),
        username: Optional[str] = "",
        password: Optional[str] = ""
    ) -> None:
        self.dsn = dsn
        self.conn = conn
        self.cursor: Cursor = None
        self.query: str = None
        self.username = username
        self.password = password
        self.status = connStatus.DISCONNECTED
        self.executor: Process = None
        self.parent_chan, self.child_chan = Pipe()
        self.logger = getLogger(__name__)
        self._quotechar = None

    @property
    def quotechar(self) -> str:
        if self._quotechar is None:
            self._quotechar = self.conn.get_info(
                    SQLGetInfo.SQL_IDENTIFIER_QUOTE_CHAR)
        return self._quotechar

    def connect(
            self,
            username: str = "",
            password: str = "",
            force: bool = False,
            start_executor: bool = False) -> None:
        uid = username or self.username
        pwd = password or self.password
        conn_str = "DSN=" + self.dsn + ";"
        if len(uid):
            self.username = uid
            conn_str = conn_str + "UID=" + uid + ";"
        if len(pwd):
            self.password = pwd
            conn_str = conn_str + "PWD=" + pwd + ";"
        if force or not self.conn.connected():
            self.conn = connect(dsn = conn_str, timeout = 5)
            self.status = connStatus.IDLE
        if start_executor:
            self.executor = Process(
                    target = executor_process,
                    args=(self.child_chan, self.logger.getEffectiveLevel(),))
            self.executor.start()
            self.logger.info("Started executor process: %d", self.executor.pid)
            self.parent_chan.send(cmsg("connect", conn_str, None))
            resp = self.parent_chan.recv()
            # How do you handle failure here?
            if not resp.status == commandStatus.OK:
                self.logger.error("Error atempting to connect in executor process")
                self.executor.terminate()
                self.executor.join()
                raise ConnectError("Connection failure in executor")

    def async_execute(self, query) -> cmsg:
        if self.executor and self.executor.is_alive():
            self.logger.debug("Sending query %s to pid %d",
                    query, self.executor.pid)
            self.parent_chan.send(
                    cmsg("execute", query, None))
            # Will block but can be interrupted
            res = self.parent_chan.recv()
            self.logger.debug("Execution done")
            self.query = query
            # Check if catalog has changed in which case
            # execute query locally
            self.parent_chan.send(cmsg("currentcatalog", None, None))
            rescat = self.parent_chan.recv()
            if rescat.status == commandStatus.FAIL:
                # TODO raise exception here since
                # connection catalogs are possibly out of sync
                # and we don't have a way of knowing
                res = cmsg("execute", "", commandStatus.FAIL)
            elif not rescat.payload == self.current_catalog():
                # query changed the catalog
                # so let's change the database locally
                self.logger.debug("Execution changed catalog")
                self.execute("USE " + rescat.payload)
        else:
            res = cmsg("execute", "", commandStatus.FAIL)
        return res

    def async_fetch(self, size) -> cmsg:
        if self.executor and self.executor.is_alive():
            self.logger.debug("Fetching size %d from pid %d",
                    size, self.executor.pid)
            self.parent_chan.send(cmsg("fetch", size, None))
            res = self.parent_chan.recv()
            self.logger.debug("Fetching done")
        else:
            res = cmsg("fetch", "", commandStatus.FAIL)
        return res

    def async_fetchdone(self) -> cmsg:
        if self.executor and self.executor.is_alive():
            self.parent_chan.send(cmsg("fetchdone", None, None))
            res = self.parent_chan.recv()
        else:
            res = cmsg("fetchdone", "", commandStatus.FAIL)
        return res

    def execute(self, query, parameters = None) -> Cursor:
        self.cursor = self.conn.cursor()
        self.cursor.execute(query, parameters)
        self.query = query
        return self.cursor

    def list_catalogs(self) -> list:
        return self.conn.list_catalogs()

    def list_schemas(self) -> list:
        if self.conn.connected():
            return self.conn.list_schemas()
        return None

    def find_tables(
            self,
            catalog = "",
            schema = "",
            table = "",
            type = "") -> list:
        res = self.conn.find_tables(catalog = catalog,
                schema = schema,
                table = table,
                type = type)
        return res

    def find_columns(self, catalog, schema, table, column):
        return self.conn.find_columns(
                catalog = catalog,
                schema = schema,
                table = table,
                column = column)

    def current_catalog(self) -> str:
        if self.conn.connected():
            return self.conn.catalog_name
        return None

    def connected(self) -> bool:
        return self.conn.connected()

    def catalog_support(self) -> bool:
        return self.conn.get_info(SQLGetInfo.SQL_CATALOG_NAME) == 'Y'

    def get_info(self, code: int) -> str:
        return self.conn.get_info(code)

    def close(self) -> None:
        if self.executor and self.executor.is_alive():
            self.executor.terminate()
            self.executor.join()
        # TODO: When disconnecting
        # We likely don't want to allow any exception to
        # propagate.  Catch DatabaseError?
        if self.conn.connected():
            self.conn.close()

    def close_cursor(self) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        self.query = None

    def preview_query(self, table, filter_query = "", limit = 1000) -> str:
        qry = "SELECT * FROM " + table + " " + filter_query
        if limit > 0:
            qry = qry + " LIMIT " + str(limit)
        return qry

    def formatted_fetch(self, size, format_name = "psql"):
        while True:
            res = self.async_fetch(size)
            if (res.status == commandStatus.FAIL) or (not res.type == "fetch"):
                return "Encountered a problem while fetching"
            elif len(res.payload[1]) == 0:
                break
            else:
                yield "\n".join(
                        formatter.format_output(
                            res.payload[1],
                            res.payload[0],
                            format_name = format_name))

connWrappers = {}

def mssql_preview_query(self, table, filter_query = "", limit = 1000) -> str:
    qry = " * FROM " + table + " " + filter_query
    if limit > 0:
        qry = "SELECT TOP " + str(limit) + qry
    else:
        qry = "SELECT" + qry
    return qry

connWrappers["MySQL"] = type("MySQL", (sqlConnection,), {
    "find_tables": lambda self, catalog, schema, table, type:
    self.conn.find_tables(catalog = schema, schema = "", table = table, type = type),
    "find_columns": lambda self, catalog, schema, table, column:
    self.conn.find_columns(catalog = schema, schema = "", table = table, column = column)
    })

connWrappers["Microsoft SQL Server"] = type(
        "Microsoft SQL Server", (sqlConnection,), {
            "preview_query": mssql_preview_query
        })
connWrappers["SQLite"] = type("SQLite", (sqlConnection,), {})
