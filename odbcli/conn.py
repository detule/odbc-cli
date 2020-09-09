from enum import Enum
from cyanodbc import connect, Connection, SQLGetInfo, Cursor, DatabaseError, ConnectError
from typing import Optional
from cli_helpers.tabular_output import TabularOutputFormatter
from multiprocessing import Process, Pipe
from logging import getLogger
from re import sub
from threading import Lock
from .executor import executor_process, cmsg, commandStatus

formatter = TabularOutputFormatter()

class connStatus(Enum):
    DISCONNECTED = 0
    IDLE = 1
    EXECUTING = 2
    FETCHING = 3
    ERROR = 4

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
        self._search_escapechar = None
        self._search_escapepattern = None
        # Lock to be held by database interaction that happens
        # in the main process.  Recall, main-buffer as well as preview
        # buffer queries get executed in a separate process, however
        # auto-completion, as well as object browser expansion happen
        # in the main process possibly multi-threaded.  Multi threaded is fine
        # we don't want the main process to lock-up while writing a query,
        # however, we don't want to potentially hammer the connection with
        # multiple auto-completion result queries before each has had a chance
        # to return.
        self._lock = Lock()

    @property
    def quotechar(self) -> str:
        if self._quotechar is None:
            self._quotechar = self.conn.get_info(
                    SQLGetInfo.SQL_IDENTIFIER_QUOTE_CHAR)
            # pyodbc note
            # self._quotechar = self.conn.getinfo(
        return self._quotechar

    @property
    def search_escapechar(self) -> str:
        if self._search_escapechar is None:
            self._search_escapechar = self.conn.get_info(
                    SQLGetInfo.SQL_SEARCH_PATTERN_ESCAPE)
        return self._search_escapechar

    @property
    def search_escapepattern(self) -> str:
        if self._search_escapepattern is None:
            # https://stackoverflow.com/questions/2428117/casting-raw-strings-python
            self._search_escapepattern = \
                (self.search_escapechar).encode("unicode-escape").decode() + "\\1"

        return self._search_escapepattern

    def sanitize_search_string(self, term) -> str:
        if term is not None and len(term):
            res = sub("(_|%)", self.search_escapepattern, term)
        else:
            res = term
        return term

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
            try:
                self.conn = connect(dsn = conn_str, timeout = 5)
                self.status = connStatus.IDLE
            except ConnectError as e:
                self.logger.error("Error while connecting: %s", str(e))
                raise ConnectError(e)
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

    def async_lastresponse(self) -> cmsg:
        if self.executor and self.executor.is_alive():
            self.logger.debug("Asking for last message, executor pid %d",
                    self.executor.pid)
            self.parent_chan.send(cmsg("lastresponse", None, None))
            resp = self.parent_chan.recv()
            # Above should never fail
            return resp

    def async_execute(self, query) -> cmsg:
        if self.executor and self.executor.is_alive():
            self.logger.debug("Sending query %s to pid %d",
                    query, self.executor.pid)
            # TODO: message should carry
            # current catalog.  One might
            # think that the main process
            # connection always "follows"
            # database changes since all
            # main queries get executed
            # against executor thread
            # and main process conn only
            # gets used for sidebar/auto
            # completion.  But, for
            # example the MYSQL driver
            # if starting without a
            # declared database will just
            # switch to the first db
            # when running find_columns
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
        with self._lock:
            self.cursor = self.conn.cursor()
            self.cursor.execute(query, parameters)
            self.query = query
        return self.cursor

    def list_catalogs(self) -> list:
        # pyodbc note
        # return conn.cursor().tables(catalog = "%").fetchall()
        res = []
        try:
            if self.conn.connected():
                self.logger.debug("Calling list_catalogs...")
                with self._lock:
                    res = self.conn.list_catalogs()
                self.logger.debug("list_catalogs: done")
        except DatabaseError as e:
            self.status = connStatus.ERROR
            self.logger.warning("list_catalogs: %s", str(e))

        return res

    def list_schemas(self, catalog = None) -> list:
        res = []

        # We only trust this generic implementation if attempting to list
        # schemata in curent catalog (or catalog argument is None)
        if catalog is not None and not catalog == self.current_catalog():
            return res

        try:
            if self.conn.connected():
                self.logger.debug("Calling list_schemas...")
                with self._lock:
                    res = self.conn.list_schemas()
                self.logger.debug("list_schemas: done")
        except DatabaseError as e:
            self.status = connStatus.ERROR
            self.logger.warning("list_schemas: %s", str(e))

        return res

    def find_tables(
            self,
            catalog = "",
            schema = "",
            table = "",
            type = "") -> list:
        res = []

        try:
            if self.conn.connected():
                self.logger.debug("Calling find_tables...")
                with self._lock:
                    res = self.conn.find_tables(
                        catalog = catalog,
                        schema = schema,
                        table = table,
                        type = type)
                self.logger.debug("find_tables: done")
        except DatabaseError as e:
            self.logger.warning("find_tables: %s.%s.%s, type %s: %s", catalog, schema, table, type, str(e))

        return res

    def find_columns(
            self,
            catalog = "",
            schema = "",
            table = "",
            column = "") -> list:
        res = []

        try:
            if self.conn.connected():
                self.logger.debug("Calling find_columns...")
                with self._lock:
                    res = self.conn.find_columns(
                            catalog = catalog,
                            schema = schema,
                            table = table,
                            column = column)
                self.logger.debug("find_columns: done")
        except DatabaseError as e:
            self.logger.warning("find_columns: %s.%s.%s, column %s: %s", catalog, schema, table, column, str(e))

        return res

    def current_catalog(self) -> str:
        if self.conn.connected():
            return self.conn.catalog_name
        return None

    def connected(self) -> bool:
        return self.conn.connected()

    def catalog_support(self) -> bool:
        res = self.conn.get_info(SQLGetInfo.SQL_CATALOG_NAME)
        return res == True or res == 'Y'
        # pyodbc note
        # return self.conn.getinfo(pyodbc.SQL_CATALOG_NAME) == True or self.conn.getinfo(pyodbc.SQL_CATALOG_NAME) == 'Y'

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

    def preview_query(self, table, filter_query = "", limit = -1) -> str:
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

class MSSQL(sqlConnection):
    def find_tables(
            self,
            catalog = "",
            schema = "",
            table = "",
            type = "") -> list:
        """ FreeTDS does not allow us to query catalog == '', and
            schema = '' which, according to the ODBC spec for SQLTables should
            return tables outside of any catalog/schema.  In the case of FreeTDS
            what gets passed to the sp_tables sproc is null, which in turn
            is interpreted as a wildcard.  For the time being intercept
            these queries here (used in auto completion) and return empty
            set.  """

        if catalog == "\x00" and schema == "\x00":
            return []

        return super().find_tables(
                catalog = catalog,
                schema = schema,
                table = table,
                type = type)

    def list_schemas(self, catalog = None) -> list:
        """ Optimization for listing out-of-database schemas by
            always querying catalog.sys.schemas. """
        res = []
        qry = "SELECT name FROM {catalog}.sys.schemas " \
              "WHERE name NOT IN ('db_owner', 'db_accessadmin', " \
              "'db_securityadmin', 'db_ddladmin', 'db_backupoperator', " \
              "'db_datareader', 'db_datawriter', 'db_denydatareader', " \
              "'db_denydatawriter')"

        if catalog is None and self.current_catalog():
            catalog = self.sanitize_search_string(self.current_catalog())

        if catalog:
            try:
                self.logger.debug("Calling list_schemas...")
                crsr = self.execute(qry.format(catalog = catalog))
                res = crsr.fetchall()
                crsr.close()
                self.logger.debug("Calling list_schemas: done")
                schemas = [r[0] for r in res]
                if len(schemas):
                    return schemas
            except DatabaseError as e:
                self.logger.warning("MSSQL list_schemas: %s", str(e))

        return super().list_schemas(catalog = catalog)

    def mssql_preview_query(
            self,
            table,
            filter_query = "",
            limit = -1) -> str:
        qry = " * FROM " + table + " " + filter_query
        if limit > 0:
            qry = "SELECT TOP " + str(limit) + qry
        else:
            qry = "SELECT" + qry
        return qry

class PSSQL(sqlConnection):
    def find_tables(
            self,
            catalog = "",
            schema = "",
            table = "",
            type = "") -> list:
        """ At least the psql odbc driver I am using has an annoying habbit
            of treating the catalog and schema fields interchangible, which
            in turn screws up with completion"""

        if not catalog in [self.current_catalog(), self.sanitize_search_string(self.current_catalog())]:
            return []

        return super().find_tables(
                catalog = catalog,
                schema = schema,
                table = table,
                type = type)

    def find_columns(
            self,
            catalog = "",
            schema = "",
            table = "",
            column = "") -> list:
        """ At least the psql odbc driver I am using has an annoying habbit
            of treating the catalog and schema fields interchangible, which
            in turn screws up with completion"""

        if not catalog in [self.current_catalog(), self.sanitize_search_string(self.current_catalog())]:
            return []

        return super().find_columns(
                catalog = catalog,
                schema = schema,
                table = table,
                column = column)

class MySQL(sqlConnection):

    def list_schemas(self, catalog = None) -> list:
        """ Only catalogs for MySQL, it seems,
            however, list_schemas returns [""] which
            causes blank entries to show up in auto
            completion.  Also confuses some of the checks we have
            that look for len(list_schemas) < 1 to decide whether
            to fall-back to find_tables.  Make sure that for MySQL
            we do, in-fact fall-back to find_tables"""
        return []

    def find_tables(
            self,
            catalog = "",
            schema = "",
            table = "",
            type = "") -> list:

        if catalog in ["", "null"] and schema not in ["", "null"]:
            catalog = schema
            schema = ""

        return super().find_tables(
                catalog = catalog,
                schema = schema,
                table = table,
                type = type)

    def find_columns(
            self,
            catalog = "",
            schema = "",
            table = "",
            column = "") -> list:

        if catalog in ["", "null"] and schema not in ["", "null"]:
            catalog = schema
            schema = ""

        return super().find_columns(
                catalog = catalog,
                schema = schema,
                table = table,
                column = column)

class SQLite(sqlConnection):

    def list_schemas(self, catalog = None) -> list:
        """Easy peasy"""
        return []
    def list_catalogs(self) -> list:
        """Easy peasy"""
        return []

connWrappers["MySQL"] = MySQL
connWrappers["Microsoft SQL Server"] = MSSQL
connWrappers["SQLite"] = SQLite
connWrappers["PostgreSQL"] = PSSQL
