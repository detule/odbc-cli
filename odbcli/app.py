import logging
import os
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.application import get_app
from logging.handlers import RotatingFileHandler
from cyanodbc import datasources
from .sidebar import myDBConn
from .conn import sqlConnection
from .completion.mssqlcompleter import MssqlCompleter
from .config import get_config, config_location, ensure_dir_exists

class sqlApp:
    def __init__(
        self,
        odbclirc_file = None
    ) -> None:
        c = self.config = get_config(odbclirc_file)
        self.initialize_logging()
        self.set_default_pager(c)
        self.table_format = c["main"]["table_format"]
        self.syntax_style = c["main"]["syntax_style"]
        self.cli_style = c["colors"]
        self.multiline: bool = c["main"].as_bool("multi_line")
        # Hack here, will be better once we we bring _create_application
        self.editing_mode_initial = EditingMode.VI if c["main"].as_bool("vi") else EditingMode.EMACS

        self.show_sidebar: bool = False
        self.show_login_prompt: bool = False
        self.show_preview: bool = False
        self.active_conn = None
        self.obj_list = []
        dsns = list(datasources().keys())
        for dsn in dsns:
            self.obj_list.append(myDBConn(
                conn = sqlConnection(dsn = dsn),
                name = dsn,
                otype = "Connection"))
        for i in range(len(self.obj_list) - 1):
            self.obj_list[i].next_object = self.obj_list[i + 1]
        self.obj_list[0].select_next()
        self.completer = MssqlCompleter(smart_completion=True, my_app = self)

    @property
    def editing_mode(self) -> EditingMode:
        return get_app().editing_mode

    @editing_mode.setter
    def editing_mode(self, value: EditingMode) -> None:
        app = get_app()
        app.editing_mode = value

    @property
    def vi_mode(self) -> bool:
        return self.editing_mode == EditingMode.VI

    @vi_mode.setter
    def vi_mode(self, value: bool) -> None:
        if value:
            self.editing_mode = EditingMode.VI
        else:
            self.editing_mode = EditingMode.EMACS

    def set_default_pager(self, config):
        configured_pager = config["main"].get("pager")
        os_environ_pager = os.environ.get("PAGER")

        if configured_pager:
            self.logger.info(
                'Default pager found in config file: "%s"', configured_pager
            )
            os.environ["PAGER"] = configured_pager
        elif os_environ_pager:
            self.logger.info(
                'Default pager found in PAGER environment variable: "%s"',
                os_environ_pager,
            )
            os.environ["PAGER"] = os_environ_pager
        else:
            self.logger.info(
                "No default pager found in environment. Using os default pager"
            )

        # Set default set of less recommended options, if they are not already set.
        # They are ignored if pager is different than less.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-SRXF"

    def initialize_logging(self):
        log_file = self.config['main']['log_file']
        if log_file == 'default':
            log_file = config_location() + 'odbcli.log'
        ensure_dir_exists(log_file)
        log_level = self.config['main']['log_level']

        # Disable logging if value is NONE by switching to a no-op handler.
        # Set log level to a high value so it doesn't even waste cycles getting
        # called.
        if log_level.upper() == 'NONE':
            handler = logging.NullHandler()
        else:
            # creates a log buffer with max size of 20 MB and 5 backup files
            handler = RotatingFileHandler(os.path.expanduser(log_file),
                    encoding='utf-8', maxBytes=1024*1024*20, backupCount=5)

        level_map = {'CRITICAL': logging.CRITICAL,
                     'ERROR': logging.ERROR,
                     'WARNING': logging.WARNING,
                     'INFO': logging.INFO,
                     'DEBUG': logging.DEBUG,
                     'NONE': logging.CRITICAL
                     }

        log_level = level_map[log_level.upper()]

        formatter = logging.Formatter(
            '%(asctime)s (%(process)d/%(threadName)s) '
            '%(name)s %(levelname)s - %(message)s')

        handler.setFormatter(formatter)

        root_logger = logging.getLogger('odbcli')
        root_logger.addHandler(handler)
        root_logger.setLevel(log_level)

        root_logger.info('Initializing odbcli logging.')
        root_logger.debug('Log file %r.', log_file)
        self.logger = logging.getLogger(__name__)
