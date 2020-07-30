from .sidebar import myDBConn
from .conn import sqlConnection
from .completion.mssqlcompleter import MssqlCompleter
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.application import get_app
from logging.handlers import RotatingFileHandler
import logging
import os

class sqlApp:
    def __init__(
        self,
        dsns: Optional[List[str]]
    ) -> None:
        self.show_sidebar: bool = False
        self.show_login_prompt: bool = False
        self.show_preview: bool = False
        self.multiline: bool = True
        self.active_conn = None
        self.obj_list = []
        for dsn in dsns:
            self.obj_list.append(myDBConn(
                conn = sqlConnection(dsn = dsn),
                name = dsn,
                otype = "Connection"))
        for i in range(len(self.obj_list) - 1):
            self.obj_list[i].next_object = self.obj_list[i + 1]
        self.obj_list[0].select_next()
        self.initialize_logging()
        self.logger = logging.getLogger(u'sqlApp.main')
        self.completer = MssqlCompleter(smart_completion=True, my_app = self)
#        self.preview_loop = asyncio.new_event_loop()

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

    def initialize_logging(self):
#        log_file = self.config['main']['log_file']
#        if log_file == 'default':
#            log_file = config_location() + 'mssqlcli.log'
        log_file = '/home/oliver/project/sqlApp.log'
#        ensure_dir_exists(log_file)
#        log_level = self.config['main']['log_level']
        log_level = "INFO"

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

        root_logger = logging.getLogger('sqlApp')
        root_logger.addHandler(handler)
        root_logger.setLevel(log_level)

        root_logger.info('Initializing sqlApp logging.')
        root_logger.debug('Log file %r.', log_file)
