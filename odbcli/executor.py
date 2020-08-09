import logging
from enum import IntEnum
from cyanodbc import Connection, Cursor, connect, DatabaseError
from logging.handlers import RotatingFileHandler
from logging import (
        NullHandler,
        CRITICAL,
        ERROR,
        WARNING,
        INFO,
        DEBUG,
        Formatter,
        getLogger
)

from collections import namedtuple
from os.path import expanduser
from os import getpid
from .config import config_location

cmsg = namedtuple('cmsg', ['type', 'payload', 'status'])

class commandStatus(IntEnum):
    OK = 0
    FAIL = 1
    OKWRESULTS = 2

def initiate_logging(log_level = INFO):

    log_file = config_location() + "executor_" + str(getpid()) + ".log"
    # Disable logging if value is NONE by switching to a no-op handler.
    # Set log level to a high value so it doesn't even waste cycles getting
    # called.
    if log_level == CRITICAL:
        handler = NullHandler()
    else:
        # creates a log buffer with max size of 20 MB and 5 backup files
        handler = RotatingFileHandler(expanduser(log_file),
                encoding='utf-8', maxBytes=1024*1024*20, backupCount=5)

    lformatter = Formatter(
        '%(asctime)s (%(process)d/%(threadName)s) '
        '%(name)s %(levelname)s - %(message)s')

    handler.setFormatter(lformatter)

    root_logger = getLogger('executor')
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    root_logger.info('Initializing executor process')
    root_logger.debug('Log file %r.', log_file)
    return root_logger

def executor_process(chan, log_level = INFO):
    conn = Connection()
    crsr = Cursor(conn)

    my_logger = initiate_logging(log_level)
    while True:
        my_logger.debug("Waiting for message")
        try:
            # Parent process handles KeyboardInterrupts
            msg = chan.recv()
        except KeyboardInterrupt:
            continue
        my_logger.debug("Message received %s", msg.type)
        status = commandStatus.OK
        response = ""
        if msg.type == "connect":
            #TODO: handle hardcoded value
            try:
                conn._connect(dsn = msg.payload, timeout = 5)
            except ConnectError as e:
                response = str(e)
                status = commandStatus.FAIL
        elif msg.type == "execute":
            #TODO: Check to make sure we connected
            try:
                crsr.execute(msg.payload)
                if crsr.description:
                    cols = [col.name for col in crsr.description]
                else:
                    cols = []
                if len(cols):
                    status = commandStatus.OKWRESULTS
                else:
                    status = commandStatus.OK
            except DatabaseError as e:
                status = commandStatus.FAIL
                response = str(e)
        elif msg.type == "fetch":
            #TODO: Check to make sure we connected
            try:
                if crsr.description:
                    cols = [col.name for col in crsr.description]
                else:
                    cols = []
                if len(cols):
                    response = (cols, crsr.fetchmany(msg.payload))
                else:
                    # Shouldn't get here, since execute returns
                    # just plain OK, rather than OKWRESULTS
                    # why would you fetch?
                    response = ([], [])
            except KeyboardInterrupt:
                # Let parent process handle KeyboardInterrupts
                continue
            except DatabaseError as e:
                status = commandStatus.FAIL
                response = str(e)
        elif msg.type == "currentcatalog":
            if conn.connected():
                response = conn.catalog_name
            else:
                status = commandStatus.FAIL
        elif msg.type == "listschemas":
            if conn.connected():
                response = conn.list_schemas()
            else:
                status = commandStatus.FAIL
        elif msg.type == "fetchdone":
            crsr.close()
        else:
            status = commandStatus.FAIL
            response = "Unknown command"

        my_logger.debug("Sending message back %d", int(status))
        chan.send(cmsg(msg.type, response, status))
