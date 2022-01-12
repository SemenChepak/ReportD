import logging
import getpass

from loger.cred_holder import Cred


class Logger(object):
    path = Cred()
    logging.basicConfig(
        filename=path.log,
        filemode='a',
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S', )

    def __init__(self, name):
        self.filename = f"daily_report.log",
        self.filemode = 'a'
        self.format = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s'
        self.level = logging.INFO
        self.datefmt = '%H:%M:%S'
        self.logger = logging.getLogger(getpass.getuser())
        self.logger.setLevel(self.level)
        self.name = name

    def info(self, msg, extra=None):
        self.logger.info(f'{self.name} {msg}', extra=extra)

    def error(self, msg, extra=None):
        self.logger.error(f'{self.name} {msg}', extra=extra)

    def debug(self, msg, extra=None):
        self.logger.debug(f'{self.name} {msg}', extra=extra)

    def warn(self, msg, extra=None):
        self.logger.warning(f'{self.name} {msg}', extra=extra)
