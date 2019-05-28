import logging
import logging.config
from time import time, strftime

global info_logger
global error_logger

info_name = r"/root/CR-SIM/log/info-" + strftime("%Y%m%d.%H.%M.%S") + ".log"
error_name = r"/root/CR-SIM/log/error-" + strftime("%Y%m%d.%H.%M.%S") + ".log"

fmt = "%(asctime)s - %(name)s - %(filename)s - %(funcName)s - %(lineno)d - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
fmt_str = logging.Formatter(fmt, datefmt)

info_logger = logging.getLogger('info')
error_logger = logging.getLogger('error')
info_logger.setLevel(logging.INFO)
error_logger.setLevel(logging.INFO)
info_handler = logging.FileHandler(info_name)
error_handler = logging.FileHandler(error_name)
info_handler.setLevel(logging.INFO)
error_handler.setLevel(logging.INFO)
info_handler.setFormatter(fmt_str)
error_handler.setFormatter(fmt_str)

info_logger.addHandler(info_handler)
error_logger.addHandler(error_handler)


if __name__ == "__main__":
    pass
