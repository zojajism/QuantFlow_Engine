import json
import logging
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
import os



def setup_logger():

    load_dotenv()
    is_dev = os.getenv("IS_DEV")
    if is_dev == None:
        is_dev = False

    file_handler = TimedRotatingFileHandler(
        "quantflow_DataCollector.log",
        when="midnight",   
        interval=1,
        backupCount=7,     
        encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    file_handler.setFormatter(formatter)

    # Console Handler - only in Dev
    handlers = [file_handler]
    if is_dev:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)   # show all levels in console
        handlers.append(console_handler)

    
    logging.basicConfig(
        level=logging.INFO,
        handlers=handlers
    )

    logger = logging.getLogger("QuantFlow_DataCollector")
    logger.info(
                json.dumps({
                        "EventCode": 0,
                        "Message": f"Logger initialized (Dev Mode)" if is_dev else "Logger initialized (Production Mode)"
                    })
            )
    
    return logger