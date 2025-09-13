import logging
import os
from pathlib import Path

def setup_logger(name):
    # Get the logger for the current module
    logger = logging.getLogger(name)

    project_root = Path(__file__).parent.parent.parent
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s |%(filename)s (%(lineno)d) |%(levelname)s:| %(message)s',
    # NOTE: Added datefmt to format the time in the log file for better readability
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / 'orchestration.log')
        ]
    )
    return logger