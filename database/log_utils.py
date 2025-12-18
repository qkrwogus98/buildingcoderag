# log_utils.py
import logging
from config import LOG_FILE_PATH

def setup_custom_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    
    # 콘솔 핸들러
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # 파일 핸들러
    file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger