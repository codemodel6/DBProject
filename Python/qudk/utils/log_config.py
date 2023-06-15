import logging
from _datetime import datetime
def configure():
    now=datetime.now()
    file_handler = logging.FileHandler(f'./log_files/application_{now.year}-{now.month}-{now.day}.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler])