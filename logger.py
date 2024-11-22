import logging
from logging.handlers import RotatingFileHandler

# Конфигурация логгера
LOG_FILE = 'update_data.log'
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s, %(levelname)s, %(message)s, %(funcName)s'
LOG_ENCODING = 'windows-1251'

logger = logging.getLogger('task_logger')
logger.setLevel(LOG_LEVEL)

file_handler = RotatingFileHandler(
    LOG_FILE,
    mode='a',
    delay=True,
    maxBytes=2 * 1024 * 1024,  # Максимальный размер файла 2 MB
    # backupCount=2,  # Хранить до 2 архивных файлов
    encoding=LOG_ENCODING
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

LOGGER_ERROR_TYPE = {
    'error': logger.error,
    'critical': logger.critical,
    'info': logger.info,
    'warning': logger.warning
}
