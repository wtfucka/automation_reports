import logging
from logging.handlers import RotatingFileHandler

# Конфигурация логгера
LOG_FILE = 'log/update_autoreports_data.log'
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s, %(levelname)s, %(message)s, %(funcName)s'
LOG_ENCODING = 'windows-1251'

logger = logging.getLogger('task_logger')
logger.setLevel(LOG_LEVEL)

file_handler = RotatingFileHandler(
    LOG_FILE,
    mode='a',
    delay=True,
    maxBytes=4 * 1024 * 1024,  # Максимальный размер файла 4 MB
    backupCount=5,  # Хранить до 5 архивных файлов
    encoding=LOG_ENCODING
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def _log_with_caller_info(
        level: str,
        message: str,
        stacklevel: int = 2
        ) -> None:
    """
    Вспомогательная функция для логирования с корректным stacklevel.

    :param level: Уровень логирования (info, warning, error, critical)
    :param message: Сообщение для логирования
    :param stacklevel: Уровень стека для определения вызывающей функции
    """
    log_func = getattr(logger, level)
    # +1 потому что мы добавляем еще один уровень (_log_with_caller_info)
    log_func(message, stacklevel=stacklevel + 1)


LOGGER_ERROR_TYPE = {
    'error': lambda msg, stacklevel=2: _log_with_caller_info(
        'error', msg, stacklevel
    ),
    'critical': lambda msg, stacklevel=2: _log_with_caller_info(
        'critical', msg, stacklevel
    ),
    'info': lambda msg, stacklevel=2: _log_with_caller_info(
        'info', msg, stacklevel
    ),
    'warning': lambda msg, stacklevel=2: _log_with_caller_info(
        'warning', msg, stacklevel
    )
}
