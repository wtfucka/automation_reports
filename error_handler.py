# from subprocess import CalledProcessError, run

# from constants import DEFAULT_SENDER_FILE, PARAMS
from logger import LOGGER_ERROR_TYPE

ERROR_TYPES = set()

# пока что не работает должным образом, т.к. файл лога занят,
# отправка не происходит
# def send_mail() -> None:
#     '''
#     Отпралвляет письмо с логом, если были ошибки.
#     '''
#     try:
#         command = [DEFAULT_SENDER_FILE, PARAMS]
#         run(command, check=True)
#         LOGGER_ERROR_TYPE['info']('Письмо успешно отправлено.')
#     except FileNotFoundError:
#         LOGGER_ERROR_TYPE['critical'](
#             'Не найден файл отправки: DEFAULT_SENDER_FILE.'
#             )
#     except CalledProcessError as error:
#         LOGGER_ERROR_TYPE['error'](f'Ошибка при отправке письма: {error}')


def handle_error(error: str, error_type: str) -> None:
    '''
    Обработчик ошибок базы данных с логированием и отправкой email.

    :param error: Текст ошибки
    :param error_type: Уровень логирования - info, error, critical, warning.
    '''
    if error_type not in LOGGER_ERROR_TYPE:
        LOGGER_ERROR_TYPE['warning'](
            f'Некорректный тип ошибки: {error_type}. Текст ошибки: {error}'
            )
        return

    LOGGER_ERROR_TYPE[error_type](error)
    if error_type in ['error', 'critical']:
        ERROR_TYPES.add(error_type)
