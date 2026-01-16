import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

from logger import LOGGER_ERROR_TYPE


def _send_email_with_log(
        smtp_server: str,
        smtp_port: int,
        sender: str,
        recipients: list[str],
        subject: str,
        html_body: str,
        log_file_path: str,
        password: str | None = None
        ) -> None:
    """
    Отправляет HTML email с вложенным лог-файлом.

    :param smtp_server: SMTP сервер.
    :param smtp_port: Порт SMTP.
    :param sender: Email отправителя.
    :param recipients: Список email получателей.
    :param subject: Тема письма.
    :param html_body: HTML содержимое письма.
    :param log_file_path: Путь к лог-файлу для вложения.
    :param password: Пароль SMTP (опционально).
    :raises smtplib.SMTPException: При ошибке отправки.
    """
    # Создаём сообщение
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ';'.join(recipients)
    msg['Subject'] = subject

    # Добавляем HTML тело
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    # Прикрепляем лог-файл (если существует)
    log_path = Path(log_file_path)
    if log_path.exists() and log_path.stat().st_size > 0:
        with open(log_path, 'rb') as f:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(f.read())

        encoders.encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename= {log_path.name}'
        )
        msg.attach(attachment)

    # Отправляем через SMTP
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.login(sender, password)  # type: ignore
    server.send_message(msg)
    server.quit()


def send_error_report(error_types: set[str]) -> None:
    """
    Отправляет email администраторам при наличии error/critical ошибок.
    Отправка происходит только в случае наличия ошибок типа error
     или critical.

    :param error_types: Множество типов ошибок из error_handler.
    """
    from datetime import datetime

    from utils import load_configs

    # Проверка: есть ли error или critical?
    if not any(et in ['error', 'critical'] for et in error_types):
        return  # Нет критических ошибок - ничего не делаем

    # Получаем настройки
    configs: dict = load_configs('file_name.json')
    mailer_config: dict = configs.get('Mailer', {})

    smtp_server = mailer_config.get('smtp_server', None)
    smtp_port = mailer_config.get('smtp_port', None)
    sender = mailer_config.get('sender', None)
    password = mailer_config.get('password', None)
    recipients = ['fake@fake.ru']

    # Если настройки не заданы - просто логируем и выходим
    if not all([smtp_server, sender, recipients]):
        LOGGER_ERROR_TYPE['warning'](
            'SMTP настройки не заданы, email не отправлен'
        )
        return

    # Форматируем типы ошибок
    error_types_str = ', '.join(sorted(error_types))

    # Читаем HTML шаблон
    html_file = Path('error_notification.html')
    if html_file.exists():
        html_body = html_file.read_text(encoding='utf-8')
        # Подставляем переменные в шаблон
        html_body = html_body.format(
            timestamp=datetime.now(),
            error_types=error_types_str
        )
    else:
        # Простой HTML на случай если шаблона нет
        html_body = f'''
        <html>
        <body>
            <h2>Ошибка в сервисе Automation_Reports</h2>
            <p>Обнаружены ошибки: {error_types_str}</p>
            <p>Проверьте приложенный лог-файл.</p>
        </body>
        </html>
        '''

    # Отправляем
    try:
        _send_email_with_log(
            smtp_server=smtp_server,  # type: ignore
            smtp_port=smtp_port,  # type: ignore
            sender=sender,  # type: ignore
            recipients=recipients,
            subject='Ошибки при работе сервиса Automation_Reports',
            html_body=html_body,
            log_file_path='log/update_autoreports_data.log',
            password=password
        )
        LOGGER_ERROR_TYPE['info'](
            f'Email с ошибками отправлен: {recipients}'
        )
    except Exception as e:
        LOGGER_ERROR_TYPE['warning'](
            f'Не удалось отправить email: {e}'
        )
