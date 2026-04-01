from __future__ import annotations

import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path

from domain.ports.interfaces import EmailSender, ErrorNotifier


def create_logger(
    log_file: str = 'log/update_autoreports_data.log',
    level: int = logging.INFO,
    encoding: str = 'windows-1251',
) -> logging.Logger:
    '''Фабрика логгера. Настройка — деталь infrastructure.'''
    fmt = '%(asctime)s, %(levelname)s, %(message)s, %(funcName)s'
    logger = logging.getLogger('task_logger')
    logger.setLevel(level)

    if not logger.handlers:
        fh = RotatingFileHandler(
            log_file, mode='a', delay=True,
            maxBytes=4 * 1024 * 1024, backupCount=5,
            encoding=encoding,
        )
        fh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(fmt))
        logger.addHandler(ch)

    return logger


class LoggingErrorNotifier(ErrorNotifier):
    '''
    Реализация ErrorNotifier через logging.
    Отслеживает, были ли error/critical.
    '''

    def __init__(self, logger: logging.Logger | None = None):
        self._logger = logger or create_logger()
        self._error_types: set[str] = set()

    def notify(
        self, message: str, severity: str = 'error',
    ) -> None:
        log_func = getattr(self._logger, severity, None)
        if log_func is None:
            self._logger.warning(
                f'Некорректный уровень: {severity}. '
                f'Сообщение: {message}',
                stacklevel=3,
            )
            return
        log_func(message, stacklevel=3)
        if severity in ('error', 'critical'):
            self._error_types.add(severity)

    def has_critical_errors(self) -> bool:
        return bool(
            self._error_types & {'error', 'critical'}
        )

    @property
    def error_types(self) -> set[str]:
        return self._error_types


class SmtpEmailSender(EmailSender):
    '''
    Реализация EmailSender через SMTP.
    SRP: отвечает только за отправку email.
    '''

    def __init__(
        self,
        error_notifier: LoggingErrorNotifier,
        smtp_server: str = '',
        smtp_port: int = 25,
        sender: str = '',
        password: str | None = None,
        recipients: list[str] | None = None,
        log_file_path: str = 'log/update_autoreports_data.log',
        html_template_path: str = 'error_notification.html',
    ):
        self._notifier = error_notifier
        self._smtp_server = smtp_server
        self._smtp_port = smtp_port
        self._sender = sender
        self._password = password
        self._recipients = recipients or []
        self._log_file_path = log_file_path
        self._html_template_path = html_template_path

    def send_error_report(self) -> None:
        if not self._notifier.has_critical_errors():
            return

        if not all([self._smtp_server, self._sender, self._recipients]):
            self._notifier.notify(
                'SMTP не настроен, email не отправлен',
                'warning',
            )
            return

        error_types_str = ', '.join(
            sorted(self._notifier.error_types)
        )
        html_body = self._build_html(error_types_str)

        try:
            self._send(
                subject=(
                    'Ошибки при работе сервиса '
                    'Automation_Reports'
                ),
                html_body=html_body,
            )
            self._notifier.notify(
                f'Email отправлен: {self._recipients}', 'info'
            )
        except Exception as e:
            self._notifier.notify(
                f'Не удалось отправить email: {e}', 'warning'
            )

    def _build_html(self, error_types_str: str) -> str:
        from datetime import datetime

        template = Path(self._html_template_path)
        if template.exists():
            html = template.read_text(encoding='utf-8')
            return html.format(
                timestamp=datetime.now(),
                error_types=error_types_str,
            )
        return (
            '<html><body>'
            '<h2>Ошибка в Automation_Reports</h2>'
            f'<p>Ошибки: {error_types_str}</p>'
            '</body></html>'
        )

    def _send(self, subject: str, html_body: str) -> None:
        msg = MIMEMultipart()
        msg['From'] = self._sender
        msg['To'] = ';'.join(self._recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        log_path = Path(self._log_file_path)
        if log_path.exists() and log_path.stat().st_size > 0:
            with open(log_path, 'rb') as f:
                att = MIMEBase('application', 'octet-stream')
                att.set_payload(f.read())
            encoders.encode_base64(att)
            att.add_header(
                'Content-Disposition',
                f'attachment; filename={log_path.name}',
            )
            msg.attach(att)

        server = smtplib.SMTP(self._smtp_server, self._smtp_port)
        if self._password:
            server.login(self._sender, self._password)
        server.send_message(msg)
        server.quit()
