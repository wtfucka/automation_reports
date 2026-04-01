from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from domain.ports.interfaces import (
    CmdFileReader,
    EmailSender,
    ErrorNotifier,
    FolderScanner,
    LogFileReader,
    ReportDataReader,
    ReportRepository,
    ScheduleFileReader,
    TaskSchedulerGateway,
    VersionControlGateway,
)
from infrastructure.db.repositories import (
    MssqlReportRepository,
    OracleReportDataReader,
    PostgreReportDataReader,
)
from infrastructure.filesystem.readers import (
    CmdFileReaderImpl,
    FolderScannerImpl,
    JsonScheduleFileReader,
    LogFileReaderImpl,
)
from infrastructure.notification.notifier import (
    LoggingErrorNotifier,
    SmtpEmailSender,
)
from infrastructure.scheduler.win32_gateway import (
    Win32TaskSchedulerGateway,
)
from infrastructure.vcs.git_gateway import (
    GitVersionControlGateway,
)

from application.use_cases.update_data import UpdateDataUseCase
from application.use_cases.insert_data import InsertDataUseCase


def _load_configs(
    config_file: str = 'file_name.json',
) -> dict[str, Any]:
    '''Загружает JSON конфигурацию.'''
    try:
        with open(Path.cwd() / config_file) as f:
            return json.load(f)
    except (IOError, OSError, json.JSONDecodeError):
        return {}


class Container:
    '''
    Простой DI-контейнер.
    Создаёт и кеширует экземпляры всех зависимостей.
    '''

    def __init__(
        self,
        config_file: str = 'file_name.json',
        root_report_paths: list[str] | None = None,
        database_type_map: dict[str, str] | None = None,
        log_file_name: str = 'log.log',
    ):
        configs = _load_configs(config_file)
        self._root_paths = root_report_paths or []
        self._db_type_map = database_type_map or {}
        self._log_file_name = log_file_name

        self._notifier = LoggingErrorNotifier()

        self._mssql_repo = MssqlReportRepository(
            configs.get('MSSQL_DB_NAME', {}),
            self._notifier,
        )
        self._oracle_reader = OracleReportDataReader(
            configs.get('ORACLE_DB_NAME', {}),
            self._notifier,
        )
        self._postgre_reader = PostgreReportDataReader(
            configs.get('POSTGRE_DB_NAME', {}),
            self._notifier,
        )

        self._schedule_reader = JsonScheduleFileReader(
            self._notifier,
        )
        self._cmd_reader = CmdFileReaderImpl(
            self._notifier,
            database_type_map=self._db_type_map,
        )
        self._log_reader = LogFileReaderImpl(
            self._notifier,
            log_file_name=self._log_file_name,
        )
        self._folder_scanner = FolderScannerImpl(
            self._root_paths,
            self._mssql_repo,
            self._notifier,
        )

        self._scheduler = Win32TaskSchedulerGateway(
            self._notifier,
        )

        self._vcs = GitVersionControlGateway(self._notifier)

        mailer_cfg = configs.get('Mailer', {})
        self._email_sender = SmtpEmailSender(
            error_notifier=self._notifier,
            smtp_server=mailer_cfg.get('smtp_server', ''),
            smtp_port=mailer_cfg.get('smtp_port', 25),
            sender=mailer_cfg.get('sender', ''),
            password=mailer_cfg.get('password'),
            recipients=['fake@fake.ru'],
        )

    @property
    def error_notifier(self) -> ErrorNotifier:
        return self._notifier

    @property
    def report_repo(self) -> ReportRepository:
        return self._mssql_repo

    @property
    def oracle_reader(self) -> ReportDataReader:
        return self._oracle_reader

    @property
    def postgre_reader(self) -> ReportDataReader:
        return self._postgre_reader

    @property
    def scheduler_gateway(self) -> TaskSchedulerGateway:
        return self._scheduler

    @property
    def schedule_file_reader(self) -> ScheduleFileReader:
        return self._schedule_reader

    @property
    def cmd_reader(self) -> CmdFileReader:
        return self._cmd_reader

    @property
    def log_reader(self) -> LogFileReader:
        return self._log_reader

    @property
    def folder_scanner(self) -> FolderScanner:
        return self._folder_scanner

    @property
    def vcs_gateway(self) -> VersionControlGateway:
        return self._vcs

    @property
    def email_sender(self) -> EmailSender:
        return self._email_sender

    def update_data_use_case(self) -> UpdateDataUseCase:
        return UpdateDataUseCase(
            folder_scanner=self._folder_scanner,
            oracle_reader=self._oracle_reader,
            postgre_reader=self._postgre_reader,
            report_repo=self._mssql_repo,
            scheduler_gateway=self._scheduler,
            schedule_file_reader=self._schedule_reader,
            cmd_reader=self._cmd_reader,
            log_reader=self._log_reader,
            vcs_gateway=self._vcs,
            error_notifier=self._notifier,
        )

    def insert_data_use_case(self) -> InsertDataUseCase:
        return InsertDataUseCase(
            folder_scanner=self._folder_scanner,
            oracle_reader=self._oracle_reader,
            postgre_reader=self._postgre_reader,
            report_repo=self._mssql_repo,
            scheduler_gateway=self._scheduler,
            schedule_file_reader=self._schedule_reader,
            cmd_reader=self._cmd_reader,
            error_notifier=self._notifier,
        )

    def close(self) -> None:
        '''Закрывает все соединения.'''
        self._mssql_repo.close()
        self._oracle_reader.close()
        self._postgre_reader.close()
