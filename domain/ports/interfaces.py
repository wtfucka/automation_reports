"""
Domain Ports — абстрактные интерфейсы (контракты).

Это «розетки», к которым подключаются внешние адаптеры.
Domain определяет ЧТО нужно, а Infrastructure — КАК это сделать.

Dependency Rule: стрелки зависимостей идут ВНУТРЬ →
    Infrastructure реализует эти интерфейсы,
    Application использует эти интерфейсы,
    Domain определяет эти интерфейсы.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from domain.entities.models import (
    ReportCmdInfo,
    ReportLogInfo,
    ReportMainData,
    ScheduledTaskEntity,
    TaskInfoEntity,
)


# ───────────────────────────────────────────────────────────────────
# Error / Logging
# ───────────────────────────────────────────────────────────────────

class ErrorNotifier(ABC):
    """
    Порт уведомления об ошибках.
    Бизнес-логика вызывает notify(), не зная, куда пойдёт
    сообщение — в лог, email, Telegram.
    """

    @abstractmethod
    def notify(self, message: str, severity: str = 'error') -> None:
        ...

    @abstractmethod
    def has_critical_errors(self) -> bool:
        """Были ли error/critical во время работы."""
        ...


# ───────────────────────────────────────────────────────────────────
# Database — Read Ports
# ───────────────────────────────────────────────────────────────────

class ReportDataReader(ABC):
    """
    Порт чтения основных данных отчёта из внешних БД.
    CCP: все методы чтения данных отчёта — одна причина изменений
    (изменение схемы внешних БД).
    """

    @abstractmethod
    def get_main_data_batch(
        self, request_ids: list[str],
    ) -> dict[str, dict[str, str]]:
        """Возвращает {request_id: data_dict} из внешней БД."""
        ...


class ReportRepository(ABC):
    """
    Порт для работы с целевой БД (MSSQL) — хранилище отчётов.
    CCP: все CRUD операции хранилища — одна причина изменений.
    """

    @abstractmethod
    def exists_batch(
        self, request_ids: list[str],
    ) -> dict[str, bool]:
        """Проверяет существование нескольких request_id."""
        ...

    @abstractmethod
    def update_reports(
        self, data: list[dict[str, Any]],
    ) -> None:
        ...

    @abstractmethod
    def insert_reports(
        self, data: list[dict[str, Any]],
    ) -> None:
        ...

    @abstractmethod
    def initialize_schema(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


# ───────────────────────────────────────────────────────────────────
# Task Scheduler
# ───────────────────────────────────────────────────────────────────

class TaskSchedulerGateway(ABC):
    """
    Порт для взаимодействия с планировщиком задач.
    Application не знает, Win32 COM или cron за ним стоит.
    """

    @abstractmethod
    def register_task(self, task: ScheduledTaskEntity) -> bool:
        ...

    @abstractmethod
    def get_task_info(self, task_name: str) -> TaskInfoEntity | None:
        ...


# ───────────────────────────────────────────────────────────────────
# File System
# ───────────────────────────────────────────────────────────────────

class ScheduleFileReader(ABC):
    """Порт чтения параметров расписания из файлов."""

    @abstractmethod
    def read(
        self, path: str, request_id: str,
    ) -> ScheduledTaskEntity | None:
        ...


class CmdFileReader(ABC):
    """Порт чтения .cmd файлов для извлечения email/темы."""

    @abstractmethod
    def read(self, root_path: str) -> ReportCmdInfo:
        ...


class LogFileReader(ABC):
    """Порт чтения лог-файлов отправки."""

    @abstractmethod
    def read(self, root_path: str) -> ReportLogInfo:
        ...


class FolderScanner(ABC):
    """Порт для сканирования папок отчётов."""

    @abstractmethod
    def walk_existing(self) -> list[str]:
        """Папки, уже известные БД."""
        ...

    @abstractmethod
    def find_all(
        self, search_folder: str | None = None,
    ) -> list[str]:
        """Все папки (или конкретная)."""
        ...


# ───────────────────────────────────────────────────────────────────
# Email
# ───────────────────────────────────────────────────────────────────

class EmailSender(ABC):
    """Порт отправки email (ошибки, уведомления)."""

    @abstractmethod
    def send_error_report(self) -> None:
        ...


# ───────────────────────────────────────────────────────────────────
# Version Control
# ───────────────────────────────────────────────────────────────────

class VersionControlGateway(ABC):
    """Порт для git-операций (архивация отчётов)."""

    @abstractmethod
    def commit_and_push(
        self,
        folders: dict[str, list[str]],
    ) -> None:
        ...
