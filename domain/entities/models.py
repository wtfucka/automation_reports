from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class TriggerType(Enum):
    '''Типы триггеров планировщика задач.'''
    ONE_TIME = 'one_time'
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'


@dataclass
class TriggerEntity:
    '''
    Доменная сущность триггера расписания.
    Содержит только данные — не знает о COM, JSON, файлах.
    '''
    trigger_type: TriggerType
    start_date: str = '2026-01-01'
    start_time: str = '00:00'
    end_date: str = ''
    interval_week_days: int = 1
    days_of_week: list[str] = field(
        default_factory=lambda: ['Понедельник']
    )
    days_of_month: list[int] = field(default_factory=lambda: [1])
    months_of_year: list[str] = field(
        default_factory=lambda: ['Январь']
    )
    run_on_last_day_of_month: bool = False
    set_all_months: bool = False
    repetition_interval: str = ''
    enabled: bool = True


@dataclass
class ScheduledTaskEntity:
    '''
    Доменная сущность задачи планировщика (агрегат).
    Содержит список триггеров и метаданные задачи.
    '''
    task_name: str
    executable_path: str
    triggers: list[TriggerEntity]
    description: str = ''
    state: int = 1
    stop_if_runs_longer: str = '2H'


@dataclass
class TaskInfoEntity:
    '''
    Доменная сущность информации о задаче,
    прочитанной из планировщика.
    '''
    task_name: str
    last_run_time: datetime | None = None
    last_result: int = 0
    author: str = ''
    actions: list[str] = field(default_factory=list)
    description: str = ''
    state: int = 0
    run_as_user: str = ''
    triggers_info: list[dict[str, Any]] = field(
        default_factory=list
    )


@dataclass
class ReportMainData:
    '''
    Основные данные отчёта, полученные из внешних БД
    (Oracle / PostgreSQL).
    '''
    request_id: str
    customer_login: str = ''
    customer_name: str = ''
    customer_company: str = ''
    customer_orgstructure: str = ''
    receiver_login: str = ''
    receiver_name: str = ''
    receiver_company: str = ''
    receiver_orgstructure: str = ''
    report_create_date: datetime | None = None


@dataclass
class ReportCmdInfo:
    '''Информация, извлечённая из .cmd файлов отчёта.'''
    task_name: str
    emails: str = ''
    theme: str = ''
    sender_type: str = ''
    database_hostname: str = ''
    database_type: str = ''


@dataclass
class ReportLogInfo:
    '''Информация из лог-файла последней отправки.'''
    task_name: str
    last_send_date: datetime | None = None
    last_send_status: str = ''
    last_send_recipients: str = ''
    last_send_attachments: str = ''
    last_send_issue: str = ''
    last_send_error: str = ''
