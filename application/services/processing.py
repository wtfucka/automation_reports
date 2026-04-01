from __future__ import annotations

from typing import Any

from domain.entities.models import TaskInfoEntity
from domain.value_objects.converters import DateFormatter


class TaskDataProcessingService:
    '''
    Сервис обработки «сырых» данных Task Scheduler
    в формат, пригодный для записи в БД.

    SRP: единственная причина изменений — изменение маппинга
    полей Task Scheduler → столбцы БД.
    '''

    FIELD_MAP = {
        'TaskName': 'task_name',
        'Last Run Time': 'task_last_run_date',
        'Last Result': 'task_last_run_result',
        'Author': 'task_author_login',
        'Task To Run': 'task_to_run',
        'Comment': 'task_description',
        'Scheduled Task State': 'task_status',
        'Run As User': 'task_run_as_user',
        'Trigger State': 'task_trigger_status',
        'Schedule Type': 'task_schedule_type',
        'StartBoundary': 'task_schedule_start_date',
        'DaysInterval': 'task_schedule_days_interval',
        'WeeksInterval': 'task_schedule_weeks_interval',
        'DaysOfWeek': 'task_schedule_week_days',
        'MonthsOfYear': 'task_schedule_months',
        'DaysOfMonth': 'task_schedule_month_days',
        'Repeat_every': 'task_schedule_repeat_every',
        'Repeat_until_time': 'task_schedule_repeat_until_time',
        'Repeat_until_duration': 'task_schedule_repeat_until_duration',
    }

    TASK_STATE_MAP = {
        0: 'Unknown',
        1: 'Отключена',
        2: 'В очереди',
        3: 'Включена',
        4: 'Выполняется',
    }

    TASK_RESULT_MAP = {
        0: 'Success',
        1: 'Incorrect function',
        267008: 'Ready',
        267009: 'Running',
        267010: 'Disabled',
        267011: 'Has not run',
        267012: 'No more runs',
        267013: 'Not scheduled',
        267014: 'Terminated',
        267015: 'No valid triggers',
        2147750665: 'Already running',
        2147942402: 'File not found',
    }

    def process_task_list(
        self,
        task_list: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        '''
        Обрабатывает список «сырых» словарей задач.
        Возвращает список словарей, готовых для БД.
        '''
        return [self._process_one(task) for task in task_list]

    def _process_one(
        self, task: dict[str, Any],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}

        for key, value in task.items():
            if key == 'Schedule Info':
                result.update(
                    self._process_triggers_info(value)
                )
            elif key == 'Last Result':
                result[self.FIELD_MAP[key]] = self._safe_lookup(
                    value, self.TASK_RESULT_MAP, 'Unknown result'
                )
            elif key == 'Scheduled Task State':
                result[self.FIELD_MAP[key]] = self._safe_lookup(
                    value, self.TASK_STATE_MAP, 'Unknown state'
                )
            elif key == 'Last Run Time':
                result[self.FIELD_MAP[key]] = (
                    DateFormatter.format_datetime(
                        value, delta_hours=-3
                    )
                )
            elif key == 'Task To Run':
                result.update(self._process_paths(value))
            else:
                mapped = self.FIELD_MAP.get(key, key)
                result[mapped] = value

        return result

    def _process_triggers_info(
        self, schedule_info: list[dict[str, str]],
    ) -> dict[str, str | None]:
        '''Форматирует триггеры в плоский словарь для БД.'''
        keys = [
            'task_schedule_type',
            'task_trigger_status',
            'task_schedule_start_date',
            'task_schedule_days_interval',
            'task_schedule_weeks_interval',
            'task_schedule_week_days',
            'task_schedule_months',
            'task_schedule_month_days',
            'task_schedule_repeat_every',
            'task_schedule_repeat_until_time',
            'task_schedule_repeat_until_duration',
        ]
        data: dict[str, list[str]] = {k: [] for k in keys}
        add_prefix = len(schedule_info) > 1

        for idx, trigger in enumerate(schedule_info):
            prefix = f'Trigger {idx + 1}: ' if add_prefix else ''
            for trig_key, trig_val in trigger.items():
                field_key = self.FIELD_MAP.get(trig_key)
                if not field_key or field_key not in data:
                    continue
                if trig_key == 'StartBoundary':
                    trig_val = DateFormatter.format_datetime(trig_val)
                elif (
                    trig_key in ('DaysOfWeek', 'DaysOfMonth', 'MonthsOfYear')
                    and trig_val
                ):
                    trig_val = ', '.join(map(str, trig_val))
                data[field_key].append(f'{prefix}{trig_val}')

        result: dict[str, str | None] = {}
        for k, v in data.items():
            joined = ', '.join(
                str(x) for x in v if x is not None
            )
            result[k] = joined if joined else None
        return result

    @staticmethod
    def _process_paths(
        path_list: list[str],
    ) -> dict[str, str]:
        from pathlib import Path

        paths: set[str] = set()
        names: set[str] = set()
        for p in path_list:
            obj = Path(p)
            paths.add(str(obj.parent))
            names.add(obj.name)
        result: dict[str, str] = {}
        if paths:
            result['task_file_path'] = ', '.join(paths)
        if names:
            result['task_file_name'] = ', '.join(names)
        return result

    @staticmethod
    def _safe_lookup(
        value, mapping: dict[int, str], default: str,
    ) -> str:
        try:
            code = (
                int(value)
                if isinstance(value, (int, str))
                and str(value).isdigit()
                else 0
            )
            return mapping.get(code, default)
        except (ValueError, TypeError):
            return default


class DataMergerService:
    '''
    Сервис слияния данных из разных источников в единый список.
    SRP: единственная ответственность — объединение по task_name.
    '''

    @staticmethod
    def merge(
        base: list[dict[str, Any]],
        extra: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        '''
        Объединяет extra в base по ключу 'task_name'.
        '''
        for base_item in base:
            matching = next(
                (
                    e for e in extra
                    if e.get('task_name') == base_item.get('task_name')
                ),
                None,
            )
            if matching:
                base_item.update(
                    {k: v for k, v in matching.items()
                     if k != 'task_name'}
                )
        return base


class MainDataProcessingService:
    '''
    Обработка «сырых» данных из Oracle/PostgreSQL.
    SRP: нормализация полей отчёта.
    '''

    @staticmethod
    def process(
        data: list[dict[str, str]],
    ) -> list[dict[str, Any]]:
        import re

        if not data:
            return []

        result = []
        for row in data:
            out = dict(row)
            if 'request_id' in out:
                out['task_name'] = out['request_id']
                for field in (
                    'customer_orgstructure',
                    'receiver_orgstructure',
                ):
                    val = out.get(field)
                    if isinstance(val, str):
                        out[field] = (
                            re.sub(r'/{2,}', '/', val).rstrip('/')
                        )
            result.append(out)
        return result
