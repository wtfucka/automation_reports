"""
Domain Value Objects — чистые утилиты без побочных эффектов.

Конвертеры дат, битовых масок и форматировщики.
Зависят ТОЛЬКО от стандартной библиотеки.
Вынесены из старого utils.py, который был «мусорным ящиком»
для всего подряд (SRP-нарушение).

CCP: все преобразования дат/масок меняются по одной причине —
если изменится формат расписания Task Scheduler.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum


class Weekday(Enum):
    """Дни недели (нумерация Task Scheduler)."""
    Понедельник = 2
    Вторник = 3
    Среда = 4
    Четверг = 5
    Пятница = 6
    Суббота = 7
    Воскресенье = 1


class Month(Enum):
    """Месяцы года."""
    Январь = 1
    Февраль = 2
    Март = 3
    Апрель = 4
    Май = 5
    Июнь = 6
    Июль = 7
    Август = 8
    Сентябрь = 9
    Октябрь = 10
    Ноябрь = 11
    Декабрь = 12


# ───────────────────────────────────────────────────────────────────
# Bitmask Converters
# ───────────────────────────────────────────────────────────────────

class BitmaskConverter:
    """Конвертер между списками и битовыми масками COM API."""

    _DOW_TO_BIT = {
        1: 0x01, 2: 0x02, 3: 0x04, 4: 0x08,
        5: 0x10, 6: 0x20, 7: 0x40,
    }
    _BIT_TO_DOW = {v: k for k, v in _DOW_TO_BIT.items()}

    @staticmethod
    def days_of_week_to_bitmask(days: list[int]) -> int:
        return sum(
            BitmaskConverter._DOW_TO_BIT[d]
            for d in days
            if d in BitmaskConverter._DOW_TO_BIT
        )

    @staticmethod
    def bitmask_to_days_of_week(bitmask: int) -> list[int]:
        if not bitmask:
            return []
        return [
            day for flag, day
            in BitmaskConverter._BIT_TO_DOW.items()
            if bitmask & flag
        ]

    @staticmethod
    def days_of_month_to_bitmask(days: list[int]) -> int:
        return sum(2 ** (d - 1) for d in days) if days else 0

    @staticmethod
    def bitmask_to_days_of_month(bitmask: int) -> list[int]:
        if not bitmask:
            return []
        return [d for d in range(1, 32) if bitmask & (1 << (d - 1))]

    @staticmethod
    def months_of_year_to_bitmask(months: list[int]) -> int:
        return sum(2 ** (m - 1) for m in months) if months else 0

    @staticmethod
    def bitmask_to_months_of_year(bitmask: int) -> list[int]:
        if not bitmask:
            return []
        return [m for m in range(1, 13) if bitmask & (1 << (m - 1))]


# ───────────────────────────────────────────────────────────────────
# Date Converters
# ───────────────────────────────────────────────────────────────────

class DateConverter:
    """Конвертер имён дней/месяцев ↔ числовых значений."""

    @staticmethod
    def get_day_name(day_nums: list[int]) -> list[str]:
        return [Weekday(n).name for n in day_nums] if day_nums else []

    @staticmethod
    def get_day_num(day_names: list[str]) -> list[int]:
        return [Weekday[n].value for n in day_names]

    @staticmethod
    def get_month_name(month_nums: list[int]) -> list[str]:
        return [Month(n).name for n in month_nums] if month_nums else []

    @staticmethod
    def get_month_num(month_names: list[str]) -> list[int]:
        return [Month[n].value for n in month_names]


# ───────────────────────────────────────────────────────────────────
# Date Formatters
# ───────────────────────────────────────────────────────────────────

class DateFormatter:
    """Форматирование дат для Task Scheduler и отображения."""

    @staticmethod
    def format_start_boundary(
        start_time: str,
        start_date: str,
        delta_hours: int = -3,
    ) -> str:
        """
        ISO 8601 строка для StartBoundary триггера.

        :param start_time: 'HH:MM'
        :param start_date: 'YYYY-MM-DD'
        :param delta_hours: Смещение часов (по умолчанию -3, MSK→UTC).
        """
        dt = datetime.strptime(start_time, '%H:%M')
        dt += timedelta(hours=delta_hours)
        return f'{start_date}T{dt.strftime("%H:%M:%S")}Z'

    @staticmethod
    def format_datetime(
        date_obj,
        fmt: str = '%Y-%m-%d %H:%M:%S',
        delta_hours: int = 0,
    ) -> datetime | None:
        """
        Универсальный парсер дат (str, datetime, pywintypes).
        Импорт pywintypes — ленивый, чтобы domain оставался чистым
        при использовании без Windows.
        """
        dt: datetime | None = None

        if isinstance(date_obj, datetime):
            dt = date_obj
        elif isinstance(date_obj, str):
            if 'T' in date_obj and '+' in date_obj:
                dt = datetime.fromisoformat(date_obj).replace(
                    tzinfo=None
                )
            elif 'T' in date_obj:
                dt = datetime.strptime(
                    date_obj, '%Y-%m-%dT%H:%M:%S'
                )
            else:
                dt = datetime.strptime(date_obj, fmt)
        else:
            # Ленивый импорт: pywintypes может отсутствовать
            try:
                import pywintypes
                if isinstance(date_obj, pywintypes.TimeType):
                    dt = datetime.fromtimestamp(
                        date_obj.timestamp()
                    )
            except ImportError:
                pass

        return dt + timedelta(hours=delta_hours) if dt else None

    @staticmethod
    def parse_log_datetime(datetime_str: str) -> datetime:
        """Парсит строку даты из лог-файлов (12h/24h)."""
        if datetime_str.endswith(('AM', 'PM')):
            return datetime.strptime(
                datetime_str, '%d.%m.%Y %I:%M:%S %p'
            )
        return datetime.strptime(
            datetime_str, '%d.%m.%Y %H:%M:%S'
        )
