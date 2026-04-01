from __future__ import annotations

from datetime import datetime
from re import match

from domain.entities.models import TriggerEntity, TriggerType


class TriggerValidator:
    '''
    Валидатор параметров триггеров.
    SRP: единственная причина изменений — правила валидации расписания.
    '''

    VALID_TRIGGER_TYPES = {t.value for t in TriggerType}
    DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}$'
    TIME_PATTERN = r'^\d{2}:\d{2}$'
    VALID_MONTH_DAYS = {str(d) for d in range(1, 32)} | {'Last'}
    VALID_WEEK_DAYS = {
        'Понедельник', 'Вторник', 'Среда', 'Четверг',
        'Пятница', 'Суббота', 'Воскресенье',
    }
    VALID_MONTHS = {
        'Январь', 'Февраль', 'Март', 'Апрель',
        'Май', 'Июнь', 'Июль', 'Август',
        'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь',
        'Ежемесячно',
    }

    def validate_all(
        self, triggers: list[TriggerEntity],
    ) -> list[str]:
        '''Валидирует список триггеров. Пустой список = ОК.'''
        if not triggers:
            return ['Отсутствуют триггеры для задачи']
        errors: list[str] = []
        for idx, trigger in enumerate(triggers, start=1):
            errors.extend(self._validate_one(trigger, idx))
        return errors

    def _validate_one(
        self, t: TriggerEntity, n: int,
    ) -> list[str]:
        errors: list[str] = []
        self._check_type(t, n, errors)
        self._check_date(t, n, errors)
        self._check_time(t, n, errors)
        self._check_datetime_values(t, n, errors)

        if t.trigger_type == TriggerType.ONE_TIME:
            self._check_end_date(t, n, errors)

        if t.trigger_type == TriggerType.MONTHLY:
            self._check_days_of_month(t, n, errors)
            self._check_months_of_year(t, n, errors)

        if t.trigger_type == TriggerType.WEEKLY:
            self._check_days_of_week(t, n, errors)

        return errors

    def _check_type(self, t, n, errors):
        if t.trigger_type.value not in self.VALID_TRIGGER_TYPES:
            errors.append(
                f'Триггер {n}: Неверный тип: '
                f'{t.trigger_type.value}'
            )

    def _check_date(self, t, n, errors):
        if not match(self.DATE_PATTERN, t.start_date):
            errors.append(
                f'Триггер {n}: Неверный формат даты: '
                f'{t.start_date}. Ожидается ГГГГ-ММ-ДД.'
            )

    def _check_time(self, t, n, errors):
        if not match(self.TIME_PATTERN, t.start_time):
            errors.append(
                f'Триггер {n}: Неверный формат времени: '
                f'{t.start_time}. Ожидается ЧЧ:ММ.'
            )

    def _check_datetime_values(self, t, n, errors):
        try:
            datetime.strptime(t.start_date, '%Y-%m-%d')
            datetime.strptime(t.start_time, '%H:%M')
        except ValueError:
            errors.append(
                f'Триггер {n}: Некорректная дата/время: '
                f'{t.start_date} {t.start_time}'
            )

    def _check_end_date(self, t, n, errors):
        '''Проверка end_date для one_time триггера.'''
        if not t.end_date:
            return
        if not match(self.DATE_PATTERN, t.end_date):
            errors.append(
                f'Триггер {n}: Неверный формат даты '
                f'окончания: {t.end_date}. '
                f'Ожидается ГГГГ-ММ-ДД.'
            )
            return
        try:
            start = datetime.strptime(t.start_date, '%Y-%m-%d')
            end = datetime.strptime(t.end_date, '%Y-%m-%d')
            if end < start:
                errors.append(
                    f'Триггер {n}: Дата окончания '
                    f'{t.end_date} раньше даты начала '
                    f'{t.start_date}.'
                )
        except ValueError:
            errors.append(
                f'Триггер {n}: Некорректная дата '
                f'окончания: {t.end_date}.'
            )

    def _check_days_of_month(self, t, n, errors):
        if not t.days_of_month and not t.run_on_last_day_of_month:
            errors.append(
                f'Триггер {n}: Пустой список дней месяца.'
            )
            return
        for day in t.days_of_month:
            if str(day) not in self.VALID_MONTH_DAYS:
                errors.append(
                    f'Триггер {n}: Некорректный день месяца: '
                    f'{day}.'
                )

    def _check_days_of_week(self, t, n, errors):
        if not t.days_of_week:
            errors.append(
                f'Триггер {n}: Пустой список дней недели.'
            )
            return
        for day in t.days_of_week:
            if day not in self.VALID_WEEK_DAYS:
                errors.append(
                    f'Триггер {n}: Некорректный день недели: '
                    f'{day}.'
                )

    def _check_months_of_year(self, t, n, errors):
        if not t.months_of_year and not t.set_all_months:
            errors.append(
                f'Триггер {n}: Пустой список месяцев.'
            )
            return
        for month in t.months_of_year:
            if month not in self.VALID_MONTHS:
                errors.append(
                    f'Триггер {n}: Некорректный месяц: '
                    f'{month}.'
                )
