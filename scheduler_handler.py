from enum import Enum
from typing import Any

from pywintypes import com_error

import win32com.client

from error_handler import handle_error
from utils import BitmaskConverter, DateConverter, DateFormatter


class TriggerConfig(Enum):
    one_time = 1
    daily = 2
    weekly = 3
    monthly = 4

    def create_trigger(self, task_definition):
        '''Создание триггера определенного типа.'''
        return task_definition.Triggers.Create(self.value)

    def configure_trigger(self, trigger, **kwargs):
        '''Настройка триггера.'''
        method_dispatch = {
            TriggerConfig.daily: self._configure_daily,
            TriggerConfig.weekly: self._configure_weekly,
            TriggerConfig.monthly: self._configure_monthly
        }
        configure_method = method_dispatch.get(self)
        if configure_method:
            self._apply_common_settings(trigger, kwargs.get('start_datetime'))
            specific_kwargs = {
                key: kwargs[key] for key in configure_method.__code__.co_varnames if key in kwargs  # noqa
            }
            configure_method(trigger, **specific_kwargs)

    def _configure_daily(self, trigger, days_interval):
        trigger.DaysInterval = days_interval

    def _configure_weekly(self, trigger, weeks_interval, days_of_week):
        trigger.WeeksInterval = weeks_interval
        trigger.DaysOfWeek = BitmaskConverter.days_of_week_to_bitmask(
            days_of_week
            )

    def _configure_monthly(self, trigger, days_of_month, months_of_year):
        trigger.DaysOfMonth = BitmaskConverter.days_of_month_to_bitmask(
            days_of_month
        )
        trigger.MonthsOfYear = BitmaskConverter.months_of_year_to_bitmask(
            months_of_year
            )

    def _apply_common_settings(self, trigger, start_datetime):
        trigger.StartBoundary = start_datetime
        trigger.Enabled = True


class TaskSchedulerManager:
    '''
    Менеджер по созданию и настройке задачи в Task Scheduler.
    UPDATED: Supports both single and multiple triggers.
    '''
    # Путь к папке задач в Task Scheduler
    TASK_FOLDER = '\\Autoreports'

    # Маппинг русских ключей JSON в английские названия полей
    TRIGGER_FIELD_MAPPING = {
        'Тип расписания': 'trigger_type',
        'Дата начала': 'start_date',
        'Время начала': 'start_time',
        'Частота отправки': 'interval_week_days',
        'Дни недели': 'days_of_week',
        'Дни месяца': 'days_of_month',
        'Месяцы': 'months_of_year',
        'Повторение': 'repetition_interval',
        'Включен': 'enabled',
    }

    # Значения по умолчанию для полей триггера
    TRIGGER_DEFAULTS = {
        'trigger_type': None,
        'start_date': None,
        'start_time': None,
        'interval_week_days': 1,
        'days_of_week': ['Понедельник',],
        'days_of_month': [1,],
        'months_of_year': ['Январь',],
        'run_on_last_day_of_month': False,
        'set_all_months': False,
        'repetition_interval': '',
        'enabled': True,
    }

    def __init__(self):
        self.scheduler = win32com.client.Dispatch('Schedule.Service')
        self.scheduler.Connect()

    def _get_or_create_folder(self) -> Any:
        '''
        Получает папку задач, создавая её при необходимости.

        :return: Объект папки Task Scheduler.
        :raises: com_error если не удалось получить или создать папку.
        '''
        try:
            return self.scheduler.GetFolder(self.TASK_FOLDER)
        except com_error:
            # Папка не существует, создаём её
            try:
                root_folder = self.scheduler.GetFolder('\\')
                folder_name = self.TASK_FOLDER.lstrip('\\')
                folder = root_folder.CreateFolder(folder_name)
                return folder
            except com_error as e:
                handle_error(
                    f'Не удалось создать папку {self.TASK_FOLDER}: {e}',
                    'error'
                )
                raise

    def create_or_update_task(
            self,
            task_name: str,
            executable_path: str,
            triggers: list[dict[str, Any]],
            task_description: str = '',
            task_state: int = 1,
            stop_if_runs_longer: str = '2H'
            ):
        '''
        Создаёт или обновляет задачу в планировщике Windows Task Scheduler.
        UPDATED: Accepts a list of trigger configurations.

        :param task_name: Имя задачи.
        :param executable_path: Путь к исполняемому файлу
        или скрипту (.cmd, .exe и т.д.).
        :param triggers: Список конфигураций триггеров. Каждый триггер -
        словарь с параметрами.
        :param task_description: Описание для задачи в Scheduler.
        :param task_state: Статус задачи (вкл/выкл) - 1/0.
        :param stop_if_runs_longer: Время в формате '2H', после
        которого задача будет остановлена.
            '''
        folder = self._get_or_create_folder()

        # Создание или обновление задачи
        task_definition = self.scheduler.NewTask(0)

        # Настройка регистрационной информации
        task_definition.RegistrationInfo.Description = task_description
        task_definition.RegistrationInfo.Author = 'automation_reports'

        # Настройка триггеров (поддержка множественных триггеров)
        successful_triggers = 0
        for trigger_config in triggers:
            try:
                self._create_single_trigger(task_definition, trigger_config)
                successful_triggers += 1
            except (com_error, ValueError, KeyError, AttributeError) as e:
                handle_error(
                    f'Ошибка создания триггера для задачи {task_name}: {e}',
                    'warning'
                )
                continue

        # Проверка: должен быть создан хотя бы один триггер
        if successful_triggers == 0:
            handle_error(
                f'Не удалось создать ни одного триггера для задачи {task_name}',  # noqa
                'error'
            )
            return False        # Настройка действия
        action = task_definition.Actions.Create(0)
        action.Path = executable_path

        # Настройка общих параметров
        task_definition.Settings.Enabled = task_state
        task_definition.Settings.StartWhenAvailable = True
        task_definition.Settings.Hidden = False

        # Установка параметров остановки задачи
        task_definition.Settings.ExecutionTimeLimit = f'PT{stop_if_runs_longer}'  # noqa

        # Настройка параметров учетной записи
        # S4U (Do not store password)
        task_definition.Principal.LogonType = 3
        # Run with highest privileges
        task_definition.Principal.RunLevel = 1

        # Настройка совместимости
        task_definition.Settings.Compatibility = 6  # Windows Server 2019

        # Регистрация задачи (создание или обновление)
        try:
            folder.RegisterTaskDefinition(
                task_name,
                task_definition,
                6,  # TASK_CREATE_OR_UPDATE
                None,  # Без пароля
                None,
                2,  # Run whether user is logged on or not
                None
            )
        except com_error as e:
            handle_error(f'Не удалось создать задачу {task_name}: {e}',
                         'warning')
            return False

        return True

    def _create_single_trigger(
            self,
            task_definition,
            trigger_config: dict[str, Any]
            ) -> None:
        '''
        Создает один триггер на основе конфигурации.

        :param task_definition: Определение задачи Task Scheduler.
        :param trigger_config: Словарь с параметрами триггера.
        '''
        trigger_type = trigger_config.get('trigger_type')
        if not trigger_type or not isinstance(trigger_type, str):
            handle_error('Отсутствует или неверный тип триггера', 'warning')
            return

        trigger_conf = TriggerConfig.__members__.get(trigger_type)

        if not trigger_conf:
            handle_error(f'Неверный тип триггера {trigger_type}', 'warning')
            return

        trigger = trigger_conf.create_trigger(task_definition)

        # Формирование времени начала
        start_boundary = DateFormatter.format_start_time_and_date(
            start_time=trigger_config.get('start_time', '00:00'),
            start_date=trigger_config.get('start_date', '2025-01-01')
        )

        # Преобразование дней недели и месяцев в числовые значения
        days_of_week = trigger_config.get('days_of_week', [])
        months_of_year = trigger_config.get('months_of_year', [])

        days_of_week_int = DateConverter.get_day_num(days_of_week) if days_of_week else []  # noqa
        months_of_year_int = DateConverter.get_month_num(months_of_year) if months_of_year else []  # noqa

        # Настройка триггера
        trigger_conf.configure_trigger(
            trigger,
            start_datetime=start_boundary,
            days_interval=trigger_config.get('interval_week_days', 1),
            weeks_interval=trigger_config.get('interval_week_days', 1),
            days_of_week=days_of_week_int,
            days_of_month=trigger_config.get('days_of_month', []),
            months_of_year=months_of_year_int
        )

        # Специальные настройки для monthly триггера
        if trigger_type == 'monthly':
            if trigger_config.get('run_on_last_day_of_month'):
                trigger.RunOnLastDayOfMonth = True
            if trigger_config.get('set_all_months'):
                trigger.MonthsOfYear = 0xFFF

        # Настройка повторения
        repetition_interval = trigger_config.get('repetition_interval')
        if repetition_interval:
            trigger.Repetition.Interval = repetition_interval
            trigger.Repetition.Duration = 'P1D'
            trigger.Repetition.StopAtDurationEnd = False

        # Включение/выключение триггера
        trigger.Enabled = trigger_config.get('enabled', True)

    def get_task_info(self, task_name: str) -> dict[str, Any]:
        '''
        Читает информацию о задаче из планировщика Windows Task Scheduler.

        :param task_name: Имя задачи.
        :return: Список строк, имитирующий вывод команды schtasks.
        '''
        folder = self._get_or_create_folder()
        try:
            task = folder.GetTask(task_name)
            definition = task.Definition

            triggers = []
            for trigger in definition.Triggers:
                days_of_week_bitmask = getattr(trigger, 'DaysOfWeek', 0)
                days_of_week = DateConverter.get_day_name(
                    BitmaskConverter.bitmask_to_days_of_week(
                        days_of_week_bitmask if days_of_week_bitmask else 0
                        )
                    )
                days_of_month_bitmask = getattr(trigger, 'DaysOfMonth', 0)
                days_of_months = BitmaskConverter.bitmask_to_days_of_month(
                    days_of_month_bitmask if days_of_month_bitmask else 0
                )
                months_of_year_bitmask = getattr(trigger, 'MonthsOfYear', 0)
                months_of_year = DateConverter.get_month_name(
                    BitmaskConverter.bitmask_to_months_of_year(
                        months_of_year_bitmask if months_of_year_bitmask else 0  # noqa
                        )
                    )
                triggers.append(
                    {
                        'Schedule Type': TriggerConfig(trigger.Type).name if getattr(trigger, 'Type', None) else None,  # noqa
                        'StartBoundary': getattr(trigger, 'StartBoundary', None),  # noqa
                        'Trigger State': getattr(trigger, 'Enabled', None),
                        'DaysInterval': getattr(trigger, 'DaysInterval', None),
                        'WeeksInterval': getattr(trigger, 'WeeksInterval', None),  # noqa
                        'DaysOfWeek': days_of_week if days_of_week else None,
                        'DaysOfMonth': days_of_months if days_of_months else None,  # noqa
                        'MonthsOfYear': months_of_year if months_of_year else None,  # noqa
                        'Repeat_every': getattr(
                            trigger.Repetition,
                            'Interval',
                            None),
                        'Repeat_until_duration': getattr(
                            trigger.Repetition,
                            'Duration',
                            None),
                        'Repeat_until_time': getattr(
                            trigger.Repetition,
                            'StopAtDurationEnd',
                            None)
                    }
                )

            actions = [action.Path for action in definition.Actions]  # noqa

            return {
                'TaskName': task.Name,
                'Last Run Time': task.LastRunTime,  # noqa
                'Last Result': task.LastTaskResult,
                'Author': definition.RegistrationInfo.Author,
                'Task To Run': actions,
                'Comment': definition.RegistrationInfo.Description,
                'Scheduled Task State': task.State,
                'Run As User': definition.Principal.UserId,
                'Schedule Info': triggers
            }
        except (com_error, Exception) as e:
            handle_error(
                f'Ошибка чтения задачи в Task Scheduler {task_name}: {e}',
                'warning'
                )
            return {}

    def parse_schedule_params(
            self,
            path: str,
            request_id: str
            ) -> dict[str, Any] | bool:
        '''
        Метод для парсинга параметров из json для Task Scheduler.
        UPDATED: Supports both single and multiple trigger formats.

        :param path: Путь до отчета.
        :param request_id: Имя задачи для Task Scheduler.
        :return: Словарь с полученными параметрами или False.
        '''
        import json
        from pathlib import Path

        from utils import check_encoding

        root_path: Path = Path(path)
        schedule_file_path = root_path.joinpath(f'{request_id}.json')
        executable_path = root_path.joinpath(f'{request_id}.cmd')

        # Проверка существования файла параметров расписания
        if not schedule_file_path.is_file():
            handle_error(f'Файл расписания не найден: {schedule_file_path}',
                         'warning')
            return False

        # Проверка существования исполняемого файла
        if not executable_path.is_file():
            handle_error(f'Исполняемый файл не найден: {executable_path}',
                         'warning')
            return False

        try:
            encoding = check_encoding(schedule_file_path)
            with open(schedule_file_path, 'r', encoding=encoding) as f:
                content: dict = json.load(f)
        except (IOError,
                OSError,
                UnicodeDecodeError,
                json.JSONDecodeError) as error:
            handle_error(f'Ошибка чтения {schedule_file_path}: {error}',
                         'warning')
            return False

        # Базовые параметры задачи
        params: dict[str, Any] = {
            'task_name': schedule_file_path.parent.name,
            'executable_path': str(executable_path),
        }

        # Определяем формат: старый (одиночный) или новый (множественный)
        if 'Триггеры' in content:
            # НОВЫЙ ФОРМАТ: множественные триггеры
            triggers_data = content['Триггеры']
            if not triggers_data or not isinstance(triggers_data, list):
                handle_error(
                    f'Некорректный формат триггеров в {schedule_file_path}',
                    'error'
                )
                return False
            params['triggers'] = self._parse_multiple_triggers(triggers_data)
            params['task_description'] = content.get('Описание задачи', '')
            params['task_state'] = content.get('Статус расписания', 1)
            params['stop_if_runs_longer'] = content.get('Остановить, если дольше', '2H')  # noqa
        else:
            # СТАРЫЙ ФОРМАТ: одиночный триггер (обратная совместимость)
            single_trigger = self._parse_single_trigger_legacy(content)
            params['triggers'] = [single_trigger]
            params['task_description'] = content.get('Описание задачи', '')
            params['task_state'] = content.get('Статус расписания', 1)
            params['stop_if_runs_longer'] = content.get('Остановить, если дольше', '2H')  # noqa

        return params

    def _parse_single_trigger_legacy(
            self,
            content: dict
            ) -> dict[str, Any]:
        '''
        Парсит одиночный триггер из старого формата JSON.
        Обеспечивает обратную совместимость.

        :param content: Содержимое JSON файла (старый формат).
        :return: Словарь с параметрами триггера.
        '''
        # Автоматический маппинг через словарь
        trigger: dict[str, Any] = {}
        for ru_key, en_key in self.TRIGGER_FIELD_MAPPING.items():
            value = content.get(ru_key)
            if value is not None:
                trigger[en_key] = value

        # Установка значений по умолчанию
        for key, default_value in self.TRIGGER_DEFAULTS.items():
            trigger.setdefault(key, default_value)

        # Обработка "Last" в днях месяца
        days_of_month = trigger.get('days_of_month', [])
        if isinstance(days_of_month, list) and "Last" in days_of_month:
            trigger['run_on_last_day_of_month'] = True
            days_of_month.remove('Last')

        # Обработка "Ежемесячно" в месяцах
        months_of_year = trigger.get('months_of_year', [])
        if isinstance(months_of_year, list) and "Ежемесячно" in months_of_year:
            trigger['set_all_months'] = True
            months_of_year.remove('Ежемесячно')

        # Обработка интервала повторения
        repetition = trigger.get('repetition_interval', '')
        if repetition and isinstance(repetition, str):
            trigger['repetition_interval'] = 'PT' + repetition

        return trigger

    def _parse_multiple_triggers(
            self,
            triggers_list: list[dict]
            ) -> list[dict[str, Any]]:
        '''
        Парсит список триггеров из нового формата JSON.

        :param triggers_list: Список словарей с параметрами триггеров.
        :return: Список распарсенных триггеров.
        '''
        parsed_triggers = []

        for trigger_data in triggers_list:
            # Автоматический маппинг через словарь
            trigger: dict[str, Any] = {}
            for ru_key, en_key in self.TRIGGER_FIELD_MAPPING.items():
                value = trigger_data.get(ru_key)
                if value is not None:
                    trigger[en_key] = value

            # Установка значений по умолчанию
            for key, default_value in self.TRIGGER_DEFAULTS.items():
                trigger.setdefault(key, default_value)

            # Обработка "Last" в днях месяца
            days_of_month = trigger.get('days_of_month', [])
            if isinstance(days_of_month, list) and "Last" in days_of_month:
                trigger['run_on_last_day_of_month'] = True
                days_of_month.remove('Last')

            # Обработка "Ежемесячно" в месяцах
            months_of_year = trigger.get('months_of_year', [])
            if isinstance(months_of_year, list) and "Ежемесячно" in months_of_year:  # noqa
                trigger['set_all_months'] = True
                months_of_year.remove('Ежемесячно')

            # Обработка интервала повторения
            repetition = trigger.get('repetition_interval', '')
            if repetition and isinstance(repetition, str):
                trigger['repetition_interval'] = 'PT' + repetition

            parsed_triggers.append(trigger)

        return parsed_triggers

    def check_required_params(
            self,
            params: dict[str, Any]
            ) -> bool:
        '''
        Проверяет корректность поступивших параметров для создания
        задачи в Task Scheduler.
        UPDATED: Supports multiple triggers validation.

        :param params: Словарь с параметрами.
        :return: True, если все параметры корректны, иначе False.
        '''
        errors = []

        # Проверка наличия триггеров
        triggers = params.get('triggers', [])
        if not triggers or not isinstance(triggers, list):
            errors.append('Отсутствуют триггеры для задачи')
            handle_error(
                f'Некорректные параметры расписания {params.get("task_name")}: {errors}',  # noqa
                'error'
            )
            return False

        # Проверка каждого триггера
        for idx, trigger in enumerate(triggers):
            trigger_errors = self._validate_single_trigger(trigger, idx + 1)
            errors.extend(trigger_errors)

        if errors:
            handle_error(
                f'Некорректные параметры расписания {params.get("task_name")}: {errors}',  # noqa
                'error'
            )

        return len(errors) == 0

    def _validate_single_trigger(
            self,
            trigger: dict[str, Any],
            trigger_num: int
            ) -> list[str]:
        '''
        Проверяет корректность параметров одного триггера.

        :param trigger: Словарь с параметрами триггера.
        :param trigger_num: Номер триггера (для сообщений об ошибках).
        :return: Список ошибок валидации.
        '''
        from re import match
        from datetime import datetime

        errors = []
        valid_trigger_types = {'daily', 'weekly', 'monthly'}
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        time_pattern = r'^\d{2}:\d{2}$'
        valid_month_days = {str(day) for day in range(1, 32)} | {"Last"}
        valid_week_days = {
            'Понедельник',
            'Вторник',
            'Среда',
            'Четверг',
            'Пятница',
            'Суббота',
            'Воскресенье'
        }
        valid_months = {
            'Январь',
            'Февраль',
            'Март',
            'Апрель',
            'Май',
            'Июнь',
            'Июль',
            'Август',
            'Сентябрь',
            'Октябрь',
            'Ноябрь',
            'Декабрь',
            'Ежемесячно'
        }

        # Проверка обязательных параметров
        required_keys = {'trigger_type', 'start_date', 'start_time'}
        for key in required_keys:
            if key not in trigger:
                errors.append(
                    f'Триггер {trigger_num}: Отсутствует обязательный параметр: {key}'  # noqa
                )

        # Проверка типа триггера
        trigger_type = trigger.get('trigger_type', '')
        if trigger_type not in valid_trigger_types:
            errors.append(
                f'Триггер {trigger_num}: Неверный тип триггера: {trigger_type}. '  # noqa
                f'Ожидается один из: {valid_trigger_types}'
            )

        # Проверка формата даты
        start_date = trigger.get('start_date', '')
        if isinstance(start_date, str) and not match(date_pattern, start_date):
            errors.append(
                f'Триггер {trigger_num}: Неверный формат даты: {start_date}. '
                'Ожидается ГГГГ-ММ-ДД.'
            )

        # Проверка формата времени
        start_time = trigger.get('start_time', '')
        if isinstance(start_time, str) and not match(time_pattern, start_time):
            errors.append(
                f'Триггер {trigger_num}: Неверный формат времени: {start_time}. '  # noqa
                'Ожидается ЧЧ:ММ.'
            )

        # Проверка корректности даты и времени
        try:
            if isinstance(start_date, str) and isinstance(start_time, str):
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(start_time, '%H:%M')
        except ValueError:
            errors.append(
                f'Триггер {trigger_num}: Некорректная дата или время: {start_date} {start_time}'  # noqa
            )

        # Проверка days_of_month
        days_of_month_val = trigger.get('days_of_month')
        if days_of_month_val is not None and isinstance(days_of_month_val, list):  # noqa
            if len(days_of_month_val) > 0 or trigger.get('run_on_last_day_of_month'):  # noqa
                for day in days_of_month_val:
                    if str(day) not in valid_month_days:
                        errors.append(
                            f'Триггер {trigger_num}: Некорректный день месяца: {day}. '  # noqa
                            'Ожидается число от 1 до 31 или "Last".'
                        )
            elif trigger_type == 'monthly':
                errors.append(
                    f'Триггер {trigger_num}: Пустой список дней месяца. '
                    'Ожидается число от 1 до 31 или "Last".'
                )

        # Проверка days_of_week
        days_of_week_val = trigger.get('days_of_week')
        if days_of_week_val is not None and isinstance(days_of_week_val, list):
            if len(days_of_week_val) > 0:
                for day in days_of_week_val:
                    if day not in valid_week_days:
                        errors.append(
                            f'Триггер {trigger_num}: Некорректный день недели: {day}. '  # noqa
                            f'Ожидается один из: {valid_week_days}.'
                        )
            elif trigger_type == 'weekly':
                errors.append(
                    f'Триггер {trigger_num}: Пустой список дней недели. '
                    f'Ожидается один из: {valid_week_days}.'
                )

        # Проверка months_of_year
        months_of_year_val = trigger.get('months_of_year')
        if months_of_year_val is not None and isinstance(months_of_year_val, list):  # noqa
            if (len(months_of_year_val) > 0 or trigger.get('set_all_months')):
                for month in months_of_year_val:
                    if month not in valid_months:
                        errors.append(
                            f'Триггер {trigger_num}: Некорректный месяц: {month}. '  # noqa
                            f'Ожидается один из: {valid_months}.'
                        )
            elif trigger_type == 'monthly':
                errors.append(
                    f'Триггер {trigger_num}: Пустой список месяцев. '
                    f'Ожидается один из: {valid_months}.'
                )

        return errors
