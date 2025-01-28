from enum import Enum

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
    '''
    def __init__(self):
        self.scheduler = win32com.client.Dispatch('Schedule.Service')
        self.scheduler.Connect()

    def create_or_update_task(
            self,
            task_name: str,
            executable_path: str,
            trigger_type: str,
            start_date: str,
            start_time: str,
            stop_if_runs_longer: str = '2H',
            interval_week_days: int = 1,
            days_of_week: list[str] = ['Понедельник',],
            days_of_month: list[int] = [1,],
            months_of_year: list[str] = ['Январь',],
            task_description: str = None,
            task_state: bool = 1,
            ):
        '''
        Создаёт или обновляет задачу в планировщике Windows Task Scheduler.

        :param task_name: Имя задачи.
        :param executable_path: Путь к исполняемому файлу
        или скрипту (.cmd, .exe и т.д.).
        :param trigger_type: Тип триггера ('daily', 'weekly', 'monthly').
        :param start_date: Дата начала работы расписания
        в формате 'YYYY-MM-DD'.
        :param start_time: Время начала работы расписания в формате 'HH:MM'.
        :param stop_if_runs_longer: Время в формате '2H', после
        которого задача будет остановлена.
        :param interval_days: Интервал для ежедневного запуска или
        еженедельного запуска (по умолчанию 1 день).
        :param days_of_week: Список дней недели для запуска
        (например, [Понедельник, Среда, Пятница]).
        :param days_of_month: Список чисел месяца для запуска
        (например, [1, 15, 30]).
        :param months_of_year: Список месяцев для запуска
        (например, [Январь, Июнь, Декабрь]).
        '''
        folder = self.scheduler.GetFolder('\\')

        # Создание или обновление задачи
        task_definition = self.scheduler.NewTask(0)

        # Настройка регистрационной информации
        task_definition.RegistrationInfo.Description = task_description
        task_definition.RegistrationInfo.Author = 'automation_reports'

        # Настройка триггера
        trigger_conf = TriggerConfig[trigger_type]
        trigger = trigger_conf.create_trigger(task_definition)

        if trigger:
            start_boundary = DateFormatter.format_start_time_and_date(
                start_time=start_time,
                start_date=start_date
            )
            days_of_week = DateConverter.get_day_num(days_of_week)
            months_of_year = DateConverter.get_month_num(months_of_year)
            trigger_conf.configure_trigger(
                trigger,
                start_datetime=start_boundary,
                days_interval=interval_week_days,
                weeks_interval=interval_week_days,
                days_of_week=days_of_week,
                days_of_month=days_of_month,
                months_of_year=months_of_year
                )

        # Настройка действия
        action = task_definition.Actions.Create(0)
        action.Path = executable_path

        # Настройка общих параметров
        task_definition.Settings.Enabled = task_state
        task_definition.Settings.StartWhenAvailable = True
        task_definition.Settings.Hidden = False

        # Установка параметров остановки задачи
        task_definition.Settings.ExecutionTimeLimit = f'PT{stop_if_runs_longer}'  # noqa

        # Настройка параметров учетной записи
        task_definition.Principal.LogonType = 3  # S4U (Do not store password)
        task_definition.Principal.RunLevel = 1  # Run with highest privileges

        # Настройка совместимости
        task_definition.Settings.Compatibility = 6  # Windows Server 2019

        # Регистрация задачи (создание или обновление)
        folder.RegisterTaskDefinition(
            task_name,
            task_definition,
            6,  # TASK_CREATE_OR_UPDATE
            None,  # Без пароля
            None,
            2,  # Run whether user is logged on or not
            None
        )

    def get_task_info(self, task_name: str) -> dict[str, ]:
        '''
        Читает информацию о задаче из планировщика Windows Task Scheduler.

        :param task_name: Имя задачи.
        :return: Список строк, имитирующий вывод команды schtasks.
        '''
        folder = self.scheduler.GetFolder('\\')
        try:
            task = folder.GetTask(task_name)
            definition = task.Definition

            triggers = []
            for idx, trigger in enumerate(definition.Triggers, start=1):
                days_of_week = DateConverter.get_day_name(
                    BitmaskConverter.bitmask_to_days_of_week(
                        getattr(trigger, 'DaysOfWeek', None)
                        )
                    )
                days_of_months = BitmaskConverter.bitmask_to_days_of_month(
                    getattr(trigger, 'DaysOfMonth', None)
                )
                months_of_year = DateConverter.get_month_name(
                    BitmaskConverter.bitmask_to_months_of_year(
                        getattr(trigger, 'MonthsOfYear', None)
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
        except Exception as e:
            handle_error(f'read_task_error: {e}', 'warning')
            return {}

    def parse_schedule_params(
            self,
            root_path: str,
            requst_id: str) -> dict[str, str | int | list[str | int]]:
        import json
        from pathlib import Path

        from utils import check_encoding

        schedule_file_path = Path(root_path).joinpath(f'{requst_id}.json')
        executable_path = Path(root_path).joinpath(f'{requst_id}.cmd')
        if schedule_file_path.is_file():
            method_dispatch = {
                'Тип расписания': 'trigger_type',
                'Дата начала': 'start_date',
                'Время начала': 'start_time',
                'Остановить, если дольше': 'stop_if_runs_longer',
                'Частота отправки': 'interval_week_days',
                'Дни недели': 'days_of_week',
                'Дни месяца': 'days_of_month',
                'Месяцы': 'months_of_year',
                'Описание задачи': 'task_description',
                'Статус расписания': 'task_state',
            }
            try:
                encoding = check_encoding(schedule_file_path)
                with open(schedule_file_path, 'r', encoding=encoding) as f:
                    content: dict = json.load(f)
            except (IOError, OSError, UnicodeDecodeError) as error:
                handle_error(f'Ошибка чтения {schedule_file_path}: {error}',
                             'warning')
                return False

            params = {
                'task_name': schedule_file_path.parent.name,
                'executable_path': executable_path,
            }
            for param_key, param_value in content.items():
                params[method_dispatch[param_key]] = param_value

            required_keys = {'trigger_type', 'start_date', 'start_time'}
            return params if all(key in params for key in required_keys) and executable_path.is_file() else False  # noqa
