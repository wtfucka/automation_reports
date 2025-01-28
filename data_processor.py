import re
from datetime import datetime
from pathlib import Path

from error_handler import handle_error
from utils import check_encoding, DateFormatter


class SchedulerDataProcessor:
    def __init__(self):
        self.fields = {
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
        self.task_state = {
            0: 'Unknown',
            1: 'Отключена',
            2: 'В очереди',
            3: 'Включена',
            4: 'Выполняется'
        }
        self.task_last_result = {
            0: "Success",
            1: "General error",
            267008: "Running",
            267009: "Ready",
            267010: "Completed",
            267011: "Not yet run",
            267012: "Disabled",
            267013: "Not scheduled",
            267014: "Skipped due to sleep",
        }

    def process_trigger_info(self,
                             schedule_info: list[dict[str, ]]) -> dict[str, ]:
        '''
        Обработчик списка словарей триггеров расписания.

        :param schedule_info: Принимает словарь с данными о триггерах.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''
        schedule_data = {
            'task_schedule_type': [],
            'task_trigger_status': [],
            'task_schedule_start_date': [],
            'task_schedule_days_interval': [],
            'task_schedule_weeks_interval': [],
            'task_schedule_week_days': [],
            'task_schedule_months': [],
            'task_schedule_month_days': [],
            'task_schedule_repeat_every': [],
            'task_schedule_repeat_until_time': [],
            'task_schedule_repeat_until_duration': []
        }
        add_prefix = len(schedule_info) > 1
        for index, trigger in enumerate(schedule_info):
            prefix = f'Trigger {index + 1}: ' if add_prefix else ''
            for trigger_key, trigger_value in trigger.items():
                if trigger_key == 'StartBoundary':
                    trigger_value = DateFormatter.format_datetime(trigger_value, delta_hours=-1)  # noqa
                    schedule_data[self.fields[trigger_key]].append(
                        f'{prefix}{trigger_value}'
                        )
                elif trigger_key in ('DaysOfWeek',
                                     'DaysOfMonth',
                                     'MonthsOfYear') and trigger_value:
                    schedule_data[self.fields[trigger_key]].append(
                        f'{prefix}{", ".join(map(str, trigger_value))}'
                        )
                else:
                    schedule_data[self.fields[trigger_key]].append(
                        f'{prefix}{trigger_value}'
                        )

        for key, value in schedule_data.items():
            value = ', '.join(map(str, filter(None, value)))
            schedule_data[key] = value if value else None

        return schedule_data

    def process_schedule_path(self, path_list: list[str]) -> dict[str, str]:
        '''
        Обработчик списка путей до файлов исполнения.

        :param path_list: Принимает список путей до файла исполнения.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''

        path_data = {
                    'task_file_path': set(),
                    'task_file_name': set(),
                }
        for path in path_list:
            path_obj = Path(path)
            path_data['task_file_path'].add(str(path_obj.parent))  # noqa
            path_data['task_file_name'].add(path_obj.name)

        for key, value in path_data.items():
            if value:
                path_data[key] = ', '.join(list(value))

        return path_data

    def task_data_processing(
            self,
            task_list: list[dict[str, str]]) -> list[dict[str, str]]:
        '''
        Обработчик информации о задаче в Task Scheduler.

        :param task_data: Принимает словарь с данными о задаче расписания.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''
        processed_task_list = []

        for task in task_list:
            processed_task = {}

            for task_key, task_value in task.items():
                if task_key == 'Schedule Info':
                    processed_task.update(self.process_trigger_info(task_value))  # noqa
                elif task_key == 'Last Result':
                    processed_task[self.fields.get(task_key, task_key)] = self.task_last_result.get(task_value, "Unknown result")  # noqa
                elif task_key == 'Scheduled Task State':
                    processed_task[self.fields.get(task_key, task_key)] = self.task_state.get(task_value, "Unknown state")  # noqa
                elif task_key == 'Last Run Time':
                    processed_task[self.fields.get(task_key, task_key)] = DateFormatter.format_datetime(task_value, delta_hours=-1)  # noqa
                elif task_key == 'Task To Run':
                    processed_task.update(self.process_schedule_path(task_value))  # noqa
                else:
                    processed_task[self.fields.get(task_key, task_key)] = task_value  # noqa

            processed_task_list.append(processed_task)

        return processed_task_list


def process_cmd_files(root_path: str) -> dict[str, str]:
    '''
    Обрабатывает файлы cmd для получения почты, отправщика и темы

    :param root_path: Путь к основной дериктории отчетов.
    :return: Список словарей с информацией о почте, отправщике и теме.
    '''
    from constants import DATABASE_TYPE

    path = Path(root_path)
    task_info = {'task_name': path.name}
    cmd_file = None

    for f in path.iterdir():
        if f.is_file() and f.name.startswith(...) and f.name.endswith(...):  # noqa
            cmd_file = f
            break

    try:
        file_path = root_path / cmd_file
        encoding = check_encoding(file_path)

        with open(file_path, 'r', encoding=encoding) as f:
            content = f.read()

        database_pattern = re.compile(...)
        sender_pattern = re.compile(...)
        database_match = database_pattern.search(content)
        sender_match = sender_pattern.search(content)

        if sender_match:
            task_info.update({
                'sender_type': re.search(..., sender_match.group(0)).group(0),
                'emails': sender_match.group(1),
                'theme': sender_match.group(2)
            })

        if database_match:
            database_name = database_match.group(1).lower()
            task_info.update(
                {
                    'database_hostname': database_name,
                    'database_type': DATABASE_TYPE.get(database_name, None)
                }
            )

    except (IOError, OSError, UnicodeDecodeError) as error:
        handle_error(f'Ошибка чтения {file_path}: {error}', 'warning')
    return task_info


def merge_with_existing_data(
        existing_data: list[dict[str, str]],
        new_data: list[dict[str, str]]
        ) -> list[dict[str, str]]:
    '''
    Метод объединения новых данных с существующими в словаре.

    :param existing_data: Список словарей с текущими данными.
    :param new_data: Список словарей с данными, которые нужно добавить.
    :return: Список словарей с объединенными данными.
    '''
    for existing_task in existing_data:
        matching_task = next(
            (task for task in new_data if task.get(
                'task_name') == existing_task.get('task_name')),
            None
        )
        if matching_task:
            existing_task.update({
                k: v for k, v in matching_task.items() if k != 'task_name'
            })

    return existing_data


def proccess_main_data(
        data: list[dict[str, str]]
        ) -> list[dict[str, str | datetime]]:
    '''
    Обработка данных полученных из БД.

    :param data: Список словарей с данными.
    :return: Список словарей с обработанными данными.
    '''
    if not data:
        return []

    for data_dict in data:

        if 'request_id' in data_dict:
            data_dict['task_name'] = data_dict['request_id']
            data_dict['customer_orgstructure'] = re.sub(
                r'/{2,}',
                '/',
                data_dict['customer_orgstructure']).rstrip('/')
            data_dict['receiver_orgstructure'] = re.sub(
                r'/{2,}',
                '/',
                data_dict['receiver_orgstructure']).rstrip('/')

    return data
