import re
from datetime import datetime
from pathlib import Path
from typing import Any

from error_handler import handle_error
from utils import (
    check_encoding,
    DateFormatter,
    normalize_orgstructure_path,
    parse_log_datetime,
    parse_log_list_field,
    read_last_n_lines,
    safe_int_for_dict_lookup
)


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
            1: "Incorrect function",
            267008: "Ready",
            267009: "Running",
            267010: "Disabled",
            267011: "Has not run",
            267012: "No more runs",
            267013: "Not scheduled",
            267014: "Terminated",
            267015: "No valid triggers",
            2147750665: "Already running",
            2147942402: "File not found",
        }

    def process_trigger_info(
            self,
            schedule_info: list[dict[str, str]]
            ) -> dict[str, str | None]:
        '''
        Обработчик списка словарей триггеров расписания.

        :param schedule_info: Принимает словарь с данными о триггерах.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''
        schedule_data: dict[str, list[str]] = {
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
                    trigger_value = DateFormatter.format_datetime(trigger_value)  # noqa
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

        result: dict[str, str | None] = {}
        for key, value in schedule_data.items():
            joined_value = ', '.join(map(str, filter(None, value)))
            result[key] = joined_value if joined_value else None

        return result

    def process_schedule_path(
            self,
            path_list: list[str]
            ) -> dict[str, str]:
        '''
        Обработчик списка путей до файлов исполнения.

        :param path_list: Принимает список путей до файла исполнения.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''

        path_data: dict[str, set[str]] = {
                    'task_file_path': set(),
                    'task_file_name': set(),
                }
        for path in path_list:
            path_obj = Path(path)
            path_data['task_file_path'].add(str(path_obj.parent))  # noqa
            path_data['task_file_name'].add(path_obj.name)

        result_data: dict[str, str] = {}
        for key, value in path_data.items():
            if value:
                result_data[key] = ', '.join(list(value))

        return result_data

    def task_data_processing(
            self,
            task_list: list[dict[str, Any]]
            ) -> list[dict[str, Any]]:
        '''
        Обработчик информации о задаче в Task Scheduler.

        :param task_data: Принимает словарь с данными о задаче расписания.
        :return: Вовзращает отформатированные данные в виде словаря.
        '''
        from collections import defaultdict
        from utils import commit_changes_to_git, move_folder_to_archive

        processed_task_list: list[dict[str, Any]] = []
        moved_folders_by_parent: defaultdict[str, list[str]] = defaultdict(
            list
        )

        for task in task_list:
            processed_task: dict[str, Any] = {}

            for task_key, task_value in task.items():
                if task_key == 'Schedule Info':
                    processed_task.update(
                        self.process_trigger_info(task_value)
                    )
                elif task_key == 'Last Result':
                    field_key = self.fields.get(task_key, task_key)
                    processed_task[field_key] = safe_int_for_dict_lookup(
                        task_value,
                        self.task_last_result,
                        "Unknown result"
                    )
                elif task_key == 'Scheduled Task State':
                    path_list = task.get('Task To Run')
                    if task_value == 1 and path_list:
                        # Task To Run is a list of paths, take first one
                        path = path_list[0] if path_list else None
                        if path:
                            current_path = Path(path).parent
                            new_destination = move_folder_to_archive(
                                str(current_path)
                            )
                            processed_task['report_in_archive'] = True
                            processed_task['report_archive_folder'] = (
                                new_destination
                            )
                            moved_folders_by_parent[
                                str(current_path.parent)
                            ].append(current_path.name)
                    field_key = self.fields.get(task_key, task_key)
                    processed_task[field_key] = safe_int_for_dict_lookup(
                        task_value,
                        self.task_state,
                        "Unknown state"
                    )
                elif task_key == 'Last Run Time':
                    field_key = self.fields.get(task_key, task_key)
                    processed_task[field_key] = (
                        DateFormatter.format_datetime(
                            task_value,
                            delta_hours=-3
                        )
                    )
                elif task_key == 'Task To Run':
                    processed_task.update(
                        self.process_schedule_path(task_value)
                    )
                else:
                    field_key = self.fields.get(task_key, task_key)
                    processed_task[field_key] = task_value

            processed_task_list.append(processed_task)

        commit_changes_to_git(moved_folders_by_parent)
        return processed_task_list


def process_cmd_files(root_path: str) -> dict[str, Any]:
    '''
    Обрабатывает файлы cmd для получения почты, отправщика и темы

    :param root_path: Путь к основной дериктории отчетов.
    :return: Словарь с информацией о почте, отправщике и теме.
    '''
    from constants import DATABASE_TYPE

    path = Path(root_path)
    task_info: dict[str, Any] = {'task_name': path.name}
    cmd_file = None

    for f in path.iterdir():
        if (f.is_file() and f.name.startswith('REQ')
                and f.name.endswith('.cmd')):
            cmd_file = f
            break

    if not cmd_file:
        return task_info

    try:
        file_path = path / cmd_file
        encoding = check_encoding(file_path)

        with open(file_path, 'r', encoding=encoding) as f:
            content = f.read()

        database_pattern = re.compile(r'-dbName\s+"([^"]+)"')
        sender_pattern = re.compile(
            r'mailer_name\.exe\s+"([\w\.-@;]+)\|([^|]+)\|'
            )
        theme_pattern = re.compile(r'set\s+"REPORT_NAME=(.+?)"')

        for line in reversed(content.splitlines()):
            sender_match = sender_pattern.search(line)
            if sender_match:
                sender_search = re.search(
                    r'mailer_name\.exe',
                    sender_match.group(0)
                )
                task_info.update({
                    'sender_type': (
                        sender_search.group(0) if sender_search else ''
                    ),
                    'emails': sender_match.group(1),
                    'theme': sender_match.group(2)
                })
                break
        # Два разных условия поиска темы для поддержания как старого, так и
        # нового варианта присваивания имени отчета
        theme_match = theme_pattern.search(content)
        if theme_match:
            task_info['theme'] = theme_match.group(1)
            # task_info.update({'theme': theme_match.group(1)})

        database_match = database_pattern.search(content)
        if database_match:
            database_name = database_match.group(1).lower()
            task_info['database_hostname'] = database_name
            task_info['database_type'] = DATABASE_TYPE.get(
                database_name,
                None
            )

    except (IOError, OSError, UnicodeDecodeError) as error:
        handle_error(f'Ошибка чтения {path / cmd_file}: {error}', 'warning')
    return task_info


def merge_with_existing_data(
        existing_data: list[dict[str, Any]],
        new_data: list[dict[str, Any]]
        ) -> list[dict[str, Any]]:
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


def process_main_data(
        data: list[dict[str, str]]
        ) -> list[dict[str, str | datetime]]:
    '''
    Обработка данных полученных из БД.

    :param data: Список словарей с данными.
    :return: Список словарей с обработанными данными.
    '''
    if not data:
        return []

    result: list[dict[str, str | datetime]] = []
    for data_dict in data:
        typed_dict: dict[str, str | datetime] = dict(data_dict)

        if 'request_id' in typed_dict:
            typed_dict['task_name'] = typed_dict['request_id']
            customer_org = normalize_orgstructure_path(
                typed_dict.get('customer_orgstructure')  # type: ignore
            )
            if customer_org is not None:
                typed_dict['customer_orgstructure'] = customer_org

            receiver_org = normalize_orgstructure_path(
                typed_dict.get('receiver_orgstructure')  # type: ignore
            )
            if receiver_org is not None:
                typed_dict['receiver_orgstructure'] = receiver_org

        result.append(typed_dict)

    return result


def process_log_files(root_path: str) -> dict[str, Any]:
    '''
    Обрабатывает файл log для получения даты, статуса, получателей
     и вложения последней отправки письма.

    :param root_path: Путь к основной дериктории отчетов.
    :return: Словарь с информацией о дате, статусе, получателях и вложениях.
    '''
    from constants import LOG_FILE_NAME

    path = Path(root_path)
    log_info: dict[str, Any] = {'task_name': path.name}
    log_file_path = path / LOG_FILE_NAME
    log_subdir_file_path = path / 'log' / LOG_FILE_NAME

    if log_file_path.is_file():
        log_file = log_file_path
    elif log_subdir_file_path.is_file():
        log_file = log_subdir_file_path
    else:
        handle_error(f'Файл лога не найден {path.name}', 'warning')
        return {}

    last_match = None

    try:
        encoding = check_encoding(log_file) or 'windows-1251'
        pattern = re.compile(
            r'(?P<datetime>\d{2}\.\d{2}\.\d{4} \d{1,2}:\d{2}:\d{2}(?: [APM]{2})?)\s*\|'  # noqa
            r'(?P<status>Success|Issue|SendError)\s*\|'
            r'(?:Письмо успешно отправлено получателям:\s*(?P<recipients>(?:\[[^\]]*\],?)*)\s*\|)?'  # noqa
            r'(?:Вложения:\s*(?P<attachments>(?:\[[^\]]*\],?)*)\s*\|)?'
            r'(?:Не найденные вложения:\s*(?P<missing_attachments>(?:\[[^\]]*\],?)*)\s*)?'  # noqa
            r'(?P<error_message>.*)'
            )

        last_lines = read_last_n_lines(log_file, n=10, encoding=encoding)

        # Обрабатываем строки с конца
        for line in reversed(last_lines):
            match = pattern.search(line)
            if match:
                current_date = datetime.now().date()
                datetime_str = match.group('datetime')
                log_date = parse_log_datetime(datetime_str).date()

                if log_date == current_date:
                    last_match = match
                    break
                elif not last_match:
                    last_match = match

        if last_match:
            last_match_log_date = last_match.group('datetime')
            log_datetime = parse_log_datetime(last_match_log_date)
            status = last_match.group('status') or ''

            recipients = parse_log_list_field(
                last_match.group('recipients') or ''
            )
            attachments = parse_log_list_field(
                last_match.group('attachments') or ''
            )
            missing_attachments = parse_log_list_field(
                last_match.group('missing_attachments') or ''
            )

            error_message = last_match.group('error_message').strip() or ''

            log_info['last_send_date'] = log_datetime
            log_info['last_send_status'] = (
                status if 'fake@fake.ru' not in recipients
                else 'SendError'
            )
            log_info['last_send_recipients'] = recipients
            log_info['last_send_attachments'] = attachments
            if missing_attachments:
                log_info['last_send_issue'] = missing_attachments
            if error_message:
                log_info['last_send_error'] = error_message

    except (IOError, OSError, UnicodeDecodeError, TypeError) as error:
        handle_error(f'Ошибка чтения {log_file}: {error}', 'warning')
        return {}

    return log_info
