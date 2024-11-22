import os
import re
from datetime import datetime
from pathlib import Path

from chardet import detect

from constants import DATABASE_TYPE
from error_handler import handle_error


def check_encoding(file_path: str) -> str | None:
    '''
    Проверяет кодировку файла и возвращает тип кодировки.

    :param file_path: Полный путь к файлу, включая сам файл.
    :return: Тип кодировки файла.
    '''
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(5000)

        encoding = detect(raw_data).get('encoding')
        if encoding is None:
            handle_error(f'Не удалось определить кодировку файла {file_path}'
                         'warning')
        if encoding in ['ascii', 'charmap', 'latin-1']:
            encoding = 'windows-1251'

        return encoding
    except (IOError, OSError, UnicodeDecodeError) as error:
        handle_error(f'Ошибка определения кодировки {file_path}: {error}',
                     'warning')
        return 'windows-1251'


def process_cmd_files(root_path: str) -> list[dict[str, str]]:
    '''
    Обрабатывает файлы cmd для получения почты, отправщика и темы

    :param root_path: Путь к основной дериктории отчетов.
    :param task_info: Словарь с назвнием таска.
    :return: Список словарей с информацией о почте, отправщике и теме.
    '''
    result = []
    task_info = {'task_name': Path(root_path).name}
    cmd_files = [f for f in os.listdir(root_path) if f.endswith('.cmd')]
    for file in cmd_files:
        file_path = os.path.join(root_path, file)
        encoding = check_encoding(file_path)
        if not encoding:
            continue

        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()

            email_theme_match = re.search(
                r'filename[\w_-]*\.exe\s+"([\w\.-@;]+)\|([^|]+)\|', content
            )
            if email_theme_match:
                task_info.update({
                    'sender_type': re.search(
                        r'filename[\w_-]*\.exe',
                        email_theme_match.group(0)
                        ).group(0),
                    'emails': email_theme_match.group(1),
                    'theme': email_theme_match.group(2)
                })
                result.append(task_info.copy())

        except (IOError, OSError, UnicodeDecodeError) as error:
            handle_error(f'Ошибка чтения {file_path}: {error}', 'warning')
            continue
    return result


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


def process_task_line(field_name: str, line: str) -> str:
    '''
    Обрабатывает строку для заданного поля, учитывая специальные случаи.

    :param field_name: Ключ для редактирования значения.
    :param line: Строка с данными.
    :return: Обработанное значение по определенным правилам.
    '''
    if field_name.startswith('Repeat'):
        parts = line.split(':')
        index = 2 if field_name == 'Repeat: Every' else 3
        return parts[index].strip() if len(parts) > index else ''

    value = line.split(':', 1)[1].strip()

    try:
        if field_name in ['Last Run Time', 'Start Time'] and value != 'N/A':
            return datetime.strptime(str(value), '%d.%m.%Y %I:%M:%S %p' if field_name == 'Last Run Time' else '%H:%M:%S')  # noqa
    except ValueError:
        return None

    replacements = {
        'TaskName': lambda x: x.replace('\\', ''),
        'Comment': lambda x: None if x.startswith('N/A') else x,
        'Last Result': lambda x: 'Success' if x == '0' else 'not launched' if x == '267011' else 'Error',  # noqa
        'Author': lambda x: x.replace('domain_name\\',
                                      '').replace('domain_name\\', ''),
        'Task To Run': lambda x: x.replace('\\', '\\')
    }
    return replacements.get(field_name, lambda x: x)(value)  # noqa


def task_data_processing(task_or_tasklist: list[str]) -> list[dict[str, str]]:
    '''
    Обработка полученных данных из Task Scheduler.
    Принимает 1 аргумент - список строк, содержащих информацию о задачах.\n
    Возвращает список словарей, где каждый словарь содержит информацию
    об одной задаче.

    :param task_or_tasklist: Список словарей с задачами Task Scheduler.
    :return: Список словарей с обработанными данными задач Task Scheduler.
    '''
    fields = {
        'TaskName': 'task_name',
        'Last Run Time': 'task_last_run_date',
        'Last Result': 'task_last_run_result',
        'Author': 'task_author_login',
        'Task To Run': 'task_to_run',
        'Comment': 'task_description',
        'Scheduled Task State': 'task_status',
        'Run As User': 'task_run_as_user',
        'Schedule Type': 'task_schedule_type',
        'Days': 'task_schedule_days',
        'Start Time': 'task_schedule_start_time',
        'Months': 'task_schedule_months',
        'Repeat: Every': 'task_schedule_repeat_every',
        'Repeat: Until: Time': 'task_schedule_repeat_until_time',
        'Repeat: Until: Duration': 'task_schedule_repeat_until_duration',
    }

    tasks_info = []
    current_task = {}

    for line in task_or_tasklist:
        if not line.strip():
            continue

        for field_name, field_key in fields.items():
            if line.startswith(f'{field_name}:'):
                processed_value = process_task_line(field_name, line)

                if field_name == 'Task To Run':
                    path_obj = Path(processed_value)
                    current_task.update({
                        'task_file_path': str(path_obj.parent),
                        'task_file_name': path_obj.name,
                        'database_hostname': path_obj.parent.parent.name.lower(

                        ),
                        'database_type': DATABASE_TYPE.get(
                            path_obj.parent.parent.name.lower(), None
                        )
                    })
                else:
                    current_task[field_key] = processed_value
        if (current_task.get('task_name', '').startswith('first_letters') and
           'task_schedule_repeat_until_duration' in current_task):
            tasks_info.append(current_task)
            current_task = {}

    return tasks_info


def proccess_main_data(
        data: list[dict[str, str]]
        ) -> list[dict[str, str | datetime]]:
    '''
    Обработка данных полученных из БД.

    :param data: Список словарей с данными.
    :return: Список обработанного данных в словаре.
    '''
    if not data:
        return []

    for data_dict in data:

        if 'request_id' in data_dict:
            data_dict['task_name'] = data_dict['request_id']

        for field_key, field_value in data_dict.items():
            if field_key == 'report_create_date' and field_value:
                data_dict[field_key] = datetime.strptime(str(field_value),
                                                         '%Y-%m-%d %H:%M:%S')
            if field_key == 'customer_orgstructure' and field_value:
                data_dict[field_key] = re.sub(r'/{2,}',
                                              '/',
                                              field_value).rstrip('/')
    return data
