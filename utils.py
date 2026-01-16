from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any


class BitmaskConverter:
    @staticmethod
    def days_of_week_to_bitmask(days_of_week: list[int]) -> int:
        '''Преобразует список дней недели в битовую маску.'''
        bitmask_map = {
            1: 0x01,  # Sunday
            2: 0x02,  # Monday
            3: 0x04,  # Tuesday
            4: 0x08,  # Wednesday
            5: 0x10,  # Thursday
            6: 0x20,  # Friday
            7: 0x40   # Saturday
        }
        return sum(bitmask_map[day] for day in days_of_week if day in bitmask_map)  # noqa

    @staticmethod
    def bitmask_to_days_of_week(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список дней недели.'''
        if bitmask is None:
            return []

        days_of_week = []
        bitmask_map = {
            0x01: 1,  # Sunday
            0x02: 2,  # Monday
            0x04: 3,  # Tuesday
            0x08: 4,  # Wednesday
            0x10: 5,  # Thursday
            0x20: 6,  # Friday
            0x40: 7   # Saturday
        }
        for flag, day in bitmask_map.items():
            if bitmask & flag:
                days_of_week.append(day)
        return days_of_week

    @staticmethod
    def days_of_month_to_bitmask(days_of_month: list[int]) -> int:
        '''Преобразует список дней месяца в битовую маску.'''
        return sum(2 ** (day - 1) for day in days_of_month) if days_of_month else False  # noqa

    @staticmethod
    def bitmask_to_days_of_month(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список дней месяца.'''
        days_of_month = []

        if bitmask == 0:
            days_of_month.append(bitmask)
        else:
            for day in range(1, 32):  # Дни месяца от 1 до 31
                if bitmask and (bitmask & (1 << (day - 1))):
                    days_of_month.append(day)

        return days_of_month

    @staticmethod
    def months_of_year_to_bitmask(months_of_year: list[int]) -> int:
        '''Преобразует список месяцев года в битовую маску.'''
        return sum(2 ** (month - 1) for month in months_of_year) if months_of_year else False  # noqa

    @staticmethod
    def bitmask_to_months_of_year(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список месяцев года.'''
        months_of_year = []
        for month in range(1, 13):  # Месяцы года от 1 до 12
            if bitmask and (bitmask & (1 << (month - 1))):
                months_of_year.append(month)
        return months_of_year


class DateConverter:
    # Переменные на русском, потому что в текущей задаче так проще.
    # Делать еще дополнительный маппинг не хочется.
    class Weekday(Enum):

        Понедельник = 2
        Вторник = 3
        Среда = 4
        Четверг = 5
        Пятница = 6
        Суббота = 7
        Воскресенье = 1

    class Month(Enum):
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

    @staticmethod
    def get_day_name(list_of_days: list[int]) -> list[str]:
        '''
        Статичный метод получения названия дня недели по индексу.

        :param list_of_days: Список цифр, соответствующих индексу дня.
        :return: Список названий дней недели.
        '''
        if list_of_days is None:
            return []

        day_names = []
        for day_num in list_of_days:
            day_names.append(DateConverter.Weekday(day_num).name)
        return day_names

    @staticmethod
    def get_day_num(list_of_days: list[str]) -> list[int]:
        '''
        Статичный метод получения индекса дня недели по названию.

        :param list_of_days: Список строк, соответствующих названию дня недели.
        :return: Список индексов дней недели.
        '''
        day_nums = []
        for day_name in list_of_days:
            day_nums.append(DateConverter.Weekday[day_name].value)
        return day_nums

    @staticmethod
    def get_month_name(list_of_months: list[int]) -> list[str]:
        '''
        Статичный метод получения названия месяца по индексу.

        :param list_of_months: Список чисел, соответствующих индексу месяца.
        :return: Список названий месяцев.
        '''
        month_names = []
        for month_num in list_of_months:
            month_names.append(DateConverter.Month(month_num).name)
        return month_names

    @staticmethod
    def get_month_num(list_of_months: list[str]) -> list[int]:
        '''
        Статичный метод получения индекса дня недели по названию.

        :param list_of_months: Список строк, соответствующих индексу месяца.
        :return: Список индексов месяцев.
        '''
        month_nums = []
        for month_name in list_of_months:
            month_nums.append(DateConverter.Month[month_name].value)
        return month_nums


class DateFormatter:
    @staticmethod
    def format_datetime(date_obj,
                        format: str = '%Y-%m-%d %H:%M:%S',
                        delta_hours: int = 0) -> datetime | None:
        '''
        Форматирует строку даты и времени, применяя изменение времени в часах.

        :param date_str: Строка даты и времени для форматирования.
        :param fmt: Формат, в котором представлена дата.
        :param delta_hours: Количество часов, чтобы добавить или вычесть.
        :return: Объект datetime без информации о временной зоне.
        '''
        import pywintypes

        if isinstance(date_obj, pywintypes.TimeType):
            dt = datetime.fromtimestamp(date_obj.timestamp())
        elif isinstance(date_obj, datetime):
            dt = date_obj
        elif isinstance(date_obj, str):
            if 'T' in date_obj and '+' in date_obj:
                dt = datetime.fromisoformat(date_obj)
                dt = dt.replace(tzinfo=None)
            elif 'T' in date_obj:
                dt = datetime.strptime(date_obj, '%Y-%m-%dT%H:%M:%S')
            else:
                dt = datetime.strptime(date_obj, format)
        else:
            dt = None
        return dt + timedelta(hours=delta_hours) if dt else None

    @staticmethod
    def format_start_time_and_date(start_time: str,
                                   start_date: str,
                                   delta_hours: int = -3) -> str:
        '''
        Форматирует время и дату начала, создавая строку с форматом ISO 8601.

        :param start_time: Время начала в формате 'HH:MM'.
        :param start_date: Дата начала в формате 'YYYY-MM-DD'.
        :param delta_hours: Количество часов, чтобы добавить или вычесть.
        :return: Строка даты и времени в формате ISO 8601.
        '''
        start_time_dt = datetime.strptime(start_time, '%H:%M')
        start_time_dt += timedelta(hours=delta_hours)
        start_time_str = start_time_dt.strftime('%H:%M:%S')
        return f'{start_date}T{start_time_str}Z'

    @staticmethod
    def parse_standard_datetime(date_str: str) -> datetime:
        '''
        Парсит стандартную строку даты и времени в объект datetime.

        :param date_str: Строка даты и времени в формате 'YYYY-MM-DD HH:MM:SS'.
        :return: Объект datetime.
        '''
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')


def parse_log_datetime(datetime_str: str) -> datetime:
    '''
    Helper function to parse datetime strings from log files.
    Handles both 12-hour (AM/PM) and 24-hour formats.

    :param datetime_str: Datetime string to parse.
    :return: Parsed datetime object.
    '''
    if datetime_str.endswith(('AM', 'PM')):
        return datetime.strptime(datetime_str, '%d.%m.%Y %I:%M:%S %p')
    return datetime.strptime(datetime_str, '%d.%m.%Y %H:%M:%S')


def parse_log_list_field(raw_value: str) -> str:
    '''
    Helper function to parse comma-separated list fields from log files.
    Converts format '[item1],[item2]' to 'item1,item2'.

    :param raw_value: Raw string value to parse.
    :return: Cleaned comma-separated string.
    '''
    if not raw_value:
        return ''
    return ','.join([
        item.strip('[]')
        for item in raw_value.split('],[')
        if item
    ])


def normalize_orgstructure_path(path: str | None) -> str | None:
    '''
    Helper function to normalize orgstructure paths.
    Removes duplicate slashes and trailing slashes.

    :param path: Orgstructure path to normalize.
    :return: Normalized path or None.
    '''
    import re
    if not isinstance(path, str):
        return path
    return re.sub(r'/{2,}', '/', path).rstrip('/')


def safe_int_for_dict_lookup(
        value: Any,
        mapping: dict[int, str],
        default: str
        ) -> str:
    '''
    Helper function to safely convert a value to int and look it up
    in a dictionary.

    :param value: Value to convert and lookup.
    :param mapping: Dictionary to lookup the value in.
    :param default: Default value if lookup fails.
    :return: Looked up value or default.
    '''
    try:
        code = (
            int(value)
            if isinstance(value, (int, str)) and str(value).isdigit()
            else 0
        )
        return mapping.get(code, default)
    except (ValueError, TypeError):
        return default


def check_encoding(file_path: str | Path) -> str | None:
    '''
    Проверяет кодировку файла и возвращает тип кодировки.

    :param file_path: Полный путь к файлу, включая сам файл.
    :return: Тип кодировки файла.
    '''
    from error_handler import handle_error

    from chardet import detect

    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(5000)

        encoding = detect(raw_data).get('encoding')
        if encoding is None:
            handle_error(f'Не удалось определить кодировку файла {file_path}',
                         'warning')
        if encoding in ['ascii', 'charmap', 'latin-1']:
            encoding = 'windows-1251'

        return encoding
    except (IOError, OSError, UnicodeDecodeError) as error:
        handle_error(f'Ошибка определения кодировки {file_path}: {error}',
                     'warning')
        return 'windows-1251'


def read_last_n_lines(
        file_path: Path,
        n: int = 10,
        encoding: str = 'utf-8'
        ) -> list[str]:
    '''
    Читает последние N строк из файла эффективно (без загрузки всего файла).
    Оптимизация: читает файл блоками с конца, что позволяет обрабатывать
    большие файлы (100+ MB) за миллисекунды.

    :param file_path: Путь к файлу.
    :param n: Количество строк для чтения (по умолчанию 10).
    :param encoding: Кодировка файла (по умолчанию 'utf-8').
    :return: Список последних N непустых строк.
    '''
    BUFFER_SIZE = 8192  # 8KB буфер для чтения

    try:
        with open(file_path, 'rb') as f:
            # Переходим в конец файла
            f.seek(0, 2)  # SEEK_END
            file_size = f.tell()

            if file_size == 0:
                return []

            # Читаем блоками с конца
            position = file_size
            buffer = b''
            lines = []

            while len(lines) < n and position > 0:
                # Определяем размер блока для чтения
                chunk_size = min(BUFFER_SIZE, position)
                position -= chunk_size

                # Читаем блок
                f.seek(position)
                chunk = f.read(chunk_size)
                buffer = chunk + buffer

                # Декодируем и разбиваем на строки
                try:
                    text = buffer.decode(encoding)
                    all_lines = text.split('\n')

                    # Если не в начале файла, первая "строка" может быть
                    # неполной
                    if position > 0:
                        buffer = all_lines[0].encode(encoding)
                        lines = all_lines[1:] + lines
                    else:
                        lines = all_lines + lines

                    # Проверяем, достаточно ли строк
                    if len(lines) >= n:
                        break
                except UnicodeDecodeError:
                    # Попали на середину многобайтового символа, читаем еще
                    continue

            # Фильтруем пустые строки и возвращаем последние N
            non_empty_lines = [line for line in lines if line.strip()]
            return (non_empty_lines[-n:]
                    if len(non_empty_lines) > n
                    else non_empty_lines)

    except (IOError, OSError) as error:
        from error_handler import handle_error
        handle_error(f'Ошибка чтения файла {file_path}: {error}', 'warning')
        return []


def find_new_folders_not_in_db() -> list[str]:
    '''
    Возвращает список путей к новым папкам, которых еще нет в БД.
    Проверяет только папки, созданные сегодня.
    OPTIMIZED: Uses batch request_id existence check.

    :return: Список путей к новым папкам.
    '''
    from pathlib import Path
    from database_handler import DatabaseConnector

    connector = DatabaseConnector()
    current_date = datetime.now().date()
    new_folders = []

    # Получаем все папки за один проход
    all_folders = path_find_walker('find')

    candidates = []
    for folder_path in all_folders:
        root_path = Path(folder_path)
        creation_time = root_path.stat().st_ctime
        creation_date = datetime.fromtimestamp(creation_time).date()

        if creation_date == current_date:
            request_id = root_path.name
            if request_id.startswith('REQ'):
                candidates.append((request_id, str(root_path)))

    request_ids = [req_id for req_id, _ in candidates]
    exists_map = connector.request_ids_exist_in_db(request_ids)

    new_folders = [
        path for req_id, path in candidates
        if not exists_map.get(req_id, False)
    ]

    return new_folders


def path_find_walker(
        type: str = 'walk',
        search_folder: str | None = None
        ) -> list[str]:
    '''
    Собирает папки, по которым нужно пройти.
    OPTIMIZED: Uses batch request_id existence check for 'walk' mode.

    :param type: walk - проход по корневым папкам.
                 find - поиск определенной папки или папок текущей датой.
    :param search_folder: Искомая папка формата RA, длиной 15 символов.
    :return: Список найденных директорий.
    '''
    from pathlib import Path
    from database_handler import DatabaseConnector

    from constants import ROOT_REPORT_PATHS

    connector = DatabaseConnector()
    ignored_dirs = {'!example', 'archive', 'log', 'old', '.git'}
    parent_dir = Path.cwd().parent
    result_paths = []

    all_candidate_paths = []

    for reports_path in ROOT_REPORT_PATHS:
        root_path = parent_dir / reports_path
        for dir_path in root_path.iterdir():
            if dir_path.name.lower() in ignored_dirs:
                continue
            if dir_path.is_dir():
                all_candidate_paths.append(dir_path)

    if type == 'walk':
        request_ids = [p.name for p in all_candidate_paths]
        exists_map = connector.request_ids_exist_in_db(request_ids)

        result_paths = [
            str(p) for p in all_candidate_paths
            if exists_map.get(p.name, False)
        ]

    elif type == 'find':
        if search_folder:
            result_paths = [
                str(p) for p in all_candidate_paths
                if p.name == search_folder
            ]
        else:
            result_paths = [str(p) for p in all_candidate_paths]

    return result_paths


def parse_arguments() -> set:
    '''
    Меод парсинга аргументов из командной строки.

    :return: Возвращает полученные аргументы с типом - множество.
    '''
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Обработка данных.')
    parser.add_argument('--tasks',
                        type=str,
                        help='Список задач через запятую',
                        default='')
    args = parser.parse_args()
    return set(args.tasks.split(',')) if args.tasks else set()


def move_folder_to_archive(source_path: str) -> str:
    '''
    Перемещает указанную папку в Архив.
    Если папка с таким именем уже существует в архиве, объединяет содержимое.

    :param source_path: Путь к папке, которая должна быть перемещена.
    :return: Возвращает строку с путем к архивной папке.
    '''
    from pathlib import Path
    from shutil import move, copytree

    from error_handler import handle_error

    source = Path(source_path)
    archive_dir = source.parent / 'Archive'

    # Создаем папку Archive если её нет
    archive_dir.mkdir(exist_ok=True)

    destination = archive_dir / source.name

    try:
        # Проверяем, существует ли папка с таким именем в архиве
        if destination.exists():
            # Папка существует - копируем файлы, перезаписывая существующие
            for item in source.iterdir():
                src_item = source / item.name
                dst_item = destination / item.name

                if src_item.is_dir():
                    # Рекурсивно копируем подпапку
                    if dst_item.exists():
                        # Удаляем старую подпапку и копируем новую
                        import shutil
                        shutil.rmtree(dst_item)
                    copytree(src_item, dst_item)
                else:
                    # Копируем файл (перезаписываем если существует)
                    import shutil
                    shutil.copy2(src_item, dst_item)

            # Удаляем исходную папку после копирования
            import shutil
            shutil.rmtree(source)

            handle_error(
                f'Папка {source.name} уже существовала в архиве. '
                f'Содержимое объединено.',
                'info'
            )
        else:
            # Папки нет - просто перемещаем
            move(str(source), str(destination))

        return str(destination)

    except (FileNotFoundError, PermissionError, OSError) as e:
        handle_error(
            f'Ошибка перемещения папки {source.name} в архив: {e}',
            'warning'
        )
        return 'Перемещение отчета в архив не удалось.'


def commit_changes_to_git(moved_folders: dict[str, list[str]]) -> None:
    '''
    Выполняет команды Git add, commit и push для перемещенных в архив отчетов.

    :param moved_folders: Словарь с родительскими папками и списками
                          перемещенных в них отчетов.
    '''
    from subprocess import CalledProcessError, run

    from error_handler import handle_error

    if not moved_folders:
        return

    # Собираем все папки с их сообщениями для коммита
    commit_data = []
    for parent_folder, folders in moved_folders.items():
        commit_message = (
            f'Перемещенные в архив отчеты: {",".join(folders)}'
        )
        commit_data.append((parent_folder, commit_message))

    # Выполняем add и commit для всех папок
    for parent_folder, commit_message in commit_data:
        try:
            run(['git', 'add', '.'], cwd=parent_folder, check=True)
            run(
                ['git', 'commit', '-m', commit_message],
                cwd=parent_folder,
                check=True
            )
        except CalledProcessError as e:
            handle_error(
                f'Не удалось создать коммит для {parent_folder}: {e}',
                'warning'
            )

    # Выполняем push для всех папок
    for parent_folder, _ in commit_data:
        try:
            run(['git', 'push'], cwd=parent_folder, check=True)
        except CalledProcessError as e:
            handle_error(
                f'Не удалось выполнить push для {parent_folder}: {e}',
                'warning'
            )


def log_execution_time(func):
    '''
    Декоратор для логирования времени начала, завершения и длительности
    выполнения функции.

    Формат времени: 2025-11-20 11:55:03,263
    '''
    import time
    from datetime import datetime
    from functools import wraps

    from logger import LOGGER_ERROR_TYPE

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Время начала
        start_time = time.time()
        start_datetime = datetime.now()
        start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]

        LOGGER_ERROR_TYPE['info']('-' * 60)
        LOGGER_ERROR_TYPE['info'](f'Сервис запущен: {start_str}')

        try:
            # Выполняем функцию
            result = func(*args, **kwargs)
            return result
        finally:
            # Время завершения
            end_time = time.time()
            end_datetime = datetime.now()
            end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]

            # Вычисляем длительность
            duration = end_time - start_time
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = duration % 60

            LOGGER_ERROR_TYPE['info'](f'Сервис завершён: {end_str}')
            LOGGER_ERROR_TYPE['info'](
                f'Время выполнения: {hours:02d}:{minutes:02d}:{seconds:06.3f}'
            )
            LOGGER_ERROR_TYPE['info']('-' * 60)
            LOGGER_ERROR_TYPE['info'](' ')

    return wrapper


def load_configs(config_file: str) -> dict[str, Any]:
    '''
    Метод возвращает словарь с данными для подключения к различным БД.

    :param config_file: Имя БД.
    :return: Словарь с данными для подключения.
    '''
    from json import JSONDecodeError, load
    from pathlib import Path

    from error_handler import handle_error

    configs_folder = Path().cwd()
    try:
        with open(configs_folder / config_file) as f:
            return load(f)
    except (IOError, OSError, JSONDecodeError) as error:
        handle_error(f'Ошибка чтения конфигураций: {error}', 'critical')
        return {}
