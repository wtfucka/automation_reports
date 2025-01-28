from datetime import datetime, timedelta
from enum import Enum


class BitmaskConverter:
    @staticmethod
    def days_of_week_to_bitmask(days_of_week: list[int]) -> int:
        '''Преобразует список дней недели в битовую маску.'''
        return sum(2 ** day for day in days_of_week) if days_of_week else []

    @staticmethod
    def bitmask_to_days_of_week(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список дней недели.'''
        days_of_week = []
        for day in range(7):  # Дни недели кодируются от 0 до 6
            if bitmask and (bitmask & (1 << day)):
                days_of_week.append(day)
        return days_of_week

    @staticmethod
    def days_of_month_to_bitmask(days_of_month: list[int]) -> int:
        '''Преобразует список дней месяца в битовую маску.'''
        return sum(2 ** (day - 1) for day in days_of_month) if days_of_month else []  # noqa

    @staticmethod
    def bitmask_to_days_of_month(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список дней месяца.'''
        days_of_month = []
        for day in range(1, 32):  # Дни месяца от 1 до 31
            if bitmask and (bitmask & (1 << (day - 1))):
                days_of_month.append(day)
        return days_of_month

    @staticmethod
    def months_of_year_to_bitmask(months_of_year: list[int]) -> int:
        '''Преобразует список месяцев года в битовую маску.'''
        return sum(2 ** (month - 1) for month in months_of_year) if months_of_year else []  # noqa

    @staticmethod
    def bitmask_to_months_of_year(bitmask: int) -> list[int]:
        '''Преобразует битовую маску в список месяцев года.'''
        months_of_year = []
        for month in range(1, 13):  # Месяцы года от 1 до 12
            if bitmask and (bitmask & (1 << (month - 1))):
                months_of_year.append(month)
        return months_of_year


class DateConverter:
    class Weekday(Enum):
        # Переменные на русском, потому что в текущей задаче так проще.
        # Делать еще дополнительный маппинг не хочется.

        Понедельник = 1
        Вторник = 2
        Среда = 3
        Четверг = 4
        Пятница = 5
        Суббота = 6
        Воскресенье = 7

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
                        delta_hours: int = 0) -> datetime:
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
            format_type = '%Y-%m-%dT%H:%M:%S' if 'T' in date_obj else format  # noqa
            dt = datetime.strptime(date_obj, format_type)
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


def check_encoding(file_path: str) -> str:
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
            handle_error(f'Не удалось определить кодировку файла {file_path}'
                         'warning')
        if encoding in ['ascii', 'charmap', 'latin-1']:
            encoding = 'windows-1251'

        return encoding
    except (IOError, OSError, UnicodeDecodeError) as error:
        handle_error(f'Ошибка определения кодировки {file_path}: {error}',
                     'warning')
        return 'windows-1251'


def find_new_folders_not_in_db() -> list[str]:
    '''
    Ищет папки созданные текущей датой и возвращает те, которых нет в БД.

    :return: Список путей к новым папкам.
    '''
    from pathlib import Path
    from database_handler import DatabaseConnector

    connector = DatabaseConnector()

    current_date = datetime.now().date()
    new_folders = []
    for root_path in path_find_walker('find'):
        root_path = Path(root_path)
        creation_time = root_path.stat().st_ctime
        creation_date = datetime.fromtimestamp(creation_time).date()

        if creation_date == current_date:
            request_id = root_path.name
            if request_id.startswith(...) and not connector.request_id_exists_in_db(request_id): # noqa
                new_folders.append(root_path)
    return new_folders


def path_find_walker(
        type: str = 'walk',
        search_folder: str = None
        ) -> list[str]:
    '''
    Собирает папки, по которым нужно пройти.

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

    for reports_path in ROOT_REPORT_PATHS:
        root_path = parent_dir / reports_path
        for dir_path in root_path.iterdir():
            if dir_path.name.lower() in ignored_dirs:
                continue
            if dir_path.is_dir():
                if type == 'walk' and connector.request_id_exists_in_db(dir_path.name):  # noqa
                    result_paths.append(dir_path)
                elif type == 'find':
                    if search_folder and dir_path.name == search_folder:
                        result_paths.append(dir_path)
                    else:
                        result_paths.append(dir_path)

    return result_paths


def parse_arguments() -> set:
    '''
    Меод парсинга аргументов из командной строки.

    :return: Возвращает полученные аргументы с типом - множество.
    '''
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Обработка данных.")
    parser.add_argument('--tasks',
                        type=str,
                        help="Список задач через запятую",
                        default="")
    args = parser.parse_args()
    return set(args.tasks.split(',')) if args.tasks else set()
