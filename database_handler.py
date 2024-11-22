import oracledb
import pymssql

from error_handler import handle_error

MSSQL_CONFIG = {
    ...
}

ORACLE_CONFIG = {
    ...
}


def execute_mssql_query_read(query: str,
                             params: tuple = ()) -> list[dict[str, str]]:
    '''
    Чтение данных из БД MSSQL.

    :param query: Текст SQL-запроса.
    :param params: Параметры для запроса.
    :return: Словарь со значением поля request_id
    '''
    try:
        with pymssql.connect(**MSSQL_CONFIG) as connection:
            cursor = connection.cursor(as_dict=True)
            cursor.execute(query, params)
            return cursor.fetchall()
    except (pymssql.IntegrityError, pymssql.DatabaseError) as error:
        handle_error(f'Ошибка выполнения MSSQL запроса: {error}',
                     'critical')


def execute_mssql_query(query: str,
                        params: tuple = ()) -> None:
    '''
    Выполняет запрос к MS SQL.

    :param query: Текст SQL-запроса.
    :param params: Параметры для запроса.
    '''
    try:
        with pymssql.connect(**MSSQL_CONFIG) as connection:
            cursor = connection.cursor()
            cursor.execute(query, params)
            connection.commit()
    except (pymssql.IntegrityError, pymssql.DatabaseError) as error:
        handle_error(f'Ошибка выполнения MSSQL запроса: {error}',
                     'critical')


def execute_oracle_query(query: str,
                         params: list = []) -> list[dict[str, str]]:
    '''
    Выполняет запрос к Oracle.

    :param query: Текст SQL-запроса.
    :param params: Параметры для запроса.
    '''
    try:
        oracledb.init_oracle_client()
        with oracledb.connect(**ORACLE_CONFIG) as connection:
            cursor = connection.cursor()
            cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return result
    except (oracledb.IntegrityError, oracledb.DatabaseError) as error:
        handle_error(f"Ошибка выполнения Oracle запроса: {error}",
                     'critical')
        return []


def initialize_db() -> None:
    '''
    Создает таблицу, если она не существует с нужными полями.
    '''
    query = '''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='table_name' AND xtype='U')
            CREATE TABLE table_name (
                id INT IDENTITY(1,1) PRIMARY KEY,
                request_id NVARCHAR(50) UNIQUE,
                customer_login NVARCHAR(30),
                customer_name NVARCHAR(150),
                customer_company NVARCHAR(150),
                customer_orgstructure NVARCHAR(MAX),
                report_create_date DATETIME,
                report_name NVARCHAR(200) UNIQUE,
                report_recipients_email NVARCHAR(MAX),
                report_sender_type NVARCHAR(30),
                report_on_monitoring NVARCHAR(4) DEFAULT NULL CHECK (report_on_monitoring = 'true'),
                task_name NVARCHAR(50) UNIQUE,
                task_last_run_date DATETIME,
                task_last_run_result NVARCHAR(20),
                task_author_login NVARCHAR(30),
                task_file_path NVARCHAR(150),
                task_file_name NVARCHAR(50),
                task_description NVARCHAR(MAX),
                task_status NVARCHAR(15),
                task_run_as_user NVARCHAR(30),
                task_schedule_type NVARCHAR(30),
                task_schedule_days NVARCHAR(40),
                task_schedule_start_time TIME,
                task_schedule_months NVARCHAR(40),
                task_schedule_repeat_every NVARCHAR(70),
                task_schedule_repeat_until_time NVARCHAR(70),
                task_schedule_repeat_until_duration NVARCHAR(70),
                additional_info NVARCHAR(MAX),
                database_type NVARCHAR(20),
                database_hostname NVARCHAR(20)
            )
        '''
    execute_mssql_query(query)


def request_id_exists_in_db(request_id: str) -> bool:
    '''
    Проверяет, существует ли request_id в БД.

    :param request_id: Номер запроса.
    :return: True, если request_id есть в БД, иначе False.
    '''
    query = 'SELECT id FROM table_name WHERE id = %s'
    params = (request_id,)
    result = execute_mssql_query_read(query, params)
    return len(result) > 0


def update_item(data: dict[str, str]) -> None:
    '''
    Обновляет информацию об автоотчете в БД.

    :param data: Словарь с данными.
    '''
    if 'request_id' not in data:
        handle_error('Ключ - request_id отсутствует в данных', 'warning')
        return

    query = '''
                UPDATE table_name SET
                    report_create_date = %s,
                    report_name = %s,
                    report_recipients_email = %s,
                    report_sender_type = %s,
                    task_name = %s,
                    task_last_run_date = %s,
                    task_last_run_result = %s,
                    task_author_login = %s,
                    task_file_path = %s,
                    task_file_name = %s,
                    task_description = %s,
                    task_status = %s,
                    task_run_as_user = %s,
                    task_schedule_type = %s,
                    task_schedule_days = %s,
                    task_schedule_start_time = %s,
                    task_schedule_months = %s,
                    task_schedule_repeat_every = %s,
                    task_schedule_repeat_until_time = %s,
                    task_schedule_repeat_until_duration = %s,
                    database_type = %s,
                    database_hostname = %s
                WHERE request_id = %s
            '''
    params = (
        data.get('report_create_date', None),
        data.get('theme', None),
        data.get('emails', None),
        data.get('sender_type', None),
        data['task_name'],
        data.get('task_last_run_date', None),
        data.get('task_last_run_result', None),
        data.get('task_author_login', None),
        data.get('task_file_path', None),
        data.get('task_file_name', None),
        data.get('task_description', None),
        data.get('task_status', None),
        data.get('task_run_as_user', None),
        data.get('task_schedule_type', None),
        data.get('task_schedule_days', None),
        data.get('task_schedule_start_time', None),
        data.get('task_schedule_months', None),
        data.get('task_schedule_repeat_every', None),
        data.get('task_schedule_repeat_until_time', None),
        data.get('task_schedule_repeat_until_duration', None),
        data.get('database_type', None),
        data.get('database_hostname', None),
        data['task_name']
            )
    execute_mssql_query(query, params)


def update_tasks_in_db(tasks: list[dict[str, str]]) -> None:
    """
    Обновляет информацию об автоотчетах в БД.

    :param tasks: Список словарей с данными.
    """
    for data in tasks:
        try:
            update_item(data)
        except Exception:
            continue


def get_main_data_from_db(request_id: str) -> list[dict[str, str]]:
    '''
    Получает данные из другой базы данных.
    Возвращает список словарей с результатами.

    :param request_id: Номер запроса в формате RA длиной 15 символов.
    :return: Список словарей с данными о запросе(ах).
    '''
    query = '''
        select
            columns
        from table_name
        where id = :request_id
        '''
    result = execute_oracle_query(query, [request_id,])
    return result


def insert_new_item_in_db(data_list: list[dict[str, str]]) -> None:
    '''
    Добавляет базовую информацию о новом автоотчете.

    :param data: Словарь с данными.
    '''
    if data_list:
        for data in data_list:
            query = '''
                INSERT INTO table_name (
                        request_id,
                        customer_login,
                        customer_name,
                        customer_company,
                        customer_orgstructure,
                        report_create_date,
                        report_name,
                        report_recipients_email,
                        report_sender_type,
                        task_name,
                        task_last_run_date,
                        task_last_run_result,
                        task_author_login,
                        task_file_path,
                        task_file_name,
                        task_description,
                        task_status,
                        task_run_as_user,
                        task_schedule_type,
                        task_schedule_days,
                        task_schedule_start_time,
                        task_schedule_months,
                        task_schedule_repeat_every,
                        task_schedule_repeat_until_time,
                        task_schedule_repeat_until_duration,
                        database_type,
                        database_hostname
                    )
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                    '''
            params = (
                data.get('request_id', None),
                data.get('customer_login', None),
                data.get('customer_name', None),
                data.get('customer_company', None),
                data.get('customer_orgstructure', None),
                data.get('report_create_date', None),
                data.get('theme', None),
                data.get('emails', None),
                data.get('sender_type', None),
                data.get('task_name', None),
                data.get('task_last_run_date', None),
                data.get('task_last_run_result', None),
                data.get('task_author_login', None),
                data.get('task_file_path', None),
                data.get('task_file_name', None),
                data.get('task_description', None),
                data.get('task_status', None),
                data.get('task_run_as_user', None),
                data.get('task_schedule_type', None),
                data.get('task_schedule_days', None),
                data.get('task_schedule_start_time', None),
                data.get('task_schedule_months', None),
                data.get('task_schedule_repeat_every', None),
                data.get('task_schedule_repeat_until_time', None),
                data.get('task_schedule_repeat_until_duration', None),
                data.get('database_type', None),
                data.get('database_hostname', None)
                )
            try:
                execute_mssql_query(query, params)
            except Exception:
                continue
