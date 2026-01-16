import oracledb
import psycopg2
import pymssql
from typing import Any

from error_handler import handle_error


class DatabaseConnector:
    def __init__(self, config_file: str = 'file_name.json') -> None:
        from utils import load_configs

        self.configs: dict[str, Any] = load_configs(config_file)
        self.mssql_config: dict[str, Any] = self.configs.get('MSSQL_DB_NAME', {})  # noqa
        self.oracle_config: dict[str, Any] = self.configs.get('ORACLE_DB_NAME', {})  # noqa
        self.postgre_config: dict[str, Any] = self.configs.get('POSTGRE_DB_NAME', {})  # noqa

        # Connection pool - переиспользование открытых подключений
        self._mssql_connection = None
        self._oracle_connection = None
        self._postgre_connection = None
        self._oracle_initialized = False

    def _get_mssql_connection(self):
        '''
        Получает или создает MSSQL соединение (connection pooling).

        :return: Активное соединение с MSSQL.
        '''
        if self._mssql_connection is None:
            try:
                self._mssql_connection = pymssql.connect(
                    host=self.mssql_config['host'],
                    database=self.mssql_config['database'],
                    user=self.mssql_config['user'],
                    password=self.mssql_config['password']
                )
            except Exception as error:
                handle_error(
                    f'Ошибка подключения к MSSQL: {error}',
                    'critical'
                )
                raise
        return self._mssql_connection

    def _get_oracle_connection(self):
        '''
        Получает или создает Oracle соединение (connection pooling).

        :return: Активное соединение с Oracle.
        '''
        if self._oracle_connection is None:
            try:
                if not self._oracle_initialized:
                    oracledb.init_oracle_client()
                    self._oracle_initialized = True
                self._oracle_connection = oracledb.connect(
                    **self.oracle_config
                )
            except Exception as error:
                handle_error(
                    f'Ошибка подключения к Oracle: {error}',
                    'critical'
                )
                raise
        return self._oracle_connection

    def _get_postgre_connection(self):
        '''
        Получает или создает PostgreSQL соединение (connection pooling).

        :return: Активное соединение с PostgreSQL.
        '''
        if self._postgre_connection is None:
            try:
                self._postgre_connection = psycopg2.connect(
                    **self.postgre_config
                )
            except Exception as error:
                handle_error(
                    f'Ошибка подключения к PostgreSQL: {error}',
                    'critical'
                )
                raise
        return self._postgre_connection

    def close_connections(self):
        '''
        Закрывает все открытые соединения.
        Вызывается в конце работы скрипта.
        '''
        if self._mssql_connection:
            try:
                self._mssql_connection.close()
            except Exception:
                pass
            self._mssql_connection = None

        if self._oracle_connection:
            try:
                self._oracle_connection.close()
            except Exception:
                pass
            self._oracle_connection = None

        if self._postgre_connection:
            try:
                self._postgre_connection.close()
            except Exception:
                pass
            self._postgre_connection = None

    def __del__(self):
        '''
        Автоматически закрывает соединения при удалении объекта.
        '''
        self.close_connections()

    def execute_mssql_query_read(self,
                                 query: str,
                                 params: tuple = ()) -> list[dict[str, str]]:
        '''
        Чтение данных из БД MSSQL.
        Используем connection pooling.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список со словарем с полученными данными.
        '''
        try:
            connection = self._get_mssql_connection()
            cursor = connection.cursor(as_dict=True)
            cursor.execute(query, params)
            return cursor.fetchall()  # type: ignore
        except (pymssql.IntegrityError, pymssql.DatabaseError) as error:
            handle_error(f'Ошибка выполнения MSSQL запроса - чтения: {error}',
                         'critical')
            return []

    def execute_mssql_query(
            self,
            query: str,
            params: tuple | list[tuple] = ()  # type: ignore
    ) -> None:
        '''
        Выполняет запрос к MS SQL. Поддерживает как одиночные,
        так и batch операции.
        Используем connection pooling.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса (tuple для одиночного,
                       list[tuple] для batch).
        '''
        try:
            connection = self._get_mssql_connection()
            with connection.cursor() as cursor:
                try:
                    # Определяем batch или single операцию
                    if isinstance(params, list) and params:
                        cursor.executemany(query, params)  # type: ignore
                    else:
                        cursor.execute(query, params)
                    connection.commit()
                except pymssql.DatabaseError as error:
                    connection.rollback()
                    params_info = (
                        f"batch {len(params)} records"
                        if isinstance(params, list)
                        else str(params)
                    )
                    handle_error(
                        f'Ошибка выполнения MSSQL запроса: '
                        f'{query[:100]}. '
                        f'С параметрами: {params_info}. '
                        f'Ошибка: {error}',
                        'critical')
        except Exception as error:
            handle_error(f'Ошибка выполнения MSSQL запроса: {error}.',
                         'critical')

    def execute_oracle_query(
            self,
            query: str,
            params: tuple | None = None
            ) -> list[dict[str, str]]:
        '''
        Выполняет запрос к Oracle.
        Используем connection pooling.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список словарей с полученными данными.
        '''
        if params is None:
            params = ()

        try:
            connection = self._get_oracle_connection()
            cursor = connection.cursor()
            cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]  # type: ignore   # noqa
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except oracledb.DatabaseError as error:
            handle_error(f'Ошибка выполнения Oracle запроса: {error}',
                         'critical')
            return []

    def execute_postgre_query(
            self,
            query: str,
            params: tuple | None = None
            ) -> list[dict[str, str]]:
        '''
        Выполняет запрос к PostgreSQL.
        Используем connection pooling.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список словарей с полученными данными.
        '''
        if params is None:
            params = ()

        try:
            connection = self._get_postgre_connection()
            cursor = connection.cursor()
            cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except (psycopg2.IntegrityError, psycopg2.DatabaseError) as error:
            handle_error(f'Ошибка выполнения Postgre запроса: {error}',
                         'critical')
            return []

    def initialize_db(self) -> None:
        '''
        Создает таблицу, если она не существует с нужными полями.
        '''
        query = '''
            IF NOT EXISTS (
                SELECT * FROM sysobjects
                WHERE name='autoreports' AND xtype='U'
            )
            CREATE TABLE autoreports (
                id INT IDENTITY(1,1) PRIMARY KEY,
                last_update_date datetime,
                request_id NVARCHAR(50) UNIQUE,
                customer_login NVARCHAR(30),
                customer_name NVARCHAR(150),
                customer_company NVARCHAR(200),
                customer_orgstructure NVARCHAR(MAX),
                receiver_login NVARCHAR(30),
                receiver_name NVARCHAR(150),
                receiver_company NVARCHAR(200),
                receiver_orgstructure NVARCHAR(MAX),
                report_create_date DATETIME,
                report_name NVARCHAR(200) UNIQUE,
                report_recipients_email NVARCHAR(MAX),
                report_sender_type NVARCHAR(30),
                report_on_monitoring BIT DEFAULT NULL,
                report_in_archive BIT DEFAULT NULL,
                report_archive_folder NVARCHAR(150),
                last_send_date DATETIME,
                last_send_status NVARCHAR(30),
                last_send_recipients NVARCHAR(MAX),
                last_send_attachments NVARCHAR(MAX),
                last_send_issue NVARCHAR(MAX),
                last_send_error NVARCHAR(MAX),
                task_name NVARCHAR(50) UNIQUE,
                task_last_run_date DATETIME,
                task_last_run_result NVARCHAR(25),
                task_author_login NVARCHAR(30),
                task_file_path NVARCHAR(1000),
                task_file_name NVARCHAR(150),
                task_description NVARCHAR(MAX),
                task_status NVARCHAR(15),
                task_run_as_user NVARCHAR(30),
                task_trigger_is_active NVARCHAR(10),
                task_schedule_type NVARCHAR(2000),
                task_schedule_start_date NVARCHAR(2000),
                task_schedule_days_interval NVARCHAR(2000),
                task_schedule_weeks_interval NVARCHAR(2000),
                task_schedule_week_days NVARCHAR(2000),
                task_schedule_months NVARCHAR(2000),
                task_schedule_month_days NVARCHAR(2000),
                task_schedule_repeat_every NVARCHAR(2000),
                task_schedule_repeat_until_time NVARCHAR(2000),
                task_schedule_repeat_until_duration NVARCHAR(2000),
                additional_info NVARCHAR(MAX),
                database_type NVARCHAR(50),
                database_hostname NVARCHAR(50)
            )
        '''
        self.execute_mssql_query(query)

    def request_id_exists_in_db(self, request_id: str) -> bool:
        '''
        Проверяет, существует ли request_id в БД.

        :param request_id: Номер запроса.
        :return: True, если request_id есть в БД, иначе False.
        '''
        query = 'SELECT request_id FROM dbo.autoreports WHERE request_id = %s'
        params = (request_id,)
        result = self.execute_mssql_query_read(query, params)
        return len(result) > 0

    def update_tasks_in_db(self, tasks: list[dict[str, str]]) -> None:
        '''
        Обновляет информацию об автоотчетах в БД используя batch операцию.

        :param tasks: Список словарей с данными.
        '''
        if not tasks:
            return

        query = '''
            UPDATE dbo.autoreports SET
                report_create_date = %s,
                report_name = %s,
                report_recipients_email = %s,
                report_sender_type = %s,
                report_in_archive = %s,
                report_archive_folder = %s,
                last_send_date = %s,
                last_send_status = %s,
                last_send_recipients = %s,
                last_send_attachments = %s,
                last_send_issue = %s,
                last_send_error = %s,
                task_name = %s,
                task_last_run_date = %s,
                task_last_run_result = %s,
                task_author_login = %s,
                task_file_path = %s,
                task_file_name = %s,
                task_description = %s,
                task_status = %s,
                task_run_as_user = %s,
                task_trigger_is_active = %s,
                task_schedule_type = %s,
                task_schedule_start_date = %s,
                task_schedule_days_interval = %s,
                task_schedule_weeks_interval = %s,
                task_schedule_week_days = %s,
                task_schedule_months = %s,
                task_schedule_month_days = %s,
                task_schedule_repeat_every = %s,
                task_schedule_repeat_until_time = %s,
                task_schedule_repeat_until_duration = %s,
                database_type = %s,
                database_hostname = %s
            WHERE request_id = %s
        '''

        # Подготовка параметров для массового выполнения
        params_list = [
            (
                data.get('report_create_date', None),
                data.get('theme', None),
                data.get('emails', None),
                data.get('sender_type', None),
                data.get('report_in_archive', None),
                data.get('report_archive_folder', None),
                data.get('last_send_date', None),
                data.get('last_send_status', None),
                data.get('last_send_recipients', None),
                data.get('last_send_attachments', None),
                data.get('last_send_issue', None),
                data.get('last_send_error', None),
                data.get('task_name'),
                data.get('task_last_run_date', None),
                data.get('task_last_run_result', None),
                data.get('task_author_login', None),
                data.get('task_file_path', None),
                data.get('task_file_name', None),
                data.get('task_description', None),
                data.get('task_status', None),
                data.get('task_run_as_user', None),
                data.get('task_trigger_status', None),
                data.get('task_schedule_type', None),
                data.get('task_schedule_start_date', None),
                data.get('task_schedule_days_interval', None),
                data.get('task_schedule_weeks_interval', None),
                data.get('task_schedule_week_days', None),
                data.get('task_schedule_months', None),
                data.get('task_schedule_month_days', None),
                data.get('task_schedule_repeat_every', None),
                data.get('task_schedule_repeat_until_time', None),
                data.get('task_schedule_repeat_until_duration', None),
                data.get('database_type', None),
                data.get('database_hostname', None),
                data.get('task_name')
            )
            for data in tasks
        ]

        self.execute_mssql_query(query, params_list)

    def get_main_data_from_db_oracle(
            self,
            request_id: str
            ) -> dict[str, str]:
        '''
        Получает данные из другой базы данных.
        Возвращает словарь с результатами.

        :param request_id: Номер запроса в формате RA длиной 15 символов.
        :return: Словарь с данными о запросе.
        '''
        query = '''
            select
                request_id,
                customer_login,
                customer_name,
                customer_company,
                customer_orgstructure,
                receiver_login,
                receiver_name,
                receiver_company,
                receiver_orgstructure,
                report_create_date
            from table_name
            where request_id = :request_id
            '''
        result = self.execute_oracle_query(query, (request_id,))
        if result:
            return result[0]
        return {}

    def get_main_data_from_db_postgre(
            self,
            request_id: str
            ) -> dict[str, str]:
        '''
        Метод запроса данных из БД PostGre.

        :param request_id: Номер запроса в формате REQ/RA длиной 15 символов.
        :return: Словарь с данными о запросе.
        '''
        query = '''
            select
                request_id,
                customer_login,
                customer_name,
                customer_company,
                customer_orgstructure,
                receiver_login,
                receiver_name,
                receiver_company,
                receiver_orgstructure,
                report_create_date
            from table_name
            where request_id = %s
            '''
        result = self.execute_postgre_query(query, (request_id,))
        if result:
            return result[0]
        return {}

    def request_ids_exist_in_db(
            self,
            request_ids: list[str]
    ) -> dict[str, bool]:
        '''
        Проверяет существование нескольких request_id в БД за один запрос.
        BATCH OPTIMIZATION: N queries → 1 query

        :param request_ids: Список номеров запросов.
        :return: Словарь {request_id: exists_bool}
        '''
        if not request_ids:
            return {}

        # Строим динамический запрос
        placeholders = ', '.join(['%s'] * len(request_ids))
        query = f'''
            SELECT request_id FROM dbo.autoreports
            WHERE request_id IN ({placeholders})
        '''

        result = self.execute_mssql_query_read(query, tuple(request_ids))
        existing_ids = {row['request_id'] for row in result}

        return {req_id: req_id in existing_ids for req_id in request_ids}

    def get_main_data_from_db_oracle_batch(
            self,
            request_ids: list[str]
    ) -> dict[str, dict[str, str]]:
        '''
        Получает данные из Oracle для нескольких request_id за один запрос.
        BATCH OPTIMIZATION: N queries → 1 query

        :param request_ids: Список номеров запросов.
        :return: Словарь {request_id: data_dict}
        '''
        if not request_ids:
            return {}

        # Создаем placeholder'ы для запроса в oracle
        placeholders = ', '.join(
            [f':{i+1}' for i in range(len(request_ids))]
        )

        query = f'''
            select
                request_id,
                customer_login,
                customer_name,
                customer_company,
                customer_orgstructure,
                receiver_login,
                receiver_name,
                receiver_company,
                receiver_orgstructure,
                report_create_date
            from table_name
            where request_id IN ({placeholders})
        '''

        result = self.execute_oracle_query(query, tuple(request_ids))

        # Конвертируем список в словарь данных
        return {row['request_id']: row for row in result}

    def get_main_data_from_db_postgre_batch(
            self,
            request_ids: list[str]
    ) -> dict[str, dict[str, str]]:
        '''
        Получает данные из PostgreSQL для нескольких request_id
        за один запрос.
        BATCH OPTIMIZATION: N queries → 1 query

        :param request_ids: Список номеров запросов.
        :return: Словарь {request_id: data_dict}
        '''
        if not request_ids:
            return {}

        placeholders = ', '.join(['%s'] * len(request_ids))

        query = f'''
            select
                request_id,
                customer_login,
                customer_name,
                customer_company,
                customer_orgstructure,
                receiver_login,
                receiver_name,
                receiver_company,
                receiver_orgstructure,
                report_create_date
            from table_name
            where request_id IN ({placeholders})
        '''

        result = self.execute_postgre_query(query, tuple(request_ids))

        # Конвертируем список в словарь данных
        return {row['request_id']: row for row in result}

    def insert_new_item_in_db(
            self,
            data_list: list[dict[str, str]]
    ) -> None:
        '''
        Добавляет новые автоотчеты в БД используя batch операцию.

        :param data_list: Список словарей с данными.
        '''
        if not data_list:
            return

        query = '''
            INSERT INTO dbo.autoreports (
                request_id,
                customer_login,
                customer_name,
                customer_company,
                customer_orgstructure,
                receiver_login,
                receiver_name,
                receiver_company,
                receiver_orgstructure,
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
                task_trigger_is_active,
                task_schedule_type,
                task_schedule_start_date,
                task_schedule_days_interval,
                task_schedule_weeks_interval,
                task_schedule_week_days,
                task_schedule_months,
                task_schedule_month_days,
                task_schedule_repeat_every,
                task_schedule_repeat_until_time,
                task_schedule_repeat_until_duration,
                database_type,
                database_hostname
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
        '''

        # Подготовка параметров для массового выполнения
        params_list = [
            (
                data.get('request_id', None),
                data.get('customer_login', None),
                data.get('customer_name', None),
                data.get('customer_company', None),
                data.get('customer_orgstructure', None),
                data.get('receiver_login', None),
                data.get('receiver_name', None),
                data.get('receiver_company', None),
                data.get('receiver_orgstructure', None),
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
                data.get('task_trigger_status', None),
                data.get('task_schedule_type', None),
                data.get('task_schedule_start_date', None),
                data.get('task_schedule_days_interval', None),
                data.get('task_schedule_weeks_interval', None),
                data.get('task_schedule_week_days', None),
                data.get('task_schedule_months', None),
                data.get('task_schedule_month_days', None),
                data.get('task_schedule_repeat_every', None),
                data.get('task_schedule_repeat_until_time', None),
                data.get('task_schedule_repeat_until_duration', None),
                data.get('database_type', None),
                data.get('database_hostname', None)
            )
            for data in data_list
        ]

        self.execute_mssql_query(query, params_list)
        new_reports = [data.get('request_id') for data in data_list if data.get('request_id')]  # noqa
        handle_error(f'Новые отчеты: {",".join(new_reports)}', 'info')  # noqa # type: ignore
