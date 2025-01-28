import oracledb
import psycopg2
import pymssql

from error_handler import handle_error


class DatabaseConnector:
    def __init__(self, config_file=...):
        self.configs = self.load_configs(config_file)
        self.mssql_config = self.configs.get(...)
        self.oracle_config = self.configs.get(...)
        self.postgre_config = self.configs.get(...)

    def load_configs(self, config_file: str) -> dict[str, str]:
        '''
        Метод возвращает словарь с данными для подклчюения к различным БД.

        :param config_file: Имя БД.
        :return: Словарь с данными для подключения.
        '''
        from json import JSONDecodeError, load
        from pathlib import Path

        configs_folder = Path().cwd()
        try:
            with open(configs_folder / config_file) as f:
                return load(f)
        except (IOError, OSError, JSONDecodeError) as error:
            handle_error(f'Ошибка чтения конфигураций: {error}', 'critical')
            return {}

    def execute_mssql_query_read(self,
                                 query: str,
                                 params: tuple = ()) -> list[dict[str, str]]:
        '''
        Чтение данных из БД MSSQL.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список со словарем с полученными данными.
        '''
        try:
            with pymssql.connect(
                host=self.mssql_config['host'],
                database=self.mssql_config['database'],
                user=self.mssql_config['user'],
                password=self.mssql_config['password']
            ) as connection:
                cursor = connection.cursor(as_dict=True)
                cursor.execute(query, params)
                return cursor.fetchall()
        except (pymssql.IntegrityError, pymssql.DatabaseError) as error:
            handle_error(f'Ошибка выполнения MSSQL запроса - чтения: {error}',
                         'critical')
            return []

    def execute_mssql_query(self, query: str, params: tuple = ()) -> None:
        '''
        Выполняет запрос к MS SQL.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        '''
        try:
            with pymssql.connect(
                host=self.mssql_config['host'],
                database=self.mssql_config['database'],
                user=self.mssql_config['user'],
                password=self.mssql_config['password']
            ) as connection:
                cursor = connection.cursor()
                cursor.execute(query, params)
                connection.commit()
        except (pymssql.IntegrityError, pymssql.DatabaseError) as error:
            handle_error(f'Ошибка выполнения MSSQL запроса - записи: {error}',
                         'critical')

    def execute_oracle_query(self,
                             query: str,
                             params: tuple = []) -> list[dict[str, str]]:
        '''
        Выполняет запрос к Oracle.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список словарей с полученными данными.
        '''
        try:
            oracledb.init_oracle_client()
            with oracledb.connect(**self.oracle_config) as connection:
                cursor = connection.cursor()
                cursor.execute(query, params)
                columns = [col[0].lower() for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except (oracledb.IntegrityError, oracledb.DatabaseError) as error:
            handle_error(f'Ошибка выполнения Oracle запроса: {error}',
                         'critical')
            return []

    def execute_postgre_query(self,
                              query: str,
                              params: tuple = []) -> list[dict[str, str]]:
        '''
        Выполняет запрос к PostgreSQL.

        :param query: Текст SQL-запроса.
        :param params: Параметры для запроса.
        :return: Список словарей с полученными данными.
        '''
        try:
            with psycopg2.connect(**self.postgre_config) as connection:
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
        Создает таблицу, если она не существует, с нужными полями.
        '''
        query = '''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='some_table' AND xtype='U')
            CREATE TABLE some_table (
                id INT IDENTITY(1,1) PRIMARY KEY,
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
                task_name NVARCHAR(50) UNIQUE,
                task_last_run_date DATETIME,
                task_last_run_result NVARCHAR(25),
                task_author_login NVARCHAR(30),
                task_file_path NVARCHAR(150),
                task_file_name NVARCHAR(50),
                task_description NVARCHAR(MAX),
                task_status NVARCHAR(15),
                task_run_as_user NVARCHAR(30),
                task_trigger_is_active NVARCHAR(10),
                task_schedule_type NVARCHAR(100),
                task_schedule_start_date NVARCHAR(200),
                task_schedule_days_interval NVARCHAR(60),
                task_schedule_weeks_interval NVARCHAR(60),
                task_schedule_week_days NVARCHAR(150),
                task_schedule_months NVARCHAR(200),
                task_schedule_month_days NVARCHAR(100),
                task_schedule_repeat_every NVARCHAR(150),
                task_schedule_repeat_until_time NVARCHAR(150),
                task_schedule_repeat_until_duration NVARCHAR(150),
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
        query = 'SELECT request_id FROM some_table WHERE request_id = %s'
        params = (request_id,)
        result = self.execute_mssql_query_read(query, params)
        return len(result) > 0

    def update_item(self, data: dict[str, str]) -> None:
        '''
        Обновляет информацию об автоотчете в БД.

        :param data: Словарь с данными.
        '''
        if 'request_id' not in data:
            handle_error('Ключ - request_id отсутствует в данных', 'warning')
            return

        query = '''
                    UPDATE some_table SET
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
            data['task_name']
                )
        self.execute_mssql_query(query, params)

    def update_tasks_in_db(self, tasks: list[dict[str, str]]) -> None:
        '''
        Запускает обновление информации об автоотчетах в БД.

        :param tasks: Список словарей с данными.
        '''
        for data in tasks:
            try:
                self.update_item(data)
            except Exception as e:
                handle_error(f'update_tasks_error: {e}', 'warning')
                continue

    def get_main_data_from_db_oracle(self,
                                     request_id: str) -> list[dict[str, str]]:
        '''
        Получает данные из другой базы данных.
        Возвращает список словарей с результатами.

        :param request_id: Номер запроса в формате RA длиной 15 символов.
        :return: Список словарей с данными о запросе(ах).
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
            from some_table
            where request_id = :request_id
            '''
        return self.execute_oracle_query(query, [request_id,])[0]

    def get_main_data_from_db_postgre(self,
                                      request_id: str) -> list[dict[str, str]]:
        '''
        Метод запроса данных из БД PostGre.

        :param request_id: Номер запроса в формате REQ/RA длиной 15 символов.
        :return: Список словарей с данными о запросе.
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
            from some_table
            where request_id = :request_id
            '''
        return self.execute_postgre_query(query, [request_id,])

    def insert_new_item_in_db(self,
                              data_list: list[dict[str, str]]) -> None:
        '''
        Добавляет новый автоотчет и информацию о нём.

        :param data: Словарь с данными.
        '''
        if data_list:
            for data in data_list:
                query = '''
                    INSERT INTO some_table (
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
                params = (
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
                    data.get('task_trigger_state', None),
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
                try:
                    self.execute_mssql_query(query, params)
                except Exception as e:
                    handle_error(f'insert_task_error: {e}', 'warning')
                    continue
