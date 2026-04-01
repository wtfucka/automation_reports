from __future__ import annotations

from typing import Any

from domain.ports.interfaces import (
    ErrorNotifier,
    ReportDataReader,
    ReportRepository,
)


class _MssqlConnector:
    '''Lazy-connect к MSSQL с пулом соединений.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._config = config
        self._notifier = notifier
        self._conn = None

    @property
    def connection(self):
        if self._conn is None:
            import pymssql
            try:
                self._conn = pymssql.connect(**self._config)
            except Exception as e:
                self._notifier.notify(
                    f'Ошибка подключения к MSSQL: {e}',
                    'critical',
                )
                raise
        return self._conn

    def execute_read(
        self, query: str, params: tuple = (),
    ) -> list[dict[str, str]]:
        import pymssql
        try:
            cursor = self.connection.cursor(as_dict=True)
            cursor.execute(query, params)
            return cursor.fetchall()
        except (
            pymssql.IntegrityError,
            pymssql.DatabaseError,
        ) as e:
            self._notifier.notify(
                f'MSSQL read error: {e}', 'critical'
            )
            return []

    def execute_write(
        self, query: str,
        params: tuple | list[tuple] = (),
    ) -> None:
        import pymssql
        try:
            conn = self.connection
            with conn.cursor() as cursor:
                try:
                    if isinstance(params, list) and params:
                        cursor.executemany(query, params)
                    else:
                        cursor.execute(query, params)
                    conn.commit()
                except pymssql.DatabaseError as e:
                    conn.rollback()
                    self._notifier.notify(
                        f'MSSQL write error: {e}', 'critical'
                    )
        except Exception as e:
            self._notifier.notify(
                f'MSSQL execute error: {e}', 'critical'
            )

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


class _OracleConnector:
    '''Lazy-connect к Oracle.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._config = config
        self._notifier = notifier
        self._conn = None
        self._initialized = False

    @property
    def connection(self):
        if self._conn is None:
            import oracledb
            try:
                if not self._initialized:
                    oracledb.init_oracle_client()
                    self._initialized = True
                self._conn = oracledb.connect(**self._config)
            except Exception as e:
                self._notifier.notify(
                    f'Ошибка подключения к Oracle: {e}',
                    'critical',
                )
                raise
        return self._conn

    def execute_read(
        self, query: str, params: tuple = (),
    ) -> list[dict[str, str]]:
        import oracledb
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            cols = [c[0].lower() for c in cursor.description]
            return [
                dict(zip(cols, row))
                for row in cursor.fetchall()
            ]
        except oracledb.DatabaseError as e:
            self._notifier.notify(
                f'Oracle read error: {e}', 'critical'
            )
            return []

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


class _PostgreConnector:
    '''Lazy-connect к PostgreSQL.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._config = config
        self._notifier = notifier
        self._conn = None

    @property
    def connection(self):
        if self._conn is None:
            import psycopg2
            try:
                self._conn = psycopg2.connect(**self._config)
            except Exception as e:
                self._notifier.notify(
                    f'Ошибка подключения к PostgreSQL: {e}',
                    'critical',
                )
                raise
        return self._conn

    def execute_read(
        self, query: str, params: tuple = (),
    ) -> list[dict[str, str]]:
        import psycopg2
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            cols = [c[0].lower() for c in cursor.description]
            return [
                dict(zip(cols, row))
                for row in cursor.fetchall()
            ]
        except (
            psycopg2.IntegrityError,
            psycopg2.DatabaseError,
        ) as e:
            self._notifier.notify(
                f'PostgreSQL read error: {e}', 'critical'
            )
            return []

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


_MAIN_DATA_QUERY_ORACLE = '''
    SELECT
        request_id, customer_login, customer_name,
        customer_company, customer_orgstructure,
        receiver_login, receiver_name,
        receiver_company, receiver_orgstructure,
        report_create_date
    FROM table_name
    WHERE request_id IN ({placeholders})
'''

_MAIN_DATA_QUERY_POSTGRE = '''
    SELECT
        request_id, customer_login, customer_name,
        customer_company, customer_orgstructure,
        receiver_login, receiver_name,
        receiver_company, receiver_orgstructure,
        report_create_date
    FROM table_name
    WHERE request_id IN ({placeholders})
'''


class OracleReportDataReader(ReportDataReader):
    '''Реализация ReportDataReader для Oracle.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._db = _OracleConnector(config, notifier)

    def get_main_data_batch(
        self, request_ids: list[str],
    ) -> dict[str, dict[str, str]]:
        if not request_ids:
            return {}
        placeholders = ', '.join(
            f':{i+1}' for i in range(len(request_ids))
        )
        query = _MAIN_DATA_QUERY_ORACLE.format(
            placeholders=placeholders
        )
        rows = self._db.execute_read(
            query, tuple(request_ids)
        )
        return {r['request_id']: r for r in rows}

    def close(self):
        self._db.close()


class PostgreReportDataReader(ReportDataReader):
    '''Реализация ReportDataReader для PostgreSQL.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._db = _PostgreConnector(config, notifier)

    def get_main_data_batch(
        self, request_ids: list[str],
    ) -> dict[str, dict[str, str]]:
        if not request_ids:
            return {}
        placeholders = ', '.join(
            ['%s'] * len(request_ids)
        )
        query = _MAIN_DATA_QUERY_POSTGRE.format(
            placeholders=placeholders
        )
        rows = self._db.execute_read(
            query, tuple(request_ids)
        )
        return {r['request_id']: r for r in rows}

    def close(self):
        self._db.close()


class MssqlReportRepository(ReportRepository):
    '''Реализация ReportRepository для MSSQL.'''

    def __init__(
        self, config: dict[str, Any],
        notifier: ErrorNotifier,
    ):
        self._db = _MssqlConnector(config, notifier)
        self._notifier = notifier

    def exists_batch(
        self, request_ids: list[str],
    ) -> dict[str, bool]:
        if not request_ids:
            return {}
        ph = ', '.join(['%s'] * len(request_ids))
        query = (
            'SELECT request_id FROM dbo.autoreports '
            f'WHERE request_id IN ({ph})'
        )
        rows = self._db.execute_read(
            query, tuple(request_ids)
        )
        existing = {r['request_id'] for r in rows}
        return {rid: rid in existing for rid in request_ids}

    def update_reports(
        self, data: list[dict[str, Any]],
    ) -> None:
        if not data:
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
        params_list = [
            self._to_update_tuple(d) for d in data
        ]
        self._db.execute_write(query, params_list)

    def insert_reports(
        self, data: list[dict[str, Any]],
    ) -> None:
        if not data:
            return

        query = '''
            INSERT INTO dbo.autoreports (
                request_id, customer_login, customer_name,
                customer_company, customer_orgstructure,
                receiver_login, receiver_name,
                receiver_company, receiver_orgstructure,
                report_create_date, report_name,
                report_recipients_email, report_sender_type,
                task_name, task_last_run_date,
                task_last_run_result, task_author_login,
                task_file_path, task_file_name,
                task_description, task_status,
                task_run_as_user, task_trigger_is_active,
                task_schedule_type, task_schedule_start_date,
                task_schedule_days_interval,
                task_schedule_weeks_interval,
                task_schedule_week_days, task_schedule_months,
                task_schedule_month_days,
                task_schedule_repeat_every,
                task_schedule_repeat_until_time,
                task_schedule_repeat_until_duration,
                database_type, database_hostname
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
        '''
        params_list = [
            self._to_insert_tuple(d) for d in data
        ]
        self._db.execute_write(query, params_list)

        new_ids = [
            d.get('request_id', '')
            for d in data if d.get('request_id')
        ]
        self._notifier.notify(
            f'Новые отчеты: {",".join(new_ids)}', 'info'
        )

    def initialize_schema(self) -> None:
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
        self._db.execute_write(query)

    def close(self) -> None:
        self._db.close()

    @staticmethod
    def _to_update_tuple(d: dict) -> tuple:
        return (
            d.get('report_create_date'),
            d.get('theme'),
            d.get('emails'),
            d.get('sender_type'),
            d.get('report_in_archive'),
            d.get('report_archive_folder'),
            d.get('last_send_date'),
            d.get('last_send_status'),
            d.get('last_send_recipients'),
            d.get('last_send_attachments'),
            d.get('last_send_issue'),
            d.get('last_send_error'),
            d.get('task_name'),
            d.get('task_last_run_date'),
            d.get('task_last_run_result'),
            d.get('task_author_login'),
            d.get('task_file_path'),
            d.get('task_file_name'),
            d.get('task_description'),
            d.get('task_status'),
            d.get('task_run_as_user'),
            d.get('task_trigger_status'),
            d.get('task_schedule_type'),
            d.get('task_schedule_start_date'),
            d.get('task_schedule_days_interval'),
            d.get('task_schedule_weeks_interval'),
            d.get('task_schedule_week_days'),
            d.get('task_schedule_months'),
            d.get('task_schedule_month_days'),
            d.get('task_schedule_repeat_every'),
            d.get('task_schedule_repeat_until_time'),
            d.get('task_schedule_repeat_until_duration'),
            d.get('database_type'),
            d.get('database_hostname'),
            d.get('task_name'),
        )

    @staticmethod
    def _to_insert_tuple(d: dict) -> tuple:
        return (
            d.get('request_id'),
            d.get('customer_login'),
            d.get('customer_name'),
            d.get('customer_company'),
            d.get('customer_orgstructure'),
            d.get('receiver_login'),
            d.get('receiver_name'),
            d.get('receiver_company'),
            d.get('receiver_orgstructure'),
            d.get('report_create_date'),
            d.get('theme'),
            d.get('emails'),
            d.get('sender_type'),
            d.get('task_name'),
            d.get('task_last_run_date'),
            d.get('task_last_run_result'),
            d.get('task_author_login'),
            d.get('task_file_path'),
            d.get('task_file_name'),
            d.get('task_description'),
            d.get('task_status'),
            d.get('task_run_as_user'),
            d.get('task_trigger_status'),
            d.get('task_schedule_type'),
            d.get('task_schedule_start_date'),
            d.get('task_schedule_days_interval'),
            d.get('task_schedule_weeks_interval'),
            d.get('task_schedule_week_days'),
            d.get('task_schedule_months'),
            d.get('task_schedule_month_days'),
            d.get('task_schedule_repeat_every'),
            d.get('task_schedule_repeat_until_time'),
            d.get('task_schedule_repeat_until_duration'),
            d.get('database_type'),
            d.get('database_hostname'),
        )
