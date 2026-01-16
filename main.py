from data_handler import insert_data, update_data, connector
from error_handler import handle_error, ERROR_TYPES
from email_notifier import send_error_report
from utils import log_execution_time, parse_arguments


@log_execution_time
def main():
    tasks: set = parse_arguments()

    try:
        if 'daily_update' in tasks:
            try:
                update_data()
            except Exception as e:
                handle_error(f'Ошибка в update_data: {e}', 'critical')

        if tasks and 'daily_update' not in tasks:
            try:
                insert_data(tasks.discard('find'))
            except Exception as e:
                handle_error(f'Ошибка в insert_data: {e}', 'critical')
    finally:
        # Закрываем все соединения с БД перед выходом
        try:
            connector.close_connections()
        except Exception as e:
            handle_error(f'Ошибка закрытия открытых соединений с БД: {e}',
                         'error')
        # Отправляем email если были ошибки
        send_error_report(ERROR_TYPES)


if __name__ == '__main__':
    main()
