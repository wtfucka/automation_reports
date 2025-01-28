from data_handler import insert_data, update_data
from error_handler import handle_error
from utils import parse_arguments


def main():
    tasks = parse_arguments()

    if 'daily_update' in tasks:
        try:
            update_data()
        except Exception as e:
            handle_error(f'Ошибка в update_data: {e}', 'critical')

    if tasks and 'daily_update' not in tasks:
        try:
            insert_data(tasks)
        except Exception as e:
            handle_error(f'Ошибка в insert_data: {e}', 'critical')


if __name__ == '__main__':
    main()
