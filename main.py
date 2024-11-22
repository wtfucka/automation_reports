from sys import argv

from data_handler import insert_data, update_data
from error_handler import handle_error


def main():
    INPUT_ARG = [arg for arg in argv[1].split(',')] if len(argv) > 1 else []

    if 'daily_update' in INPUT_ARG:
        try:
            update_data()
        except Exception as e:
            handle_error(f'Ошибка в update_data: {e}', 'critical')

    if INPUT_ARG and 'daily_update' not in INPUT_ARG:
        try:
            insert_data(INPUT_ARG)
        except Exception as e:
            handle_error(f'Ошибка в insert_data: {e}', 'critical')


if __name__ == '__main__':
    main()
