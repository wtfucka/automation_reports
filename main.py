from __future__ import annotations

import time
from argparse import ArgumentParser
from datetime import datetime

from container import Container

# Константы (были в constants.py)
ROOT_REPORT_PATHS = ['path_name']
DATABASE_TYPE = {'db_name': 'postgre'}
LOG_FILE_NAME = 'log.log'


def parse_arguments() -> set[str]:
    parser = ArgumentParser(description='Обработка данных.')
    parser.add_argument(
        '--tasks', type=str, default='',
        help='Список задач через запятую',
    )
    args = parser.parse_args()
    return set(args.tasks.split(',')) if args.tasks else set()


def main() -> None:
    start = time.time()
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]

    # Сборка зависимостей
    container = Container(
        root_report_paths=ROOT_REPORT_PATHS,
        database_type_map=DATABASE_TYPE,
        log_file_name=LOG_FILE_NAME,
    )

    notifier = container.error_notifier
    notifier.notify(f'{"-" * 60}', 'info')
    notifier.notify(f'Сервис запущен: {start_dt}', 'info')

    tasks = parse_arguments()

    try:
        if 'daily_update' in tasks:
            try:
                uc = container.update_data_use_case()
                uc.execute()
            except Exception as e:
                notifier.notify(
                    f'Ошибка update_data: {e}', 'critical'
                )

        if tasks and 'daily_update' not in tasks:
            try:
                uc = container.insert_data_use_case()
                uc.execute(tasks.discard('find'))  # type: ignore
            except Exception as e:
                notifier.notify(
                    f'Ошибка insert_data: {e}', 'critical'
                )
    finally:
        try:
            container.close()
        except Exception as e:
            notifier.notify(
                f'Ошибка закрытия соединений: {e}', 'error'
            )

        # Отправка email при ошибках
        container.email_sender.send_error_report()

        # Логирование времени
        end = time.time()
        end_dt = datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S,%f'
        )[:-3]
        duration = end - start
        h = int(duration // 3600)
        m = int((duration % 3600) // 60)
        s = duration % 60
        notifier.notify(f'Сервис завершён: {end_dt}', 'info')
        notifier.notify(
            f'Время выполнения: {h:02d}:{m:02d}:{s:06.3f}',
            'info',
        )
        notifier.notify(f'{"-" * 60}', 'info')


if __name__ == '__main__':
    main()
