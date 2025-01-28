from pathlib import Path

from data_processor import (
    merge_with_existing_data,
    process_cmd_files,
    proccess_main_data,
    SchedulerDataProcessor
    )
from database_handler import DatabaseConnector
from scheduler_handler import TaskSchedulerManager
from utils import find_new_folders_not_in_db, path_find_walker


connector = DatabaseConnector()
scheduler_manager = TaskSchedulerManager()
scheduler_processor = SchedulerDataProcessor()


def collect_primary_data(
        root_paths: list[Path],
        main_data_list: list[dict[str, str]],
        tasks_data_list: list[dict[str, str]],
        report_mails_theme: list[dict[str, str]]) -> None:
    for root_path in root_paths:
        path = Path(root_path)
        request_id = path.name
        task_params = scheduler_manager.parse_schedule_params(path, request_id)
        if task_params:
            scheduler_manager.create_or_update_task(**task_params)
        report_mails_theme.append(process_cmd_files(path))
        main_data_list.append(connector.get_main_data_from_db_oracle(request_id))  # noqa
        tasks_data_list.append(scheduler_manager.get_task_info(request_id))


def update_data() -> None:
    '''
    Запускает процесс обновления данных в БД.
    '''
    request_main_data_list, schedule_tasks_data_list, report_mails_theme = [], [], []  # noqa
    root_paths = path_find_walker('walk')
    collect_primary_data(root_paths, request_main_data_list, schedule_tasks_data_list, report_mails_theme)  # noqa
    processed_tasks_data = scheduler_processor.task_data_processing(schedule_tasks_data_list)  # noqa
    processed_main_data = proccess_main_data(request_main_data_list)
    main_task_data = merge_with_existing_data(
        processed_main_data,
        processed_tasks_data
    )
    all_data = merge_with_existing_data(
        main_task_data,
        report_mails_theme
        )
    connector.update_tasks_in_db(all_data)


def insert_data(request_id_list: list[str] = None) -> None:
    '''
    Запускает процесс добавления новой строки в БД.

    :param request_id: Номер запроса формата REQ/RA длинной 15 символов.
    '''
    request_main_data_list, schedule_tasks_data_list, report_mails_theme = [], [], []  # noqa

    if request_id_list and '' not in request_id_list:
        for request_id in request_id_list:
            if request_id.startswith(...) and not connector.request_id_exists_in_db(request_id):  # noqa
                root_paths = path_find_walker('find', request_id)
                collect_primary_data(root_paths, request_main_data_list, schedule_tasks_data_list, report_mails_theme)  # noqa
    else:
        new_folders = find_new_folders_not_in_db()
        collect_primary_data(new_folders, request_main_data_list, schedule_tasks_data_list, report_mails_theme)  # noqa

    if request_main_data_list:
        processed_main_data = proccess_main_data(request_main_data_list)
        processed_task_data = scheduler_processor.task_data_processing(schedule_tasks_data_list)  # noqa
        main_task_data = merge_with_existing_data(
            processed_main_data,
            processed_task_data
        )
        all_data = merge_with_existing_data(
            main_task_data,
            report_mails_theme
            )

        connector.insert_new_item_in_db(all_data)
