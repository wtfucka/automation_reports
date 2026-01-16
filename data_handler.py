from pathlib import Path
from typing import Any, Optional, Sequence

from data_processor import (
    merge_with_existing_data,
    process_cmd_files,
    process_log_files,
    process_main_data,
    SchedulerDataProcessor
    )
from database_handler import DatabaseConnector
from scheduler_handler import TaskSchedulerManager
from utils import find_new_folders_not_in_db, path_find_walker


connector = DatabaseConnector()
scheduler_manager = TaskSchedulerManager()
scheduler_processor = SchedulerDataProcessor()


def collect_primary_data(
        root_paths: Sequence[Path | str],
        main_data_list: list[dict[str, Any]],
        tasks_data_list: list[dict[str, Any]],
        report_mails_theme: list[dict[str, Any]],
        log_data_list: Optional[list[dict[str, Any]]] = None) -> None:
    '''
    Собирает первичные данные из различных источников.
    OPTIMIZED: Uses batch Oracle and PostgreSQL queries.
    '''
    request_ids = [Path(root_path).name for root_path in root_paths]

    # собираем основные данные по сущности из 2-х БД
    oracle_data_map = connector.get_main_data_from_db_oracle_batch(
        request_ids
    )
    postgre_data_map = connector.get_main_data_from_db_postgre_batch(
        request_ids
    )

    for root_path in root_paths:
        path = Path(root_path)
        request_id = path.name
        task_params = scheduler_manager.parse_schedule_params(
            str(path),
            request_id
        )
        if (task_params and isinstance(task_params, dict)
                and scheduler_manager.check_required_params(task_params)):
            scheduler_manager.create_or_update_task(**task_params)  # type: ignore  # noqa
        report_mails_theme.append(process_cmd_files(str(path)))

        postgre_data = postgre_data_map.get(request_id)
        if postgre_data:
            main_data_list.append(postgre_data)
        oracle_data = oracle_data_map.get(request_id)
        if oracle_data:
            main_data_list.append(oracle_data)
        tasks_data_list.append(scheduler_manager.get_task_info(request_id))
        if log_data_list is not None:
            log_data_list.append(process_log_files(str(path)))


def update_data() -> None:
    '''
    Запускает процесс обновления данных в БД.
    '''
    main_data_list: list[dict[str, Any]] = []
    tasks_data_list: list[dict[str, Any]] = []
    report_mails_theme: list[dict[str, Any]] = []
    log_data_list: list[dict[str, Any]] = []

    root_paths = path_find_walker('walk')
    collect_primary_data(
        root_paths,
        main_data_list,
        tasks_data_list,
        report_mails_theme,
        log_data_list
    )

    processed_tasks_data = scheduler_processor.task_data_processing(
        tasks_data_list
    )
    processed_main_data = process_main_data(main_data_list)

    all_data = merge_with_existing_data(processed_main_data, processed_tasks_data)  # noqa
    all_data = merge_with_existing_data(all_data, log_data_list)
    all_data = merge_with_existing_data(all_data, report_mails_theme)

    connector.update_tasks_in_db(all_data)


def insert_data(request_id_list: Optional[set[str]] = None) -> None:
    '''
    Запускает процесс добавления новой строки в БД.
    OPTIMIZED: Uses batch request_id existence check.

    :param request_id_list: Список номеров запросов формата REQ/RA
                            длинной 15 символов.
    '''
    main_data_list: list[dict[str, Any]] = []
    tasks_data_list: list[dict[str, Any]] = []
    report_mails_theme: list[dict[str, Any]] = []

    if request_id_list:
        valid_request_ids = [
            req_id for req_id in request_id_list
            if req_id.startswith('entity_name')
        ]

        exists_map = connector.request_ids_exist_in_db(valid_request_ids)

        root_paths = []
        for request_id in valid_request_ids:
            if not exists_map.get(request_id, False):
                found_paths = path_find_walker('find', request_id)
                root_paths.extend(found_paths)
        if root_paths:
            collect_primary_data(
                root_paths,
                main_data_list,
                tasks_data_list,
                report_mails_theme
            )
    else:
        new_folders = find_new_folders_not_in_db()
        collect_primary_data(
            new_folders,
            main_data_list,
            tasks_data_list,
            report_mails_theme
        )

    if main_data_list:
        processed_main_data = process_main_data(main_data_list)
        processed_tasks_data = scheduler_processor.task_data_processing(
            tasks_data_list
        )

        all_data = merge_with_existing_data(
            processed_main_data,
            processed_tasks_data
        )
        all_data = merge_with_existing_data(all_data, report_mails_theme)

        connector.insert_new_item_in_db(all_data)
