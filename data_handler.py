import os
from datetime import datetime
from pathlib import Path

from constants import ROOT_REPORT_PATHS
from data_proccessing import (
    merge_with_existing_data,
    process_cmd_files,
    proccess_main_data,
    task_data_processing
    )
from database_handler import (
    get_main_data_from_db,
    insert_new_item_in_db,
    request_id_exists_in_db,
    update_tasks_in_db
    )
from scheduler_handler import get_task


def find_new_folders_not_in_db() -> list[str]:
    '''
    Ищет папки созданные текущей датой и возвращает те, которых нет в БД.

    :return: Список путей к новым папкам.
    '''
    current_date = datetime.now().date()
    new_folders = []
    for root_path in path_find_walker('find'):
        creation_time = os.path.getctime(root_path)
        creation_date = datetime.fromtimestamp(creation_time).date()

        if creation_date == current_date:
            request_id = Path(root_path).name
            if request_id.startswith('first_letters') and not request_id_exists_in_db(request_id):  # noqa
                new_folders.append(root_path)
    return new_folders


def path_find_walker(
        type: str = 'walk',
        search_folder: str = None
        ) -> list[str]:
    '''
    Собирает папки, по которым нужно пройти.

    :param type: walk - проход по корневым папкам.
                 find - поиск определенной папки или папок текущей датой.
    :param search_folder: Искомая папка формата RA, длиной 15 символов.
    :return: Список найденных директорий.
    '''
    ignored_dirs = {'!example', 'archive', 'log', 'old'}
    parent_dir = os.path.dirname(os.getcwd())
    result_paths = []

    for reports_path in ROOT_REPORT_PATHS:
        root_path = os.path.join(parent_dir, reports_path)
        for dir_name in os.listdir(root_path):
            if dir_name.lower() in ignored_dirs:
                continue
            full_path = os.path.join(root_path, dir_name)
            if os.path.isdir(full_path):
                if type == 'walk' and request_id_exists_in_db(dir_name):
                    result_paths.append(full_path)
                elif type == 'find':
                    if search_folder and dir_name == search_folder:
                        result_paths.append(full_path)
                    else:
                        result_paths.append(full_path)

    return result_paths


def update_data() -> None:
    '''
    Запускает процесс обновления данных в БД.
    '''
    report_mails_theme, main_data_list, tasks_data_list = [], [], []
    for root_path in path_find_walker('walk'):
        request_id = Path(root_path).name
        tasks_data_list.extend(get_task(request_id))
        main_data_list.extend(get_main_data_from_db(request_id))
        report_mails_theme.extend(process_cmd_files(root_path))
    proccessed_tasks_data = task_data_processing(tasks_data_list)
    proccessed_main_data = proccess_main_data(main_data_list)
    main_task_data = merge_with_existing_data(
        proccessed_main_data,
        proccessed_tasks_data
    )
    all_data = merge_with_existing_data(
        main_task_data,
        report_mails_theme
        )
    update_tasks_in_db(all_data)


def insert_data(request_id_list: list[str] = None) -> None:
    '''
    Запускает процесс добавления новой строки.

    :param request_id: Номер запроса формата RA длинной 15 символов.
    '''
    main_data_list, tasks_data_list, report_mails_theme = [], [], []
    if request_id_list and '' not in request_id_list:
        for request_id in request_id_list:
            if request_id.startswith('RA') and not request_id_exists_in_db(
                request_id
            ):
                main_data_list.extend(get_main_data_from_db(request_id))
                tasks_data_list.extend(get_task(request_id))
                for root_path in path_find_walker('find', request_id):
                    report_mails_theme.extend(process_cmd_files(root_path))
    else:
        new_folders = find_new_folders_not_in_db()
        for root_path in new_folders:
            request_id = Path(root_path).name
            report_mails_theme.extend(process_cmd_files(root_path))
            main_data_list.extend(get_main_data_from_db(request_id))
            tasks_data_list.extend(get_task(request_id))

    if main_data_list:
        proccessed_main_data = proccess_main_data(main_data_list)
        proccessed_task_data = task_data_processing(tasks_data_list)
        main_task_data = merge_with_existing_data(
            proccessed_main_data,
            proccessed_task_data
        )
        all_data = merge_with_existing_data(
            main_task_data,
            report_mails_theme
            )

        insert_new_item_in_db(all_data)
