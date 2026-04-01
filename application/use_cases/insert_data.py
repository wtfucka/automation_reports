from __future__ import annotations

from pathlib import Path
from typing import Any

from domain.ports.interfaces import (
    CmdFileReader,
    ErrorNotifier,
    FolderScanner,
    ReportDataReader,
    ReportRepository,
    ScheduleFileReader,
    TaskSchedulerGateway,
)
from domain.validators.trigger_validator import TriggerValidator

from application.services.processing import (
    DataMergerService,
    MainDataProcessingService,
    TaskDataProcessingService,
)


class InsertDataUseCase:
    '''
    Сценарий: добавление новых отчётов в БД.

    Может быть вызван:
    - с явным списком request_id (ручной запуск)
    - без списка (автопоиск новых папок за сегодня)
    '''

    def __init__(
        self,
        folder_scanner: FolderScanner,
        oracle_reader: ReportDataReader,
        postgre_reader: ReportDataReader,
        report_repo: ReportRepository,
        scheduler_gateway: TaskSchedulerGateway,
        schedule_file_reader: ScheduleFileReader,
        cmd_reader: CmdFileReader,
        error_notifier: ErrorNotifier,
    ):
        self._folders = folder_scanner
        self._oracle = oracle_reader
        self._postgre = postgre_reader
        self._repo = report_repo
        self._scheduler = scheduler_gateway
        self._schedule_reader = schedule_file_reader
        self._cmd_reader = cmd_reader
        self._notifier = error_notifier

        self._validator = TriggerValidator()
        self._task_processor = TaskDataProcessingService()
        self._merger = DataMergerService()
        self._main_processor = MainDataProcessingService()

    def execute(
        self,
        request_id_list: set[str] | None = None,
    ) -> None:
        '''Основной поток добавления.'''
        root_paths = self._resolve_paths(request_id_list)
        if not root_paths:
            return

        # Сбор данных
        main_data, tasks_data, cmd_data = (
            self._collect(root_paths)
        )

        if not main_data:
            return

        # Обработка и слияние
        processed_main = self._main_processor.process(main_data)
        processed_tasks = self._task_processor.process_task_list(
            tasks_data
        )
        all_data = self._merger.merge(
            processed_main, processed_tasks
        )
        all_data = self._merger.merge(all_data, cmd_data)

        # Запись
        self._repo.insert_reports(all_data)

    def _resolve_paths(
        self,
        request_id_list: set[str] | None,
    ) -> list[str]:
        '''Определяет список папок для добавления.'''
        if request_id_list:
            return self._paths_from_ids(request_id_list)
        return self._paths_new_today()

    def _paths_from_ids(
        self, request_ids: set[str],
    ) -> list[str]:
        '''Находит папки по явному списку ID, исключая уже существующие.'''
        valid_ids = [
            rid for rid in request_ids
            if rid.startswith('REQ')
        ]
        if not valid_ids:
            return []

        exists_map = self._repo.exists_batch(valid_ids)
        paths: list[str] = []
        for rid in valid_ids:
            if not exists_map.get(rid, False):
                found = self._folders.find_all(rid)
                paths.extend(found)
        return paths

    def _paths_new_today(self) -> list[str]:
        '''Ищет папки, созданные сегодня и отсутствующие в БД.'''
        from datetime import datetime

        all_folders = self._folders.find_all()
        candidates: list[tuple[str, str]] = []

        today = datetime.now().date()
        for folder_path in all_folders:
            p = Path(folder_path)
            try:
                ctime = p.stat().st_ctime
                if (
                    datetime.fromtimestamp(ctime).date() == today
                    and p.name.startswith('REQ')
                ):
                    candidates.append((p.name, str(p)))
            except OSError:
                continue

        if not candidates:
            return []

        ids = [rid for rid, _ in candidates]
        exists_map = self._repo.exists_batch(ids)
        return [
            path for rid, path in candidates
            if not exists_map.get(rid, False)
        ]

    def _collect(
        self, root_paths: list[str],
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        '''Сбор данных из всех источников.'''
        request_ids = [Path(p).name for p in root_paths]

        oracle_map = self._oracle.get_main_data_batch(request_ids)
        postgre_map = self._postgre.get_main_data_batch(
            request_ids
        )

        main_data: list[dict[str, Any]] = []
        tasks_data: list[dict[str, Any]] = []
        cmd_data: list[dict[str, Any]] = []

        for root_path in root_paths:
            path = Path(root_path)
            request_id = path.name

            # Расписание
            self._sync_scheduler(str(path), request_id)

            # Данные из внешних БД
            if request_id in oracle_map:
                main_data.append(oracle_map[request_id])
            if request_id in postgre_map:
                main_data.append(postgre_map[request_id])

            # Задача из планировщика
            task_info = self._scheduler.get_task_info(request_id)
            if task_info:
                tasks_data.append(
                    vars(task_info)
                    if hasattr(task_info, '__dict__')
                    else task_info
                )

            # CMD
            cmd_info = self._cmd_reader.read(str(path))
            cmd_data.append(vars(cmd_info))

        return main_data, tasks_data, cmd_data

    def _sync_scheduler(
        self, path: str, request_id: str,
    ) -> None:
        task = self._schedule_reader.read(path, request_id)
        if task is None:
            return

        errors = self._validator.validate_all(task.triggers)
        if errors:
            self._notifier.notify(
                f'Ошибки валидации {request_id}: {errors}',
                'error',
            )
            return

        self._scheduler.register_task(task)
