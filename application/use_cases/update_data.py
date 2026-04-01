from __future__ import annotations

from pathlib import Path
from typing import Any

from domain.ports.interfaces import (
    CmdFileReader,
    ErrorNotifier,
    FolderScanner,
    LogFileReader,
    ReportDataReader,
    ReportRepository,
    ScheduleFileReader,
    TaskSchedulerGateway,
    VersionControlGateway,
)
from domain.validators.trigger_validator import TriggerValidator

from application.services.processing import (
    DataMergerService,
    MainDataProcessingService,
    TaskDataProcessingService,
)


class UpdateDataUseCase:
    '''
    Сценарий: ежедневное обновление данных отчётов в БД.

    Все зависимости — абстракции (порты), внедряемые
    через конструктор (Dependency Injection).
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
        log_reader: LogFileReader,
        vcs_gateway: VersionControlGateway,
        error_notifier: ErrorNotifier,
    ):
        self._folders = folder_scanner
        self._oracle = oracle_reader
        self._postgre = postgre_reader
        self._repo = report_repo
        self._scheduler = scheduler_gateway
        self._schedule_reader = schedule_file_reader
        self._cmd_reader = cmd_reader
        self._log_reader = log_reader
        self._vcs = vcs_gateway
        self._notifier = error_notifier

        self._validator = TriggerValidator()
        self._task_processor = TaskDataProcessingService()
        self._merger = DataMergerService()
        self._main_processor = MainDataProcessingService()

    def execute(self) -> None:
        '''Основной поток обновления.'''
        root_paths = self._folders.walk_existing()
        if not root_paths:
            self._notifier.notify(
                'Не найдено папок для обновления', 'warning'
            )
            return

        # 1. Сбор первичных данных
        main_data, tasks_data, cmd_data, log_data = (
            self._collect_primary_data(root_paths)
        )

        # 2. Обработка
        processed_tasks = self._task_processor.process_task_list(
            tasks_data
        )
        processed_main = self._main_processor.process(main_data)

        # 3. Слияние
        all_data = self._merger.merge(processed_main, processed_tasks)
        all_data = self._merger.merge(all_data, log_data)
        all_data = self._merger.merge(all_data, cmd_data)

        # 4. Запись в БД
        self._repo.update_reports(all_data)

    def _collect_primary_data(
        self, root_paths: list[str],
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        '''Собирает данные из всех источников.'''
        request_ids = [Path(p).name for p in root_paths]

        # Batch-запросы к внешним БД
        oracle_map = self._oracle.get_main_data_batch(request_ids)
        postgre_map = self._postgre.get_main_data_batch(request_ids)

        main_data: list[dict[str, Any]] = []
        tasks_data: list[dict[str, Any]] = []
        cmd_data: list[dict[str, Any]] = []
        log_data: list[dict[str, Any]] = []

        for root_path in root_paths:
            path = Path(root_path)
            request_id = path.name

            # Расписание Task Scheduler
            self._sync_scheduler(str(path), request_id)

            # Данные из внешних БД
            if request_id in oracle_map:
                main_data.append(oracle_map[request_id])
            if request_id in postgre_map:
                main_data.append(postgre_map[request_id])

            # Информация о задаче из планировщика
            task_info = self._scheduler.get_task_info(request_id)
            if task_info:
                tasks_data.append(self._task_info_to_dict(task_info))

            # CMD + LOG
            cmd_info = self._cmd_reader.read(str(path))
            cmd_data.append(vars(cmd_info))

            log_info = self._log_reader.read(str(path))
            log_data.append(vars(log_info))

        return main_data, tasks_data, cmd_data, log_data

    def _sync_scheduler(
        self, path: str, request_id: str,
    ) -> None:
        '''Читает JSON, валидирует, создаёт/обновляет задачу.'''
        task = self._schedule_reader.read(path, request_id)
        if task is None:
            return

        errors = self._validator.validate_all(task.triggers)
        if errors:
            self._notifier.notify(
                f'Ошибки валидации триггеров {request_id}: '
                f'{errors}',
                'error',
            )
            return

        self._scheduler.register_task(task)

    @staticmethod
    def _task_info_to_dict(info) -> dict[str, Any]:
        '''Конвертирует TaskInfoEntity → dict для обработки.'''
        if isinstance(info, dict):
            return info
        return vars(info) if hasattr(info, '__dict__') else {}
