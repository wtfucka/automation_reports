from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from domain.entities.models import (
    ReportCmdInfo,
    ReportLogInfo,
    ScheduledTaskEntity,
    TriggerEntity,
    TriggerType,
)
from domain.ports.interfaces import (
    CmdFileReader,
    ErrorNotifier,
    FolderScanner,
    LogFileReader,
    ReportRepository,
    ScheduleFileReader,
)
from domain.value_objects.converters import DateFormatter


def _check_encoding(file_path: Path) -> str:
    '''Определяет кодировку файла через chardet.'''
    from chardet import detect

    try:
        with open(file_path, 'rb') as f:
            raw = f.read(5000)
        enc = detect(raw).get('encoding')
        if enc in (None, 'ascii', 'charmap', 'latin-1'):
            return 'windows-1251'
        return enc  # type: ignore
    except (IOError, OSError):
        return 'windows-1251'


def _read_last_n_lines(
    file_path: Path,
    n: int = 10,
    encoding: str = 'utf-8',
) -> list[str]:
    '''Эффективное чтение последних N строк из файла.'''
    buf_size = 8192
    try:
        with open(file_path, 'rb') as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return []

            pos = size
            buf = b''
            lines: list[str] = []

            while len(lines) < n and pos > 0:
                chunk_sz = min(buf_size, pos)
                pos -= chunk_sz
                f.seek(pos)
                buf = f.read(chunk_sz) + buf

                try:
                    text = buf.decode(encoding)
                    all_lines = text.split('\n')
                    if pos > 0:
                        buf = all_lines[0].encode(encoding)
                        lines = all_lines[1:] + lines
                    else:
                        lines = all_lines + lines
                except UnicodeDecodeError:
                    continue

            non_empty = [l for l in lines if l.strip()]
            return non_empty[-n:]
    except (IOError, OSError):
        return []


class JsonScheduleFileReader(ScheduleFileReader):
    '''
    Читает расписание из JSON файлов.
    Для поддержки YAML — создайте YamlScheduleFileReader
    (Open/Closed).
    '''

    FIELD_MAPPING = {
        'Тип расписания': 'trigger_type',
        'Дата начала': 'start_date',
        'Время начала': 'start_time',
        'Дата окончания': 'end_date',
        'Частота отправки': 'interval_week_days',
        'Дни недели': 'days_of_week',
        'Дни месяца': 'days_of_month',
        'Месяцы': 'months_of_year',
        'Повторение': 'repetition_interval',
        'Включен': 'enabled',
    }

    def __init__(self, notifier: ErrorNotifier):
        self._notifier = notifier

    def read(
        self, path: str, request_id: str,
    ) -> ScheduledTaskEntity | None:
        root = Path(path)
        json_file = root / f'{request_id}.json'
        cmd_file = root / f'{request_id}.cmd'

        if not json_file.is_file():
            self._notifier.notify(
                f'JSON расписания не найден: {json_file}',
                'warning',
            )
            return None

        if not cmd_file.is_file():
            self._notifier.notify(
                f'CMD не найден: {cmd_file}', 'warning'
            )
            return None

        try:
            enc = _check_encoding(json_file)
            with open(json_file, 'r', encoding=enc) as f:
                content: dict = json.load(f)
        except (
            IOError, OSError, UnicodeDecodeError,
            json.JSONDecodeError,
        ) as e:
            self._notifier.notify(
                f'Ошибка чтения {json_file}: {e}', 'warning'
            )
            return None

        # Поддержка обоих форматов
        if 'Триггеры' in content:
            raw_list = content['Триггеры']
            if not isinstance(raw_list, list) or not raw_list:
                self._notifier.notify(
                    f'Некорректные триггеры: {json_file}',
                    'error',
                )
                return None
        else:
            raw_list = [content]

        triggers = [self._parse_trigger(r) for r in raw_list]

        return ScheduledTaskEntity(
            task_name=root.name,
            executable_path=str(cmd_file),
            triggers=triggers,
            description=content.get('Описание задачи', ''),
            state=content.get('Статус расписания', 1),
            stop_if_runs_longer=content.get(
                'Остановить, если дольше', '2H'
            ),
        )

    def _parse_trigger(self, raw: dict) -> TriggerEntity:
        mapped: dict[str, Any] = {}
        for ru, en in self.FIELD_MAPPING.items():
            val = raw.get(ru)
            if val is not None:
                mapped[en] = val

        tt_str = mapped.pop('trigger_type', 'daily')
        mapped['trigger_type'] = TriggerType(tt_str)

        dom = mapped.get('days_of_month', [])
        if isinstance(dom, list) and 'Last' in dom:
            mapped['run_on_last_day_of_month'] = True
            dom.remove('Last')

        moy = mapped.get('months_of_year', [])
        if isinstance(moy, list) and 'Ежемесячно' in moy:
            mapped['set_all_months'] = True
            moy.remove('Ежемесячно')

        rep = mapped.get('repetition_interval', '')
        if rep and isinstance(rep, str):
            mapped['repetition_interval'] = 'PT' + rep

        return TriggerEntity(**mapped)


class CmdFileReaderImpl(CmdFileReader):
    '''Извлекает email, тему и БД из .cmd файлов.'''

    def __init__(
        self,
        notifier: ErrorNotifier,
        database_type_map: dict[str, str] | None = None,
    ):
        self._notifier = notifier
        self._db_type_map = database_type_map or {}

    def read(self, root_path: str) -> ReportCmdInfo:
        path = Path(root_path)
        info = ReportCmdInfo(task_name=path.name)

        cmd_file = self._find_cmd(path)
        if not cmd_file:
            return info

        try:
            enc = _check_encoding(cmd_file)
            with open(cmd_file, 'r', encoding=enc) as f:
                content = f.read()
        except (IOError, OSError, UnicodeDecodeError) as e:
            self._notifier.notify(
                f'Ошибка чтения {cmd_file}: {e}', 'warning'
            )
            return info

        # Парсинг
        db_match = re.search(
            r'-dbName\s+"([^"]+)"', content
        )
        if db_match:
            db_name = db_match.group(1).lower()
            info.database_hostname = db_name
            info.database_type = self._db_type_map.get(
                db_name, ''
            )

        sender_pat = re.compile(
            r'mailer_name\.exe\s+"([\w\.-@;]+)\|([^|]+)\|'
        )
        for line in reversed(content.splitlines()):
            m = sender_pat.search(line)
            if m:
                s = re.search(
                    r'mailer_name\.exe', m.group(0)
                )
                info.sender_type = s.group(0) if s else ''
                info.emails = m.group(1)
                info.theme = m.group(2)
                break

        theme_match = re.search(
            r'set\s+"REPORT_NAME=(.+?)"', content
        )
        if theme_match:
            info.theme = theme_match.group(1)

        return info

    @staticmethod
    def _find_cmd(path: Path) -> Path | None:
        for f in path.iterdir():
            if (
                f.is_file()
                and f.name.startswith('REQ')
                and f.name.endswith('.cmd')
            ):
                return f
        return None


class LogFileReaderImpl(LogFileReader):
    '''Читает лог последней отправки отчёта.'''

    def __init__(
        self,
        notifier: ErrorNotifier,
        log_file_name: str = 'log.log',
    ):
        self._notifier = notifier
        self._log_name = log_file_name

    def read(self, root_path: str) -> ReportLogInfo:
        path = Path(root_path)
        info = ReportLogInfo(task_name=path.name)

        log_file = self._find_log(path)
        if not log_file:
            self._notifier.notify(
                f'Лог не найден: {path.name}', 'warning'
            )
            return info

        pattern = re.compile(
            r'(?P<datetime>\d{2}\.\d{2}\.\d{4} '
            r'\d{1,2}:\d{2}:\d{2}(?: [APM]{2})?)\s*\|'
            r'(?P<status>Success|Issue|SendError)\s*\|'
            r'(?:Письмо успешно отправлено получателям:\s*'
            r'(?P<recipients>(?:\[[^\]]*\],?)*)\s*\|)?'
            r'(?:Вложения:\s*'
            r'(?P<attachments>(?:\[[^\]]*\],?)*)\s*\|)?'
            r'(?:Не найденные вложения:\s*'
            r'(?P<missing>(?:\[[^\]]*\],?)*)\s*)?'
            r'(?P<error>.*)'
        )

        try:
            enc = _check_encoding(log_file) or 'windows-1251'
            lines = _read_last_n_lines(
                log_file, n=10, encoding=enc
            )
            match = self._find_best_match(lines, pattern)

            if match:
                dt = DateFormatter.parse_log_datetime(
                    match.group('datetime')
                )
                status = match.group('status') or ''

                recipients = self._parse_list(
                    match.group('recipients') or ''
                )
                attachments = self._parse_list(
                    match.group('attachments') or ''
                )
                missing = self._parse_list(
                    match.group('missing') or ''
                )
                error_msg = (
                    match.group('error').strip()
                    if match.group('error') else ''
                )

                info.last_send_date = dt
                info.last_send_status = (
                    'SendError'
                    if 'fake@fake.ru' in recipients
                    else status
                )
                info.last_send_recipients = recipients
                info.last_send_attachments = attachments
                if missing:
                    info.last_send_issue = missing
                if error_msg:
                    info.last_send_error = error_msg

        except (IOError, OSError, TypeError) as e:
            self._notifier.notify(
                f'Ошибка чтения лога {log_file}: {e}',
                'warning',
            )

        return info

    def _find_log(self, path: Path) -> Path | None:
        for candidate in (
            path / self._log_name,
            path / 'log' / self._log_name,
        ):
            if candidate.is_file():
                return candidate
        return None

    @staticmethod
    def _find_best_match(lines, pattern):
        '''Ищет лучшее совпадение (предпочтительно сегодня).'''
        today = datetime.now().date()
        best = None
        for line in reversed(lines):
            m = pattern.search(line)
            if not m:
                continue
            dt = DateFormatter.parse_log_datetime(
                m.group('datetime')
            )
            if dt.date() == today:
                return m
            if best is None:
                best = m
        return best

    @staticmethod
    def _parse_list(raw: str) -> str:
        if not raw:
            return ''
        return ','.join(
            item.strip('[]')
            for item in raw.split('],[')
            if item
        )


class FolderScannerImpl(FolderScanner):
    '''
    Сканер папок отчётов на файловой системе.
    SRP: знает только про структуру папок.
    '''

    IGNORED_DIRS = {
        '!example', 'archive', 'log', 'old', '.git',
    }

    def __init__(
        self,
        root_report_paths: list[str],
        report_repo: ReportRepository,
        notifier: ErrorNotifier,
    ):
        self._root_paths = root_report_paths
        self._repo = report_repo
        self._notifier = notifier

    def walk_existing(self) -> list[str]:
        '''Папки, уже зарегистрированные в БД.'''
        candidates = self._all_candidates()
        ids = [p.name for p in candidates]
        exists = self._repo.exists_batch(ids)
        return [
            str(p) for p in candidates
            if exists.get(p.name, False)
        ]

    def find_all(
        self, search_folder: str | None = None,
    ) -> list[str]:
        candidates = self._all_candidates()
        if search_folder:
            return [
                str(p) for p in candidates
                if p.name == search_folder
            ]
        return [str(p) for p in candidates]

    def _all_candidates(self) -> list[Path]:
        parent = Path.cwd().parent
        result: list[Path] = []
        for rp in self._root_paths:
            root = parent / rp
            if not root.exists():
                continue
            for d in root.iterdir():
                if (
                    d.is_dir()
                    and d.name.lower() not in self.IGNORED_DIRS
                ):
                    result.append(d)
        return result
