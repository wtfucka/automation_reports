from __future__ import annotations

from domain.entities.models import (
    ScheduledTaskEntity,
    TaskInfoEntity,
    TriggerType,
)
from domain.ports.interfaces import ErrorNotifier, TaskSchedulerGateway
from domain.value_objects.converters import (
    BitmaskConverter,
    DateConverter,
    DateFormatter,
)


class Win32TaskSchedulerGateway(TaskSchedulerGateway):
    '''
    Реализация TaskSchedulerGateway через Win32 COM API.
    Для перехода на cron — создайте CronTaskSchedulerGateway,
    не трогая этот файл (Open/Closed Principle).
    '''

    TASK_FOLDER = '\\Autoreports'

    _TYPE_MAP = {
        TriggerType.ONE_TIME: 1,
        TriggerType.DAILY: 2,
        TriggerType.WEEKLY: 3,
        TriggerType.MONTHLY: 4,
    }

    def __init__(self, notifier: ErrorNotifier):
        import win32com.client
        self._notifier = notifier
        self._scheduler = win32com.client.Dispatch(
            'Schedule.Service'
        )
        self._scheduler.Connect()

    def register_task(self, task: ScheduledTaskEntity) -> bool:
        from pywintypes import com_error

        folder = self._get_or_create_folder()
        if folder is None:
            return False

        task_def = self._scheduler.NewTask(0)
        task_def.RegistrationInfo.Description = task.description
        task_def.RegistrationInfo.Author = 'automation_reports'

        ok = 0
        for trigger in task.triggers:
            try:
                self._add_trigger(task_def, trigger)
                ok += 1
            except (
                com_error, ValueError, KeyError, AttributeError,
            ) as e:
                self._notifier.notify(
                    f'Ошибка триггера {task.task_name}: {e}',
                    'warning',
                )

        if ok == 0:
            self._notifier.notify(
                f'Нет триггеров для {task.task_name}', 'error'
            )
            return False

        action = task_def.Actions.Create(0)
        action.Path = task.executable_path

        s = task_def.Settings
        s.Enabled = task.state
        s.StartWhenAvailable = True
        s.Hidden = False
        s.ExecutionTimeLimit = f'PT{task.stop_if_runs_longer}'
        s.Compatibility = 6

        p = task_def.Principal
        p.LogonType = 3
        p.RunLevel = 1

        try:
            folder.RegisterTaskDefinition(
                task.task_name, task_def,
                6, None, None, 2, None,
            )
        except com_error as e:
            self._notifier.notify(
                f'Регистрация не удалась {task.task_name}: {e}',
                'warning',
            )
            return False

        return True

    def get_task_info(
        self, task_name: str,
    ) -> TaskInfoEntity | None:
        from pywintypes import com_error

        folder = self._get_or_create_folder()
        if folder is None:
            return None

        try:
            task = folder.GetTask(task_name)
            defn = task.Definition

            triggers_info = []
            for t in defn.Triggers:
                dow = getattr(t, 'DaysOfWeek', 0) or 0
                dom = getattr(t, 'DaysOfMonth', 0) or 0
                moy = getattr(t, 'MonthsOfYear', 0) or 0

                triggers_info.append({
                    'Schedule Type': (
                        TriggerType(t.Type).name
                        if getattr(t, 'Type', None)
                        else None
                    ),
                    'StartBoundary': getattr(
                        t, 'StartBoundary', None
                    ),
                    'Trigger State': getattr(
                        t, 'Enabled', None
                    ),
                    'DaysInterval': getattr(
                        t, 'DaysInterval', None
                    ),
                    'WeeksInterval': getattr(
                        t, 'WeeksInterval', None
                    ),
                    'DaysOfWeek': (
                        DateConverter.get_day_name(
                            BitmaskConverter
                            .bitmask_to_days_of_week(dow)
                        ) or None
                    ),
                    'DaysOfMonth': (
                        BitmaskConverter
                        .bitmask_to_days_of_month(dom) or None
                    ),
                    'MonthsOfYear': (
                        DateConverter.get_month_name(
                            BitmaskConverter
                            .bitmask_to_months_of_year(moy)
                        ) or None
                    ),
                    'Repeat_every': getattr(
                        t.Repetition, 'Interval', None
                    ),
                    'Repeat_until_duration': getattr(
                        t.Repetition, 'Duration', None
                    ),
                    'Repeat_until_time': getattr(
                        t.Repetition, 'StopAtDurationEnd', None
                    ),
                })

            return TaskInfoEntity(
                task_name=task.Name,
                last_run_time=task.LastRunTime,
                last_result=task.LastTaskResult,
                author=defn.RegistrationInfo.Author,
                actions=[a.Path for a in defn.Actions],
                description=defn.RegistrationInfo.Description,
                state=task.State,
                run_as_user=defn.Principal.UserId,
                triggers_info=triggers_info,
            )
        except (com_error, Exception) as e:
            self._notifier.notify(
                f'Ошибка чтения задачи {task_name}: {e}',
                'warning',
            )
            return None

    def _get_or_create_folder(self):
        from pywintypes import com_error

        try:
            return self._scheduler.GetFolder(self.TASK_FOLDER)
        except com_error:
            try:
                root = self._scheduler.GetFolder('\\')
                return root.CreateFolder(
                    self.TASK_FOLDER.lstrip('\\')
                )
            except com_error as e:
                self._notifier.notify(
                    f'Не удалось создать папку '
                    f'{self.TASK_FOLDER}: {e}',
                    'error',
                )
                return None

    def _add_trigger(self, task_def, entity) -> None:
        com_type = self._TYPE_MAP[entity.trigger_type]
        t = task_def.Triggers.Create(com_type)

        t.StartBoundary = DateFormatter.format_start_boundary(
            start_time=entity.start_time,
            start_date=entity.start_date,
        )
        t.Enabled = entity.enabled

        if entity.trigger_type == TriggerType.ONE_TIME:
            # Разовый триггер: только StartBoundary.
            # EndBoundary — дата, после которой триггер удаляется.
            if entity.end_date:
                t.EndBoundary = (
                    DateFormatter.format_start_boundary(
                        start_time='23:59',
                        start_date=entity.end_date,
                        delta_hours=0,
                    )
                )

        elif entity.trigger_type == TriggerType.DAILY:
            t.DaysInterval = entity.interval_week_days

        elif entity.trigger_type == TriggerType.WEEKLY:
            t.WeeksInterval = entity.interval_week_days
            days_int = DateConverter.get_day_num(
                entity.days_of_week
            )
            t.DaysOfWeek = (
                BitmaskConverter.days_of_week_to_bitmask(days_int)
            )

        elif entity.trigger_type == TriggerType.MONTHLY:
            t.DaysOfMonth = (
                BitmaskConverter.days_of_month_to_bitmask(
                    entity.days_of_month
                )
            )
            months_int = DateConverter.get_month_num(
                entity.months_of_year
            )
            t.MonthsOfYear = (
                BitmaskConverter.months_of_year_to_bitmask(
                    months_int
                )
            )
            if entity.run_on_last_day_of_month:
                t.RunOnLastDayOfMonth = True
            if entity.set_all_months:
                t.MonthsOfYear = 0xFFF

        if entity.repetition_interval:
            t.Repetition.Interval = entity.repetition_interval
            t.Repetition.Duration = 'P1D'
            t.Repetition.StopAtDurationEnd = False
