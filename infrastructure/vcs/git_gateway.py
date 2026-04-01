from __future__ import annotations

from subprocess import CalledProcessError, run

from domain.ports.interfaces import ErrorNotifier, VersionControlGateway


class GitVersionControlGateway(VersionControlGateway):
    '''Реализация VCS через git CLI.'''

    def __init__(self, notifier: ErrorNotifier):
        self._notifier = notifier

    def commit_and_push(
        self, folders: dict[str, list[str]],
    ) -> None:
        if not folders:
            return

        for parent_folder, names in folders.items():
            msg = f'Перемещены в архив: {",".join(names)}'
            try:
                run(
                    ['git', 'add', '.'],
                    cwd=parent_folder,
                    check=True,
                )
                run(
                    ['git', 'commit', '-m', msg],
                    cwd=parent_folder,
                    check=True,
                )
            except CalledProcessError as e:
                self._notifier.notify(
                    f'Git commit failed for '
                    f'{parent_folder}: {e}',
                    'warning',
                )

        for parent_folder in folders:
            try:
                run(
                    ['git', 'push'],
                    cwd=parent_folder,
                    check=True,
                )
            except CalledProcessError as e:
                self._notifier.notify(
                    f'Git push failed for '
                    f'{parent_folder}: {e}',
                    'warning',
                )
