from subprocess import run

from error_handler import handle_error


def execute_command(args: list[str]) -> list[str]:
    '''
    Выполняет команду через subprocess.run и возвращает результат.

    :param args: Список аргументов командной строки.
    :return: Результат команды в виде списка строк.
    '''
    try:
        result = run(
            args,
            capture_output=True,
            text=True,
            encoding='cp866'
        )
        if result.returncode != 0:
            handle_error(f"Ошибка выполнения команды: {' '.join(args)}\n{result.stderr}", 'error')  # noqa
            return []
        return result.stdout.splitlines()
    except FileNotFoundError:
        handle_error('Команда schtasks не найдена. Убедитесь, что Task Scheduler доступен.', 'critical')  # noqa
        return []
    except Exception as e:
        handle_error(f'Неизвестная ошибка: {e}', 'critical')
        return []


def get_all_tasks() -> list[str]:
    '''
    Получение всех задач из Task Scheduler.

    :return: Список всех задач Task Scheduler.
    '''
    return execute_command(['schtasks', '/query', '/v', '/fo', 'LIST'])


def get_task(taskname: str) -> list[str]:
    '''
    Получение информации о конкретной задаче в Task Scheduler по имени задачи.

    :param taskname: Название задания в Task Scheduler.
    :return: Информацию о задаче Task Scheduler.
    '''
    return execute_command(
        ['schtasks', '/query', '/tn', taskname, '/v', '/fo', 'LIST']
        )
