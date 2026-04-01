pipeline status link

# Automation Reports - Сервис автоматизации сбора информации об автоотчетах и управления их расписанием

Автоматический сбор, обработка и синхронизация информации об автоотчетах с Task Scheduler и MS SQL базой данных.

---

## Содержание

- [Описание](#описание)
- [Возможности](#возможности)
- [Требования](#требования)
- [Установка](#установка)
- [Использование](#использование)
- [Структура проекта](#структура-проекта)
- [Граф зависимостей](#граф-зависимостей-ацикличный--adp)
- [Применённые принципы](#применённые-принципы)
- [Собираемые данные](#собираемые-данные)
- [Автоматическое создание расписания](#автоматическое-создание-расписания)
- [Логирование и мониторинг](#логирование-и-мониторинг)
- [Troubleshooting](#-troubleshooting)
- [Разработка](#разработка)
- [Дополнительные ресурсы](#дополнительные-ресурсы)
- [Поддержка](#поддержка)
- [Авторы](#авторы)
- [Лицензия](#лицензия)

---

## Описание

Сервис автоматизирует процесс сбора информации об автоотчетах и управления задачами в Windows Task Scheduler. Основные функции:

- **Сбор данных** - автоматический сбор информации о заказчиках, получателях, расписаниях
- **Синхронизация с БД** - запись/обновление данных в MS SQL.
- **Управление расписаниями** - создание/обновление/отключение задач в Task Scheduler
- **Уведомления** - отправка email при критических ошибках
- **Автоматизация** - ежедневный запуск без ручного вмешательства

---

## Возможности

### Основные функции

- **Автоматический сбор данных** из нескольких источников:
  - Oracle Database
  - PostgreSQL
  - MS SQL
  - Task Scheduler
  - CMD файлы конфигурации
  - LOG файлы отчетов

- **Умное управление задачами**:
  - Создание расписаний по JSON конфигурации
  - Поддержка множественных триггеров
  - Перемещение отключенных отчетов в архив

- **Мониторинг и уведомления**:
  - Логирование всех операций с ротацией (4 MB лимит)
  - Email уведомления при ошибках `error`/`critical`
  - HTML шаблон письма с деталями ошибок

---

## Требования

### Системные требования

| Компонент | Версия | Примечание |
|-----------|--------|------------|
| **ОС** | Windows Server 2016+ | Требуется для Task Scheduler |
| **Python** | 3.10 или выше | Использует современный type hints |
| **Права доступа** | Администратор | Для работы с Task Scheduler |
| **Память** | 2 GB+ RAM | Для обработки больших данных |

### Python зависимости

```txt
cffi==2.0.0               # C Foreign Function Interface
chardet==5.2.0            # Определение кодировки файлов
cryptography==46.0.3      # Криптография для безопасных соединений
oracledb==3.4.1           # Oracle Database client
psycopg2==2.9.10          # PostgreSQL adapter
pymssql==2.3.10           # MS SQL Server client
pywin32==311              # Windows API (Task Scheduler)
typing_extensions==4.15.0 # Type hints расширения
```

**Дополнительные зависимости для разработки:**

```txt
flake8==7.3.0         # Линтер для проверки PEP8
pycodestyle==2.14.0   # Проверка стиля кода
pyflakes==3.4.0       # Статический анализ кода
mccabe==0.7.0         # Проверка сложности кода
pycparser==2.23       # Парсер C кода (зависимость cffi)
```

### Доступ к базам данных

- **Oracle** - DB_NAME (schema_name)
- **PostgreSQL** - DB_NAME (schema_name)
- **MS SQL** - DB_NAME (таблица autoreports)

Конфигурация подключений находится в `file_name.json` (не в репозитории).

---

## Установка

### 1. Клонирование репозитория

```bash
git clone .../automation_reports.git
cd automation_reports
```

### 2. Создание виртуального окружения

```bash
# Создание venv
python -m venv venv

# Активация (Windows)
venv\Scripts\activate

# Активация (PowerShell)
.\venv\Scripts\Activate.ps1
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Конфигурация

Создайте файл `file_name.json` в корневой директории:

```json
{
  "DB_NAME": {
        "host": "host_name/IP",
        "port": "host_port",
        "database": "database_name",
        "user": "user_name",
        "password": "password"
    }
}
```

### 5. Инициализация БД

При первом запуске таблица `autoreports` будет создана автоматически.

---

## Использование

### Запуск из командной строки

#### 1. Ежедневное обновление (daily_update)

Обновляет информацию о всех существующих автоотчетах:

```bash
python main.py --tasks daily_update
```

**Что происходит:**
- Обходит все папки отчетов
- Собирает данные из Task Scheduler, БД, логов
- Обновляет записи в MS SQL
- Отправляет email при ошибках

#### 2. Поиск и добавление новых отчетов (find)

Поиск новых папок (созданных сегодня) и добавление в БД:

```bash
python main.py --tasks "find"
```

#### 3. Автоматический поиск новых отчетов

Задача запускается автоматически ежедневно в 23:55 (МСК):

```
Имя задачи: task_name
Триггер: Ежедневно в 23:55 (МСК)
Действие 1: python.exe main.py --tasks find
Действие 2: python.exe main.py --tasks daily_update
```

#### 4. Добавление конкретного отчета вручную

Добавить/обновить информацию по конкретному request_id:

```bash
python main.py --tasks find,REQ00000000001
```

Или несколько отчетов:

```bash
python main.py --tasks find,REQ00000000001,REQ00000000002
```

### Запуск через Task Scheduler

Задача запускается автоматически ежедневно в 02:05 (МСК):

```
Имя задачи: task_name
Триггер: Ежедневно в 02:05 (МСК)
Действие: python.exe main.py --tasks daily_update
```

### Примеры вывода

#### Успешное выполнение:
```
2025-12-16 09:13:34,410, INFO, ------------------------------------------------------------, wrapper
2025-12-16 09:13:34,410, INFO, Сервис запущен: 2025-12-16 09:13:34,410, wrapper
2025-12-16 09:13:40,930, INFO, Сервис завершён: 2025-12-16 09:13:40,930, wrapper
2025-12-16 09:13:40,930, INFO, Время выполнения: 00:00:06.520, wrapper
2025-12-16 09:13:40,930, INFO, ------------------------------------------------------------, wrapper
2025-12-16 09:13:40,930, INFO,  , wrapper
```

#### Выполнение с ошибками:
```
2025-11-20 11:36:14,589, ERROR, Некорректные параметры расписания REQ0000000000001: ['Триггер 1: Пустой список дней месяца. Ожидается число от 1 до 31 или "Last".'], handle_error
2025-11-28 11:16:34,391, CRITICAL, Ошибка в update_data: 'request_number', handle_error
Email с ошибками отправлен: fake@fake.ru.
```

---

## Структура проекта

```
automation_reports/
├── main.py                          # Точка входа + CLI
├── container.py                     # Composition Root (DI-контейнер)
│
├── domain/                          # ВНУТРЕННИЙ КРУГ (высокие политики)
│   ├── entities/
│   │   └── models.py                # Сущности: Task, Trigger, Report
│   ├── ports/
│   │   └── interfaces.py            # ABC-интерфейсы (контракты)
│   ├── value_objects/
│   │   └── converters.py            # Bitmask, Date конвертеры
│   └── validators/
│       └── trigger_validator.py     # Чистая валидация триггеров
│
├── application/                     # СРЕДНИЙ КРУГ (use cases)
│   ├── use_cases/
│   │   ├── update_data.py           # Сценарий: ежедневное обновление
│   │   └── insert_data.py           # Сценарий: добавление новых отчётов
│   └── services/
│       └── processing.py            # TaskDataProcessing, DataMerger
│
└── infrastructure/                  # ВНЕШНИЙ КРУГ (низкие политики)
    ├── db/
    │   └── repositories.py          # MSSQL, Oracle, PostgreSQL адаптеры
    ├── scheduler/
    │   └── win32_gateway.py         # Win32 COM Task Scheduler
    ├── filesystem/
    │   └── readers.py               # JSON, CMD, LOG, FolderScanner
    ├── notification/
    │   └── notifier.py              # Logging + SMTP Email
    └── vcs/
        └── git_gateway.py           # Git commit/push
```

---

## Граф зависимостей (Ацикличный — ADP)

```
  infrastructure ──→ application ──→ domain
  (внешний круг)     (средний)      (внутренний)

  Стрелки ВСЕГДА направлены к центру.
  Циклов нет.
```

---


## Применённые принципы

### 1. Dependency Rule (Правило зависимостей)
Зависимости **всегда** направлены от внешнего круга к внутреннему.
`infrastructure` → `application` → `domain`.
Domain **ничего** не знает о БД, файлах, COM API.

### 2. SRP (Single Responsibility Principle)
Каждый модуль имеет **одну причину для изменений**:

| Модуль | Причина изменений |
|--------|------------------|
| `trigger_validator.py` | Правила валидации расписания |
| `win32_gateway.py` | API Task Scheduler |
| `repositories.py` | Схема БД / SQL запросы |
| `readers.py` | Формат файлов (JSON, CMD, LOG) |
| `processing.py` | Маппинг полей для БД |

### 3. CCP (Common Closure Principle)
Классы, изменяющиеся по одной причине, сгруппированы в один пакет:
- `infrastructure/db/` — все адаптеры к БД
- `domain/value_objects/` — все конвертеры дат и масок
- `domain/validators/` — вся валидация

### 4. Dependency Inversion (DIP)
Use Cases зависят от **абстракций** (`domain/ports/interfaces.py`),
а не от конкретных реализаций. Конкретные классы передаются через
конструктор (Dependency Injection).

### 5. Open/Closed Principle (OCP)
Для добавления нового источника данных:
- YAML расписание → создать `YamlScheduleFileReader(ScheduleFileReader)`
- cron → создать `CronTaskSchedulerGateway(TaskSchedulerGateway)`

**Существующий код не меняется.**

---

## Собираемые данные

### Информация о заказчике и получателе

| Поле | Источник | Описание |
|------|----------|----------|
| `customer_login` | Oracle/PostgreSQL | Логин заказчика отчета |
| `customer_name` | Oracle/PostgreSQL | ФИО заказчика |
| `customer_company` | Oracle/PostgreSQL | Компания заказчика |
| `customer_orgstructure` | Oracle/PostgreSQL | Структурное подразделение |
| `receiver_login` | Oracle/PostgreSQL | Логин получателя |
| `receiver_name` | Oracle/PostgreSQL | ФИО получателя |
| `receiver_company` | Oracle/PostgreSQL | Компания получателя |
| `receiver_orgstructure` | Oracle/PostgreSQL | Структурное подразделение |

### Информация об отчете

| Поле | Источник | Описание |
|------|----------|----------|
| `request_id` | Имя папки | Номер запроса (RA) |
| `report_create_date` | Oracle/PostgreSQL | Дата создания (выполнения RA) |
| `report_name` | CMD файл | Тема письма отчета |
| `report_recipients_email` | CMD файл | Email получателей |
| `report_sender_type` | CMD файл | Тип отправителя (mailer_name.exe) |
| `report_in_archive` | Task Scheduler | Отчет в архиве (True/False) |
| `report_archive_folder` | Task Scheduler | Путь к папке в архиве |

### Информация о последней отправке

| Поле | Источник | Описание |
|------|----------|----------|
| `last_send_date` | LOG файл | Дата/время последней отправки |
| `last_send_status` | LOG файл | Статус (Success/Issue/SendError) |
| `last_send_recipients` | LOG файл | Фактические получатели |
| `last_send_attachments` | LOG файл | Список вложений |
| `last_send_issue` | LOG файл | Проблемы (отсутствующие вложения) |
| `last_send_error` | LOG файл | Текст ошибки |

### Информация о задаче Task Scheduler

| Поле | Источник | Описание |
|------|----------|----------|
| `task_name` | Task Scheduler | Имя задачи (= request_id) |
| `task_last_run_date` | Task Scheduler | Дата/время последнего запуска |
| `task_last_run_result` | Task Scheduler | Результат (Success/Error/...) |
| `task_status` | Task Scheduler | Статус (Включена/Отключена/...) |
| `task_file_path` | Task Scheduler | Путь к исполняемому файлу |
| `task_file_name` | Task Scheduler | Имя исполняемого файла |
| `task_schedule_type` | Task Scheduler | Тип расписания (daily/weekly/monthly) |
| `task_schedule_start_date` | Task Scheduler | Дата начала действия |
| `task_schedule_*` | Task Scheduler | Параметры расписания |

### Информация о базе данных

| Поле | Источник | Описание |
|------|----------|----------|
| `database_type` | CMD файл | Тип БД (postgre/oracle) |
| `database_hostname` | CMD файл | Имя хоста БД |

---

## Автоматическое создание расписания

Расписание создается автоматически, если в папке отчета есть файл `{request_id}.json`.

### Формат JSON конфигурации

#### Простой пример (один триггер):
```json
{
  "Тип расписания": "daily",
  "Дата начала": "2025-12-01",
  "Время начала": "08:00",
  "Частота отправки": 1,
  "Статус расписания": 1,
  "Описание задачи": "Ежедневный отчет",
  "Остановить, если дольше": "2H"
}
```

#### Расширенный пример (множественные триггеры):
```json
{
  "Триггеры": [
    {
      "Тип расписания": "daily",
      "Дата начала": "2025-12-01",
      "Время начала": "08:00",
      "Частота отправки": 1,
      "Включен": true
    },
    {
      "Тип расписания": "weekly",
      "Дата начала": "2025-12-01",
      "Время начала": "18:00",
      "Дни недели": ["Понедельник", "Пятница"],
      "Частота отправки": 1,
      "Включен": true
    }
  ],
  "Статус расписания": 1,
  "Описание задачи": "Отчет с множественными триггерами",
  "Остановить, если дольше": "2H"
}
```

### Поддерживаемые типы расписания

| Тип | Описание | Дополнительные параметры |
|-----|----------|--------------------------|
| `daily` | Ежедневно | `Частота отправки` (дни) |
| `weekly` | Еженедельно | `Дни недели`, `Частота отправки` (недели) |
| `monthly` | Ежемесячно | `Дни месяца`, `Месяцы` |

### Дополнительные параметры

- **Повторение**: `"Повторение": "30M"` - повторять каждые 30 минут
- **Дни недели**: `["Понедельник", "Среда", "Пятница"]`
- **Дни месяца**: `[1, 15, "Last"]` - 1-е, 15-е числа и последний день
- **Месяцы**: `["Январь", "Июнь", "Декабрь"]` или `["Ежемесячно"]`

**Подробная документация**: Confluence - Создание расписания

---

## Логирование и мониторинг

### Расположение логов

```
log/update_autoreports_data.log
```

### Формат логов

```
YYYY-MM-DD HH:MM:SS,mmm, LEVEL, message, funcName
```

Пример:
```
2025-12-16 10:30:03,263, INFO, Сервис запущен: 2025-12-16 10:30:03,263, main
2025-12-16 10:30:03,300, WARNING, Файл лога не найден REQ00000000001, process_log_files
2025-12-16 10:30:03,350, ERROR, Ошибка чтения файла: Permission denied, check_encoding
2025-12-16 10:30:05,450, CRITICAL, Ошибка подключения к Oracle: Connection timeout, _get_oracle_connection
```

### Уровни логирования

| Уровень | Описание | Отправка email |
|---------|----------|----------------|
| **INFO** | Информационные сообщения | ❌ Нет |
| **WARNING** | Предупреждения, не критичные ошибки | ❌ Нет |
| **ERROR** | Ошибки, требующие внимания | ✅ **Да** |
| **CRITICAL** | Критические ошибки, блокирующие работу | ✅ **Да** |

### Ротация логов

- **Лимит размера**: 4 MB
- **При достижении лимита**: 
  - Текущий лог ротируется с суффиксом `.1`, `.2`, ... `.5`
  - Создается новый чистый лог-файл
- **Хранение**: Максимум 5 архивных файлов (старые удаляются автоматически)

### Email уведомления

При возникновении ошибок `ERROR` или `CRITICAL`:

1. **Получатель**: `fake@fake.ru`
2. **Тема**: "Ошибки при работе сервиса Automation_Reports"
3. **Содержимое**: 
   - HTML таблица с типами ошибок
   - Временная метка обнаружения
   - Рекомендации по действиям
4. **Вложение**: Файл `update_autoreports_data.log`

Пример HTML письма:

![Email notification example](error_notification_preview.png)

---

## 🔧 Troubleshooting

### Частые проблемы и решения

#### ❌ Проблема: `ModuleNotFoundError: No module named 'win32com'`

**Решение:**
```bash
pip install pywin32
# После установки запустить:
python venv/Scripts/pywin32_postinstall.py -install
```

---

#### ❌ Проблема: `Access denied` при работе с Task Scheduler

**Причина:** Недостаточно прав для создания/изменения задач.

**Решение:**
```bash
# Запустить командную строку от администратора
# Или настроить задачу на запуск от имени SYSTEM
```

---

#### ❌ Проблема: `Connection timeout` при подключении к Oracle

**Возможные причины:**
- Не установлен Oracle Instant Client
- Неверный DSN в `file_name.json`
- Проблемы с сетью/firewall

**Решение:**
```bash
# 1. Установить Oracle Instant Client
# Скачать: https://www.oracle.com/database/technologies/instant-client/downloads.html

# 2. Инициализировать в коде (уже есть):
oracledb.init_oracle_client()

# 3. Проверить DSN:
python -c "import oracledb; oracledb.init_oracle_client(); print('OK')"
```

---

#### ❌ Проблема: Email не отправляется

**Проверка:**
```python
# Тестовый скрипт test_smtp.py
import smtplib

server = smtplib.SMTP('smtp.domain.ru', 25)
server.ehlo()
print(server.ehlo())  # Должен вернуть (250, ...)
server.quit()
```

**Частые причины:**
- Неверный SMTP сервер/порт
- Требуется аутентификация (проверить password в конфиге)
- Firewall блокирует порт 25

---

#### ❌ Проблема: Ошибка `UnicodeDecodeError` при чтении файлов

**Причина:** Неверная кодировка файла.

**Решение:** Функция `check_encoding()` автоматически определяет кодировку, но если проблема сохраняется:
```python
# В utils.py функция check_encoding() использует chardet
# Fallback: windows-1251
# Можно добавить альтернативные кодировки в список проверки
```

---

#### ❌ Проблема: Папка отчета не найдена

**Проверка структуры:**
```
parent_folder/
├── db_name/
│   └── REQ00000000001/
│       ├── REQ000000000001.cmd
│       ├── REQ000000000001.json (опционально)
│       └── log.log
├── db_name/
└── db_name/
```

**Решение:** Убедитесь, что:
- Папка находится в `ROOT_REPORT_PATHS` (constants.py)
- Имя папки = request_id (15 символов, начинается с REQ)
- Не в списке игнорируемых (`!example`, `archive`, `old`, `.git`)

---

#### ❌ Проблема: Задача создана, но не запускается

**Проверка:**
1. Открыть Task Scheduler (`taskschd.msc`)
2. Найти задачу в папке `\Autoreports`
3. Посмотреть "Last Run Result"

**Частые причины:**
- Код ошибки `0x1` (1) - ошибка в скрипте
- Код ошибки `0x2` (2) - файл не найден
- Код ошибки `0x41301` - задача уже запущена

**Решение:**
```bash
# Запустить вручную для отладки:
cd путь_к_папке
python main.py --tasks daily_update
# Проверить вывод в консоль
```

---

## Разработка

### Внесение изменений

#### 1. Синхронизация с репозиторием

```bash
# Проверка актуальности
git status

# Обновление локальной версии
git pull
```

#### 2. Создание ветки для разработки (рекомендуется)

```bash
# Создание новой ветки
git checkout -b feature/my-feature

# Или для исправления бага
git checkout -b fix/bug-description
```

#### 3. Внесение изменений

- Следуйте PEP8 стилю кодирования
- Добавляйте docstrings к функциям
- Используйте type hints
- Тестируйте изменения локально

#### 4. Коммит и push

```bash
# Добавить изменения
git add .

# Создать коммит с осмысленным сообщением
git commit -m "feat: добавлена поддержка новых типов расписания"
# или
git commit -m "fix: исправлена ошибка парсинга JSON"

# Отправить в репозиторий
git push origin feature/my-feature
```

#### 5. Создание Merge Request

1. Перейти в GitLab
2. Создать Merge Request из вашей ветки в `main`
3. Дождаться прохождения CI/CD pipeline
4. Получить review (если требуется)
5. Merge

### Соглашения о коммитах

Используйте [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: новая функциональность
fix: исправление бага
docs: изменения в документации
style: форматирование, отступы (без изменения кода)
refactor: рефакторинг кода
perf: улучшение производительности
test: добавление тестов
chore: обновление зависимостей, конфигурации
```

Примеры:
```bash
git commit -m "feat: добавлена поддержка PostgreSQL 15"
git commit -m "fix: исправлена ошибка деления на ноль в statistics"
git commit -m "perf: оптимизировано чтение больших файлов логов"
git commit -m "docs: обновлен README с примерами использования"
```

### Проверка кода перед коммитом

```bash
# Проверка импортов (нет циклических зависимостей)
python -c "import main"

# Проверка PEP8 (если установлен flake8)
flake8 --max-line-length=79 .

# Проверка type hints (если установлен mypy)
mypy --ignore-missing-imports .
```

### CI/CD Pipeline

При каждом push в GitLab запускается pipeline:

```yaml
stages:
  - test
  - lint
  - deploy

test:
  stage: test
  script:
    - python -c "import main"
    - python -m pytest tests/

lint:
  stage: lint
  script:
    - flake8 --max-line-length=79 .

deploy:
  stage: deploy
  script:
    - # Деплой на сервера (если настроено)
  only:
    - main
```

Статус pipeline: pipeline status link

---

## Дополнительные ресурсы

### Документация

- Confluence - Таблица autoreports
- Confluence - Создание расписания
- Confluence - Параметры JSON

### Внутренние ссылки

- GitLab Repository
- CI/CD Pipelines
- Issues

---

## Поддержка

При возникновении проблем:

1. **Проверьте логи**: `log/update_autoreports_data.log`
2. **Проверьте раздел [Troubleshooting](#-troubleshooting)**
3. **Создайте Issue**: GitLab Issues
4. **Свяжитесь с командой**: `fake@fake.ru`

---

## Авторы

**Разработчик и поддержка:**

- [GitGub Profile](https://github.com/wtfucka) - *автор и основной разработчик*

**Команда Autoreports:** fake@fake.ru

---

## Лицензия

The Unlicense

---

*Документация обновлена: 16 декабря 2025*  
*Версия сервиса: 2.0*  
*Python: 3.11+*
