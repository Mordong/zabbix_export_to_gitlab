"""
Утилиты для синхронизации шаблонов.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from typing import Any

import yaml


# ──────────────────────────────────────────────────────────────────────────────
# Безопасные имена файлов
# ──────────────────────────────────────────────────────────────────────────────
# Запрещённые в путях GitLab/файловых систем символы.
# Кириллицу и не-ASCII оставляем как есть — GitLab и git нормально их хранят
# (пути в git представлены UTF-8 байтами; ОС-агностично).
_FORBIDDEN_RE = re.compile(r'[\x00-\x1f\x7f<>:"|?*\\/]+')
_WHITESPACE_RE = re.compile(r"\s+")


def safe_filename(name: str, max_len: int = 200) -> str:
    """
    Превращает host-имя шаблона Zabbix в безопасное имя файла,
    сохраняя кириллицу читаемой.

      "Linux by Zabbix agent"   → "Linux_by_Zabbix_agent"
      "Шаблон СУБД/PostgreSQL"  → "Шаблон_СУБД_PostgreSQL"
      "  лишние   пробелы  "    → "лишние_пробелы"
    """
    # Нормализуем юникод (NFC) — чтобы одинаковые на вид строки имели одинаковые байты
    name = unicodedata.normalize("NFC", name)
    # Заменяем все запрещённые символы на подчёркивание
    name = _FORBIDDEN_RE.sub("_", name)
    # Сжимаем пробельные группы в один _
    name = _WHITESPACE_RE.sub("_", name)
    # Схлопываем повторяющиеся подчёркивания
    name = re.sub(r"_+", "_", name)
    # Убираем точки/подчёркивания/дефисы по краям
    name = name.strip("._-")
    if not name:
        name = "template"
    # Ограничиваем длину — у разных ФС лимиты от 255 байт; берём с запасом по символам
    if len(name) > max_len:
        name = name[:max_len].rstrip("._-")
    return name


# ──────────────────────────────────────────────────────────────────────────────
# Нормализация YAML — для сравнения «по смыслу», а не «побайтово»
# ──────────────────────────────────────────────────────────────────────────────
# Поля экспорта Zabbix, которые меняются при каждом экспорте даже без правок.
# Их нужно вырезать перед сравнением, иначе будут «ложные» обновления.
_VOLATILE_TOP_KEYS = {"date"}  # zabbix_export.date — время экспорта


def _strip_volatile(node: Any) -> Any:
    """
    Рекурсивно удаляет шумовые поля. Сейчас это только zabbix_export.date.
    Структура экспорта Zabbix 7:
        zabbix_export:
          version: '7.0'
          date: '2024-01-01T00:00:00Z'
          template_groups: [...]
          templates: [...]
    """
    if isinstance(node, dict):
        return {
            k: _strip_volatile(v)
            for k, v in node.items()
            if k not in _VOLATILE_TOP_KEYS
        }
    if isinstance(node, list):
        return [_strip_volatile(x) for x in node]
    return node


def yaml_semantic_equal(a: str, b: str) -> bool:
    """
    Возвращает True, если два YAML-документа эквивалентны по смыслу
    (после нормализации: парсинг + удаление volatile-полей).
    Поддерживает UTF-8 / кириллицу автоматически — yaml.safe_load
    работает с unicode-строками без преобразований.
    """
    if a is None or b is None:
        return False
    try:
        da = yaml.safe_load(a)
        db = yaml.safe_load(b)
    except yaml.YAMLError:
        # Если хотя бы один невалиден — fallback на побайтовое сравнение
        return a == b

    return _strip_volatile(da) == _strip_volatile(db)


def dump_yaml(data: Any) -> str:
    """
    Сериализует Python-структуру (dict/list) в стабильный YAML.

    Используется для auth-экспорта (authentication / userdirectories), где
    мы сами строим документ из ответа API, а не получаем готовый YAML от
    Zabbix. Ключевые свойства:

      - allow_unicode=True  — кириллица сохраняется как есть, без \\uXXXX;
      - sort_keys=True      — порядок ключей детерминирован, поэтому
                              перестановка полей в ответе API не вызывает
                              ложных диффов при сравнении с git;
      - default_flow_style=False — человекочитаемый блочный вид;
      - width=4096          — длинные строки (DN, фильтры LDAP) не переносятся
                              в случайных местах, что тоже стабилизирует дифф.
    """
    return yaml.safe_dump(
        data,
        allow_unicode=True,
        sort_keys=True,
        default_flow_style=False,
        width=4096,
    )


# Заголовки CSV-файла маппинга групп (английские, через ';').
USER_GROUP_MAPPING_HEADER = ["LDAP group pattern", "User groups", "User role"]


def dump_user_group_mapping_csv(userdirectory: dict[str, Any]) -> str:
    """
    Строит CSV-маппинг групп для ОДНОГО user directory.

    Формат (разделитель ';', кодировка UTF-8 без BOM):

        LDAP group pattern;User groups;User role
        cn=admins,ou=groups,dc=corp;Zabbix administrators;Super admin role
        cn=ops,ou=groups,dc=corp;Group A,Group B;User role

    Столбцы:
      - LDAP group pattern — provision_groups[].name (паттерн сопоставления);
      - User groups        — имена user-групп (_grp_name) через запятую;
      - User role          — имя роли (_role_name).

    Поведение:
      - порядок строк сохраняется как в ответе API (без сортировки);
      - если provision_groups пуст — возвращается только строка заголовков;
      - значения с ';', ',', кавычками или переносом строки квотируются
        стандартным модулем csv автоматически (поэтому запятые-разделители
        внутри "User groups" не ломают структуру файла);
      - переводы строк — '\\n' (lineterminator), чтобы дифф в git был
        стабильным независимо от ОС, на которой выполняется экспорт.

    Возвращает строку (str). Записывать в файл/GitLab нужно как UTF-8.
    """
    buf = io.StringIO()
    writer = csv.writer(
        buf,
        delimiter=";",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    writer.writerow(USER_GROUP_MAPPING_HEADER)

    for pattern, groups_joined, role in _iter_group_mapping_rows(userdirectory):
        writer.writerow([pattern, groups_joined, role])

    return buf.getvalue()


def _iter_group_mapping_rows(userdirectory: dict[str, Any]):
    """
    Общий обход provision_groups для CSV и MD: возвращает кортежи
    (LDAP group pattern, "User groups через запятую", User role).

    Имена резолвятся (_grp_name / _role_name) с fallback на ID. Порядок —
    как в ответе API (без сортировки), чтобы дифф был стабильным.
    """
    for pg in userdirectory.get("provision_groups", []) or []:
        pattern = pg.get("name", "")
        role = pg.get("_role_name", "") or pg.get("roleid", "")
        groups = [
            (ug.get("_grp_name", "") or ug.get("usrgrpid", ""))
            for ug in (pg.get("user_groups", []) or [])
        ]
        yield pattern, ",".join(groups), role


def _md_escape(value: str) -> str:
    """
    Экранирует значение для ячейки Markdown-таблицы: '|' → '\\|' (иначе
    разбивает столбцы), переводы строк → пробел (ячейка должна быть в одну
    строку). Кириллица не трогается.
    """
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").replace("\r", "")


def dump_user_group_mapping_md(userdirectory: dict[str, Any]) -> str:
    """
    Строит Markdown-файл маппинга групп для ОДНОГО user directory.

    Структура (UTF-8, переводы строк '\\n'):

        # <имя directory>

        | LDAP group pattern | User groups | User role |
        | --- | --- | --- |
        | cn=admins,dc=corp | Zabbix administrators | Super admin role |
        | cn=ops,dc=corp | Группа А,Группа Б | User role |

    Те же три столбца и те же данные, что и в CSV-варианте
    (dump_user_group_mapping_csv): User groups перечисляются через запятую,
    имена резолвятся с fallback на ID, порядок строк — как в ответе API.

    Поведение для пустого provision_groups — единообразно с CSV: выводится
    заголовок и шапка таблицы без строк данных.

    Символ '|' в значениях экранируется ('\\|'), чтобы не ломать таблицу.
    """
    name = userdirectory.get("name") or userdirectory.get("userdirectoryid", "directory")
    header = " | ".join(USER_GROUP_MAPPING_HEADER)
    sep = " | ".join(["---"] * len(USER_GROUP_MAPPING_HEADER))

    lines = [
        f"# {name}",
        "",
        f"| {header} |",
        f"| {sep} |",
    ]
    for pattern, groups_joined, role in _iter_group_mapping_rows(userdirectory):
        cells = " | ".join(_md_escape(c) for c in (pattern, groups_joined, role))
        lines.append(f"| {cells} |")

    return "\n".join(lines) + "\n"


# ──────────────────────────────────────────────────────────────────────────────
# Форматирование
# ──────────────────────────────────────────────────────────────────────────────
def format_duration(seconds: int) -> str:
    """Человекочитаемая длительность для логов."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}с"
    if seconds < 3600:
        return f"{seconds // 60}м {seconds % 60}с"
    h, rem = divmod(seconds, 3600)
    return f"{h}ч {rem // 60}м"
