"""
GitLab Repository API клиент — минимальная обёртка для нужд синхронизации.

Используется встроенный urllib (без внешних зависимостей), чтобы DAG
можно было запускать на любом Airflow-воркере без дополнительных пакетов.
Все строки кодируются/декодируются в UTF-8 — кириллица в путях
и в содержимом файлов поддерживается прозрачно.
"""

from __future__ import annotations

import base64
import json
import logging
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

log = logging.getLogger(__name__)


class GitLabAPIError(RuntimeError):
    """Ошибка GitLab REST API."""


class GitLabClient:
    """
    Тонкий клиент над GitLab Repository Files API.
    Документация: https://docs.gitlab.com/ee/api/repository_files.html
    """

    def __init__(
        self,
        gitlab_url: str,
        project_id: str | int,
        token: str,
        branch: str = "main",
        verify_ssl: bool = True,
        timeout: int = 30,
    ):
        self.base_url = gitlab_url.rstrip("/")
        # project_id может быть числом или url-encoded путём (group%2Fproject)
        self.project_id = urllib.parse.quote(str(project_id), safe="")
        self.token = token
        self.branch = branch
        self.verify_ssl = verify_ssl
        self.timeout = timeout

    # ──────────────────────────────────────────────────────────────────────────
    # Низкоуровневый запрос
    # ──────────────────────────────────────────────────────────────────────────
    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        ok_404: bool = False,
    ) -> Any:
        url = f"{self.base_url}/api/v4{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        headers = {
            "PRIVATE-TOKEN": self.token,
            "Accept": "application/json",
        }
        data: bytes | None = None
        if json_body is not None:
            headers["Content-Type"] = "application/json"
            # ensure_ascii=False — чтобы кириллица в commit message не превращалась в \uXXXX
            data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")

        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        try:
            with urllib.request.urlopen(req, context=ctx, timeout=self.timeout) as r:
                body = r.read().decode("utf-8")
                return json.loads(body) if body else None
        except urllib.error.HTTPError as e:
            if e.code == 404 and ok_404:
                return None
            err_body = e.read().decode("utf-8", errors="replace")

            # Диагностика частых случаев — даём пользователю понятную подсказку
            hint = ""
            if e.code == 404 and "Project Not Found" in err_body:
                hint = (
                    f"\n  → проект '{urllib.parse.unquote(self.project_id)}' не найден.\n"
                    f"     Проверьте параметр gitlab.project_id в config.yaml:\n"
                    f"       • для пути   используйте 'group/subgroup/project' "
                    f"(точно как в URL после {self.base_url}/);\n"
                    f"       • для ID     используйте числовой ID из "
                    f"Project → Settings → General → «Project ID».\n"
                    f"     Также убедитесь, что токен имеет доступ именно к этому проекту."
                )
            elif e.code == 401:
                hint = (
                    "\n  → 401 Unauthorized: токен GitLab невалиден или просрочен."
                )
            elif e.code == 403:
                hint = (
                    "\n  → 403 Forbidden: токену не хватает прав. "
                    "Нужны scope 'api' + 'write_repository'."
                )

            raise GitLabAPIError(
                f"GitLab API {method} {path} → {e.code}: {err_body}{hint}"
            ) from e
        except urllib.error.URLError as e:
            reason_str = str(e.reason)
            hint = ""
            if "CERTIFICATE_VERIFY_FAILED" in reason_str or "unable to get local issuer" in reason_str:
                hint = (
                    f"\n→ SSL-сертификат GitLab не проходит проверку. "
                    f"Если используется внутренний CA — либо добавьте "
                    f"\"verify_ssl\": false в Extra GitLab Connection-а (быстро), "
                    f"либо установите CA в trust store контейнера (правильно)."
                )
            raise GitLabAPIError(
                f"Ошибка подключения к GitLab: {e.reason}{hint}"
            ) from e

    # ──────────────────────────────────────────────────────────────────────────
    # Pre-flight: проверка доступа к проекту
    # ──────────────────────────────────────────────────────────────────────────
    def verify_access(self) -> dict[str, Any]:
        """
        Проверяет, что проект существует и токен имеет к нему доступ.
        Возвращает информацию о проекте (path_with_namespace, default_branch, id).
        Бросает GitLabAPIError с понятным сообщением при любых проблемах.

        Дёшево (один GET) — стоит вызывать в начале синхронизации, чтобы
        не падать после получения 400 шаблонов из Zabbix.
        """
        info = self._request("GET", f"/projects/{self.project_id}")
        log.info(
            "GitLab проект OK: %s (id=%s, default_branch=%s)",
            info.get("path_with_namespace"),
            info.get("id"),
            info.get("default_branch"),
        )

        # Пустой репозиторий: ни одного коммита ещё не было. Ветки фактически нет,
        # хотя в метаданных проекта default_branch уже может быть прописан.
        # Первый коммит создаст её сам — пропускаем проверку.
        if info.get("empty_repo"):
            log.warning(
                "Репозиторий пустой. Ветка '%s' будет создана первым коммитом.",
                self.branch,
            )
            return info

        # Репозиторий не пустой — проверяем, что указанная ветка реально существует.
        try:
            self._request(
                "GET",
                f"/projects/{self.project_id}/repository/branches/"
                f"{urllib.parse.quote(self.branch, safe='')}",
            )
        except GitLabAPIError as e:
            if "404" in str(e):
                raise GitLabAPIError(
                    f"Ветка '{self.branch}' не найдена в проекте "
                    f"'{info.get('path_with_namespace')}'. "
                    f"Default branch: '{info.get('default_branch')}'. "
                    f"Поправьте gitlab.branch в config.yaml."
                ) from e
            raise
        return info

    # ──────────────────────────────────────────────────────────────────────────
    # Repository tree / file get
    # ──────────────────────────────────────────────────────────────────────────
    def list_files(self, sub_path: str = "") -> list[str]:
        """
        Возвращает список путей файлов (рекурсивно) в указанной поддиректории.
        Используется пагинация — GitLab отдаёт максимум 100 элементов на страницу.

        На пустом репозитории (нет коммитов) GitLab отвечает 404 на /tree —
        корректно интерпретируем это как «файлов пока нет».
        """
        results: list[str] = []
        page = 1
        while True:
            params = {
                "ref": self.branch,
                "recursive": "true",
                "per_page": 100,
                "page": page,
            }
            if sub_path:
                params["path"] = sub_path
            items = self._request(
                "GET", f"/projects/{self.project_id}/repository/tree",
                params=params, ok_404=True,
            ) or []
            if not items:
                break
            for it in items:
                if it.get("type") == "blob":
                    results.append(it["path"])
            if len(items) < 100:
                break
            page += 1
        return results

    def get_file_content(self, file_path: str) -> str | None:
        """
        Возвращает содержимое файла как UTF-8 строку, либо None, если файла нет.
        """
        encoded = urllib.parse.quote(file_path, safe="")
        resp = self._request(
            "GET",
            f"/projects/{self.project_id}/repository/files/{encoded}",
            params={"ref": self.branch},
            ok_404=True,
        )
        if resp is None:
            return None
        # GitLab возвращает контент в base64
        raw = base64.b64decode(resp["content"])
        return raw.decode("utf-8")

    # ──────────────────────────────────────────────────────────────────────────
    # Commit
    # ──────────────────────────────────────────────────────────────────────────
    def upsert_file(
        self,
        file_path: str,
        content: str,
        commit_message: str,
        author_email: str | None = None,
        author_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Создаёт файл, если его нет, или обновляет, если есть. Не удаляет.

        Контент передаётся в base64 — это надёжный способ для UTF-8 контента
        с произвольными символами (включая управляющие, переводы строк и т.п.).
        """
        encoded = urllib.parse.quote(file_path, safe="")

        # Проверяем существование одним запросом — иначе придётся ловить 400 на create
        exists = self.get_file_content(file_path) is not None
        method = "PUT" if exists else "POST"

        body: dict[str, Any] = {
            "branch": self.branch,
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "encoding": "base64",
            "commit_message": commit_message,
        }
        if author_email:
            body["author_email"] = author_email
        if author_name:
            body["author_name"] = author_name

        log.info(
            "GitLab %s %s (%s)",
            "UPDATE" if exists else "CREATE",
            file_path,
            commit_message,
        )
        return self._request(
            method,
            f"/projects/{self.project_id}/repository/files/{encoded}",
            json_body=body,
        )

    def commit_multiple(
        self,
        actions: list[dict[str, Any]],
        commit_message: str,
        author_email: str | None = None,
        author_name: str | None = None,
        chunk_size: int = 150,
        max_retries: int = 3,
        retry_delay_sec: float = 2.0,
    ) -> list[str]:
        """
        Коммит нескольких файлов через /commits API, РАЗБИТЫЙ НА ПАЧКИ.

        Раньше всё уходило одним POST. При тысячах файлов тело запроса
        становится огромным и отбивается WAF/прокси (например, Qrator) с
        кодом 413 "Request is too large". Поэтому действия дробятся на пачки
        по chunk_size и отправляются отдельными коммитами.

        actions: список словарей вида
            {"action": "create"|"update"|"delete", "file_path": "...", "content": "..."}

        Поведение при сбое пачки: пачка повторяется до max_retries раз (с
        паузой retry_delay_sec); если так и не прошла — пути файлов этой пачки
        добавляются в результат как неудавшиеся, и обработка ПРОДОЛЖАЕТСЯ со
        следующими пачками (для disaster-recovery важно записать максимум
        данных, а не падать на первой ошибке).

        Возвращает список file_path, которые НЕ удалось закоммитить (пустой —
        значит всё успешно). Это сознательное изменение контракта: метод
        больше не бросает исключение на ошибке коммита, а сообщает о
        проблемных файлах вызывающему.
        """
        if not actions:
            return []

        cs = max(1, int(chunk_size))
        chunks = [actions[i:i + cs] for i in range(0, len(actions), cs)]
        total = len(chunks)
        failed: list[str] = []

        for idx, chunk in enumerate(chunks, start=1):
            prepared: list[dict[str, Any]] = []
            for a in chunk:
                item = {"action": a["action"], "file_path": a["file_path"]}
                if a.get("content") is not None:
                    item["content"] = base64.b64encode(
                        a["content"].encode("utf-8")
                    ).decode("ascii")
                    item["encoding"] = "base64"
                prepared.append(item)

            msg = commit_message if total == 1 else f"{commit_message} (part {idx}/{total})"
            body: dict[str, Any] = {
                "branch": self.branch,
                "commit_message": msg,
                "actions": prepared,
            }
            if author_email:
                body["author_email"] = author_email
            if author_name:
                body["author_name"] = author_name

            log.info("GitLab COMMIT part %d/%d: %d action(s)", idx, total, len(chunk))
            if not self._commit_chunk_with_retry(body, max_retries, retry_delay_sec):
                # Пачка не прошла даже после ретраев — фиксируем её файлы и идём дальше.
                failed.extend(a["file_path"] for a in chunk)

        return failed

    def _commit_chunk_with_retry(
        self,
        body: dict[str, Any],
        max_retries: int,
        retry_delay_sec: float,
    ) -> bool:
        """
        Шлёт один чанк-коммит с ретраями. Возвращает True при успехе,
        False — если все попытки исчерпаны.
        """
        attempts = max(1, int(max_retries))
        for attempt in range(1, attempts + 1):
            try:
                self._request(
                    "POST",
                    f"/projects/{self.project_id}/repository/commits",
                    json_body=body,
                )
                return True
            except Exception as e:  # noqa: BLE001
                if attempt < attempts:
                    log.warning(
                        "Коммит пачки не удался (попытка %d/%d): %s — повтор через %.1fs",
                        attempt, attempts, e, retry_delay_sec,
                    )
                    time.sleep(retry_delay_sec)
                else:
                    log.error(
                        "Коммит пачки не удался окончательно (%d попыток): %s",
                        attempts, e,
                    )
        return False
