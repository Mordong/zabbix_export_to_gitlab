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
            raise GitLabAPIError(
                f"GitLab API {method} {path} → {e.code}: {err_body}"
            ) from e
        except urllib.error.URLError as e:
            raise GitLabAPIError(
                f"Ошибка подключения к GitLab: {e.reason}"
            ) from e

    # ──────────────────────────────────────────────────────────────────────────
    # Repository tree / file get
    # ──────────────────────────────────────────────────────────────────────────
    def list_files(self, sub_path: str = "") -> list[str]:
        """
        Возвращает список путей файлов (рекурсивно) в указанной поддиректории.
        Используется пагинация — GitLab отдаёт максимум 100 элементов на страницу.
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
                "GET", f"/projects/{self.project_id}/repository/tree", params=params
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
    ) -> dict[str, Any] | None:
        """
        Атомарный коммит сразу нескольких файлов через /commits API.
        actions: список словарей вида
            {"action": "create"|"update"|"delete", "file_path": "...", "content": "..."}

        Используем для пачки шаблонов — один коммит вместо N, экономит
        и время, и засорение истории репозитория.
        """
        if not actions:
            return None
        prepared: list[dict[str, Any]] = []
        for a in actions:
            item = {
                "action": a["action"],
                "file_path": a["file_path"],
            }
            if a.get("content") is not None:
                item["content"] = base64.b64encode(
                    a["content"].encode("utf-8")
                ).decode("ascii")
                item["encoding"] = "base64"
            prepared.append(item)

        body: dict[str, Any] = {
            "branch": self.branch,
            "commit_message": commit_message,
            "actions": prepared,
        }
        if author_email:
            body["author_email"] = author_email
        if author_name:
            body["author_name"] = author_name

        log.info("GitLab COMMIT %d action(s): %s", len(actions), commit_message)
        return self._request(
            "POST",
            f"/projects/{self.project_id}/repository/commits",
            json_body=body,
        )
