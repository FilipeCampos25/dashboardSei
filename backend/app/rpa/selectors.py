from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Dict, List


class SelectorNotFoundError(KeyError):
    """Raised when a selector path is missing from xpath_selector.json."""


@dataclass(frozen=True)
class XPathSelectors:
    _data: Dict[str, Any]
    source_path: Path

    @classmethod
    def from_file(cls, path: Path | None = None) -> "XPathSelectors":
        json_path = path or (Path(__file__).resolve().parent / "xpath_selector.json")
        if not json_path.exists():
            raise FileNotFoundError(
                f"Arquivo de seletores nao encontrado: {json_path} "
                "(esperado: backend/app/rpa/xpath_selector.json)"
            )

        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError(f"JSON de seletores invalido em {json_path}: raiz deve ser objeto")

        return cls(_data=data, source_path=json_path)

    def get(self, path: str, default: Any = None) -> Any:
        try:
            return self.require(path)
        except SelectorNotFoundError:
            return default

    def require(self, path: str) -> Any:
        if not path or not isinstance(path, str):
            raise ValueError("O caminho do seletor deve ser uma string nao vazia")

        node: Any = self._data
        traversed: List[str] = []
        for part in path.split("."):
            traversed.append(part)
            if not isinstance(node, dict) or part not in node:
                raise SelectorNotFoundError(self._build_missing_message(path, traversed[:-1], part))
            node = node[part]
        return node

    def get_many(self, path: str) -> List[str]:
        value = self.require(path)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return [normalized]
        elif isinstance(value, list):
            normalized_list = [
                item.strip()
                for item in value
                if isinstance(item, str) and item.strip()
            ]
            if normalized_list:
                return normalized_list

        raise ValueError(
            f"Seletor '{path}' invalido em {self.source_path}: esperado string ou lista de strings nao vazias"
        )

    def available_paths(self) -> List[str]:
        paths: List[str] = []
        self._collect_paths(self._data, prefix="", out=paths)
        return sorted(paths)

    def _collect_paths(self, node: Any, prefix: str, out: List[str]) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                current = f"{prefix}.{key}" if prefix else key
                out.append(current)
                self._collect_paths(value, current, out)

    def _build_missing_message(
        self,
        requested_path: str,
        parent_parts: List[str],
        missing_part: str,
    ) -> str:
        parent_path = ".".join(parent_parts)
        try:
            parent_node = self.require(parent_path) if parent_path else self._data
        except SelectorNotFoundError:
            parent_node = None

        available_children: List[str] = []
        if isinstance(parent_node, dict):
            available_children = sorted(str(k) for k in parent_node.keys())

        suggestions = self._suggest_paths(requested_path)
        suggestion_msg = ""
        if suggestions:
            suggestion_msg = f" Sugestoes: {', '.join(suggestions)}."
        elif available_children:
            suggestion_msg = f" Chaves disponiveis em '{parent_path or '<raiz>'}': {', '.join(available_children)}."

        return (
            f"Seletor '{requested_path}' nao encontrado em {self.source_path} "
            f"(parte ausente: '{missing_part}' em '{parent_path or '<raiz>'}')."
            f"{suggestion_msg}"
        )

    def _suggest_paths(self, requested_path: str) -> List[str]:
        available = self.available_paths()
        candidates = get_close_matches(requested_path, available, n=5, cutoff=0.4)
        if candidates:
            return candidates

        # Fallback: tenta sugerir combinacoes por sufixo da ultima parte.
        last_part = requested_path.split(".")[-1]
        suffix_matches = [path for path in available if path.endswith(f".{last_part}") or path == last_part]
        return suffix_matches[:5]


def load_xpath_selectors() -> XPathSelectors:
    return XPathSelectors.from_file()
