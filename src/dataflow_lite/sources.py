"""
数据源适配器模块 — 统一的数据读取接口，支持多种数据源类型
Data source adapter module — unified data reading interface for multiple source types
"""

import csv
import json
import os
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, Iterator, List, Optional


class DataSourceAdapter(ABC):
    """数据源适配器基类 Base data source adapter"""

    @abstractmethod
    def read(self, source_config: Dict) -> Iterator[Dict]:
        """读取数据源，返回数据记录迭代器"""
        pass

    @abstractmethod
    def validate(self, source_config: Dict) -> bool:
        """验证数据源配置是否有效"""
        pass

    @abstractmethod
    def describe(self, source_config: Dict) -> Dict:
        """描述数据源的基本信息"""
        pass


class FileSourceAdapter(DataSourceAdapter):
    """文件数据源适配器 — 读取单个文件内容"""

    def read(self, source_config: Dict) -> Iterator[Dict]:
        path = source_config["path"]
        encoding = source_config.get("encoding", "utf-8")

        if not os.path.exists(path):
            raise FileNotFoundError(f"Source file not found: {path}")

        with open(path, "r", encoding=encoding) as f:
            content = f.read()

        yield {
            "_source_path": path,
            "_source_type": "file",
            "_source_name": os.path.basename(path),
            "content": content,
            "size": len(content),
            "lines": content.count("\n") + 1,
        }

    def validate(self, source_config: Dict) -> bool:
        path = source_config.get("path", "")
        return os.path.exists(path) and os.path.isfile(path)

    def describe(self, source_config: Dict) -> Dict:
        path = source_config.get("path", "")
        if os.path.exists(path):
            stat = os.stat(path)
            return {
                "type": "file",
                "path": path,
                "size_bytes": stat.st_size,
                "extension": os.path.splitext(path)[1],
            }
        return {"type": "file", "path": path, "error": "File not found"}


class CSVSourceAdapter(DataSourceAdapter):
    """CSV数据源适配器 — 读取CSV文件为记录流"""

    def read(self, source_config: Dict) -> Iterator[Dict]:
        path = source_config["path"]
        encoding = source_config.get("encoding", "utf-8")
        delimiter = source_config.get("delimiter", ",")
        has_header = source_config.get("has_header", True)

        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")

        with open(path, "r", encoding=encoding, newline="") as f:
            if has_header:
                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    row["_source_path"] = path
                    row["_source_type"] = "csv"
                    yield row
            else:
                reader = csv.reader(f, delimiter=delimiter)
                headers = [f"col_{i}" for i in range(len(next(reader)))]
                f.seek(0)
                next(reader)  # skip header row again
                for values in reader:
                    row = dict(zip(headers, values))
                    row["_source_path"] = path
                    row["_source_type"] = "csv"
                    yield row

    def validate(self, source_config: Dict) -> bool:
        path = source_config.get("path", "")
        return os.path.exists(path) and path.endswith(".csv")

    def describe(self, source_config: Dict) -> Dict:
        path = source_config.get("path", "")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                row_count = sum(1 for _ in f) + 1
            return {
                "type": "csv",
                "path": path,
                "columns": len(headers),
                "rows": row_count,
                "headers": headers[:10],  # 最多显示前10列
            }
        return {"type": "csv", "path": path, "error": "File not found"}


class JSONSourceAdapter(DataSourceAdapter):
    """JSON数据源适配器 — 读取JSON文件/数组"""

    def read(self, source_config: Dict) -> Iterator[Dict]:
        path = source_config["path"]
        encoding = source_config.get("encoding", "utf-8")
        json_path = source_config.get("json_path", "")  # 支持JSONPath简化版

        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")

        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)

        # 如果json_path指定了路径，尝试导航到该路径
        if json_path:
            data = self._navigate_json(data, json_path)

        # 统一处理为数组
        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, list):
            pass
        else:
            data = [{"value": data}]

        for item in data:
            if isinstance(item, dict):
                item["_source_path"] = path
                item["_source_type"] = "json"
                yield item
            else:
                yield {
                    "_source_path": path,
                    "_source_type": "json",
                    "value": item,
                }

    def _navigate_json(self, data: Any, path: str) -> Any:
        """简单的JSON路径导航（支持 dot.notation 和 [index]）"""
        parts = re.split(r'\.|\[|\]', path)
        parts = [p for p in parts if p]
        current = data
        for part in parts:
            if part.isdigit():
                idx = int(part)
                if isinstance(current, list) and idx < len(current):
                    current = current[idx]
                else:
                    return data
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return data
        return current

    def validate(self, source_config: Dict) -> bool:
        path = source_config.get("path", "")
        if not os.path.exists(path):
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                json.load(f)
            return True
        except (json.JSONDecodeError, UnicodeDecodeError):
            return False

    def describe(self, source_config: Dict) -> Dict:
        path = source_config.get("path", "")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return {
                        "type": "json",
                        "path": path,
                        "structure": "array",
                        "count": len(data),
                        "sample_keys": list(data[0].keys())[:10] if data and isinstance(data[0], dict) else [],
                    }
                elif isinstance(data, dict):
                    return {
                        "type": "json",
                        "path": path,
                        "structure": "object",
                        "keys": list(data.keys())[:10],
                    }
            except (json.JSONDecodeError, UnicodeDecodeError):
                return {"type": "json", "path": path, "error": "Invalid JSON"}
        return {"type": "json", "path": path, "error": "File not found"}


class MarkdownSourceAdapter(DataSourceAdapter):
    """Markdown数据源适配器 — 解析Markdown文件为结构化数据"""

    def read(self, source_config: Dict) -> Iterator[Dict]:
        path = source_config["path"]
        encoding = source_config.get("encoding", "utf-8")

        if not os.path.exists(path):
            raise FileNotFoundError(f"Markdown file not found: {path}")

        with open(path, "r", encoding=encoding) as f:
            content = f.read()

        # 解析frontmatter（如果存在）
        metadata = {}
        body = content
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    metadata = self._parse_yaml_simple(parts[1].strip())
                    body = parts[2].strip()
                except Exception:
                    body = content

        # 提取标题结构
        headings = self._extract_headings(body)

        yield {
            "_source_path": path,
            "_source_type": "markdown",
            "_source_name": os.path.basename(path),
            "metadata": metadata,
            "content": body,
            "title": metadata.get("title", headings[0]["text"] if headings else ""),
            "headings": headings,
            "word_count": len(body.split()),
            "heading_count": len(headings),
        }

    def _parse_yaml_simple(self, text: str) -> Dict:
        """简单的YAML解析器（支持基本的key: value格式）"""
        result = {}
        for line in text.split("\n"):
            line = line.strip()
            if ":" in line and not line.startswith("#"):
                key, _, value = line.partition(":")
                value = value.strip().strip('"').strip("'")
                if value.lower() in ("true", "yes"):
                    value = True
                elif value.lower() in ("false", "no"):
                    value = False
                elif value.isdigit():
                    value = int(value)
                result[key.strip()] = value
        return result

    def _extract_headings(self, content: str) -> List[Dict]:
        """提取Markdown标题结构"""
        headings = []
        for line in content.split("\n"):
            match = re.match(r'^(#{1,6})\s+(.+)$', line)
            if match:
                headings.append({
                    "level": len(match.group(1)),
                    "text": match.group(2).strip(),
                })
        return headings

    def validate(self, source_config: Dict) -> bool:
        path = source_config.get("path", "")
        ext = os.path.splitext(path)[1].lower()
        return os.path.exists(path) and ext in (".md", ".markdown", ".mdx")

    def describe(self, source_config: Dict) -> Dict:
        path = source_config.get("path", "")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            headings = self._extract_headings(content)
            return {
                "type": "markdown",
                "path": path,
                "size_bytes": len(content.encode("utf-8")),
                "word_count": len(content.split()),
                "heading_count": len(headings),
                "top_headings": headings[:5],
            }
        return {"type": "markdown", "path": path, "error": "File not found"}


class DirectorySourceAdapter(DataSourceAdapter):
    """目录数据源适配器 — 递归扫描目录中的文件"""

    def read(self, source_config: Dict) -> Iterator[Dict]:
        directory = source_config["path"]
        file_extensions = source_config.get("extensions", None)
        exclude_patterns = source_config.get("exclude", [])
        recursive = source_config.get("recursive", True)
        encoding = source_config.get("encoding", "utf-8")

        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory not found: {directory}")

        import fnmatch

        for root, dirs, files in os.walk(directory):
            if not recursive:
                dirs[:] = []  # 不递归子目录

            # 应用排除模式
            dirs[:] = [
                d for d in dirs
                if not any(fnmatch.fnmatch(d, pat) for pat in exclude_patterns)
            ]

            for fname in sorted(files):
                # 跳过排除的文件
                if any(fnmatch.fnmatch(fname, pat) for pat in exclude_patterns):
                    continue

                # 过滤扩展名
                if file_extensions:
                    _, ext = os.path.splitext(fname)
                    if ext.lower() not in [e.lower() for e in file_extensions]:
                        continue

                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, directory)

                stat = os.stat(full_path)
                yield {
                    "_source_path": full_path,
                    "_source_type": "directory",
                    "_source_name": fname,
                    "_relative_path": rel_path,
                    "filename": fname,
                    "extension": os.path.splitext(fname)[1],
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                }

    def validate(self, source_config: Dict) -> bool:
        path = source_config.get("path", "")
        return os.path.exists(path) and os.path.isdir(path)

    def describe(self, source_config: Dict) -> Dict:
        path = source_config.get("path", "")
        if os.path.exists(path) and os.path.isdir(path):
            file_count = sum(len(files) for _, _, files in os.walk(path))
            return {
                "type": "directory",
                "path": path,
                "file_count": file_count,
            }
        return {"type": "directory", "path": path, "error": "Directory not found"}


# ─── 适配器注册表 ────────────────────────────────────────────
SOURCE_ADAPTERS: Dict[str, DataSourceAdapter] = {
    "file": FileSourceAdapter(),
    "csv": CSVSourceAdapter(),
    "json": JSONSourceAdapter(),
    "markdown": MarkdownSourceAdapter(),
    "directory": DirectorySourceAdapter(),
}


def get_source_adapter(source_type: str) -> DataSourceAdapter:
    """根据类型获取对应的数据源适配器"""
    adapter = SOURCE_ADAPTERS.get(source_type)
    if adapter is None:
        raise ValueError(
            f"Unsupported source type: '{source_type}'. "
            f"Supported types: {list(SOURCE_ADAPTERS.keys())}"
        )
    return adapter
