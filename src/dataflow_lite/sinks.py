"""
数据输出模块 — 统一的数据写入接口，支持多种输出目标
Data sink module — unified data writing interface for multiple output targets
"""

import csv
import json
import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional


class DataSinkAdapter(ABC):
    """数据输出适配器基类 Base data sink adapter"""

    @abstractmethod
    def write(self, records: List[Dict], config: Dict) -> int:
        """写入数据记录，返回写入数量"""
        pass

    @abstractmethod
    def validate(self, config: Dict) -> bool:
        """验证输出配置是否有效"""
        pass


class FileSinkAdapter(DataSinkAdapter):
    """文件输出适配器 — 将数据写入文本文件"""

    def write(self, records: List[Dict], config: Dict) -> int:
        output_path = config["path"]
        template = config.get("template", "{{content}}")
        encoding = config.get("encoding", "utf-8")
        mode = config.get("mode", "overwrite")  # overwrite | append

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        file_mode = "w" if mode == "overwrite" else "a"

        with open(output_path, file_mode, encoding=encoding) as f:
            for record in records:
                # 模板渲染
                def replacer(match):
                    field = match.group(1).strip()
                    val = record.get(field, "")
                    return str(val) if val is not None else ""

                import re
                content = re.sub(r'\{\{(.+?)\}\}', replacer, template)
                f.write(content + "\n")

        return len(records)

    def validate(self, config: Dict) -> bool:
        path = config.get("path", "")
        return bool(path)

    def describe(self, config: Dict) -> Dict:
        return {"type": "file", "path": config.get("path", "")}


class JSONSinkAdapter(DataSinkAdapter):
    """JSON输出适配器 — 将数据写入JSON文件"""

    def write(self, records: List[Dict], config: Dict) -> int:
        output_path = config["path"]
        encoding = config.get("encoding", "utf-8")
        indent = config.get("indent", 2)
        mode = config.get("mode", "overwrite")
        array_field = config.get("array_field", "")  # 如果指定，将记录包装到该字段下

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        # 清理内部元数据字段
        clean_records = []
        for r in records:
            clean = {k: v for k, v in r.items() if not k.startswith("_")}
            clean_records.append(clean)

        if array_field:
            output_data = {array_field: clean_records}
        else:
            output_data = clean_records

        file_mode = "w" if mode == "overwrite" else "r+"
        if mode == "append" and os.path.exists(output_path):
            # 追加模式：读取已有数据并合并
            try:
                with open(output_path, "r", encoding=encoding) as f:
                    existing = json.load(f)
                if isinstance(existing, list):
                    existing.extend(clean_records)
                    output_data = existing
                elif isinstance(existing, dict) and array_field:
                    existing[array_field].extend(clean_records)
                    output_data = existing
            except (json.JSONDecodeError, IOError):
                pass

        with open(output_path, "w", encoding=encoding) as f:
            json.dump(output_data, f, indent=indent, ensure_ascii=False, default=str)

        return len(records)

    def validate(self, config: Dict) -> bool:
        path = config.get("path", "")
        return bool(path)

    def describe(self, config: Dict) -> Dict:
        return {"type": "json", "path": config.get("path", "")}


class CSVSinkAdapter(DataSinkAdapter):
    """CSV输出适配器 — 将数据写入CSV文件"""

    def write(self, records: List[Dict], config: Dict) -> int:
        output_path = config["path"]
        encoding = config.get("encoding", "utf-8")
        delimiter = config.get("delimiter", ",")
        mode = config.get("mode", "overwrite")
        include_header = config.get("include_header", True)
        columns = config.get("columns", None)  # 指定输出列及顺序

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        if not records:
            return 0

        # 清理内部元数据字段
        clean_records = []
        for r in records:
            clean = {k: v for k, v in r.items() if not k.startswith("_")}
            clean_records.append(clean)

        # 确定列
        if columns:
            fieldnames = columns
        else:
            # 合并所有记录的字段
            fieldnames = []
            seen = set()
            for r in clean_records:
                for key in r.keys():
                    if key not in seen:
                        fieldnames.append(key)
                        seen.add(key)

        file_mode = "w" if mode == "overwrite" else "a"
        newline = "" if mode == "overwrite" else "\n"

        with open(output_path, file_mode, encoding=encoding, newline=newline) as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter,
                                    extrasaction="ignore", quoting=csv.QUOTE_ALL)
            if include_header and mode == "overwrite":
                writer.writeheader()
            writer.writerows(clean_records)

        return len(records)

    def validate(self, config: Dict) -> bool:
        path = config.get("path", "")
        return bool(path)

    def describe(self, config: Dict) -> Dict:
        return {"type": "csv", "path": config.get("path", "")}


class SQLiteSinkAdapter(DataSinkAdapter):
    """SQLite输出适配器 — 将数据写入SQLite数据库"""

    def write(self, records: List[Dict], config: Dict) -> int:
        output_path = config["path"]
        table_name = config.get("table", "dataflow_output")
        mode = config.get("mode", "overwrite")  # overwrite | append | upsert
        primary_key = config.get("primary_key", "")

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        if not records:
            return 0

        # 清理内部元数据字段
        clean_records = []
        for r in records:
            clean = {k: v for k, v in r.items() if not k.startswith("_")}
            clean_records.append(clean)

        # 收集所有字段
        all_fields = []
        seen = set()
        for r in clean_records:
            for key in r.keys():
                if key not in seen:
                    all_fields.append(key)
                    seen.add(key)

        with sqlite3.connect(output_path) as conn:
            # 创建表
            columns_def = ", ".join(f'"{f}" TEXT' for f in all_fields)
            if mode == "overwrite":
                conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})')

            # 插入数据
            placeholders = ", ".join("?" for _ in all_fields)
            for record in clean_records:
                values = [str(record.get(f, "")) for f in all_fields]

                if mode == "upsert" and primary_key and primary_key in record:
                    # Upsert: 存在则更新，不存在则插入
                    update_set = ", ".join(
                        f'"{f}" = ?' for f in all_fields if f != primary_key
                    )
                    update_values = [str(record.get(f, "")) for f in all_fields if f != primary_key]
                    conn.execute(
                        f'INSERT OR REPLACE INTO "{table_name}" ({", ".join(f"{f}" for f in all_fields)}) '
                        f'VALUES ({placeholders})',
                        values
                    )
                else:
                    conn.execute(
                        f'INSERT INTO "{table_name}" ({", ".join(f"{f}" for f in all_fields)}) '
                        f'VALUES ({placeholders})',
                        values
                    )

            conn.commit()

        return len(records)

    def validate(self, config: Dict) -> bool:
        path = config.get("path", "")
        return bool(path)

    def describe(self, config: Dict) -> Dict:
        return {"type": "sqlite", "path": config.get("path", ""), "table": config.get("table", "dataflow_output")}


class DirectorySinkAdapter(DataSinkAdapter):
    """目录输出适配器 — 将数据写入目录中的文件"""

    def write(self, records: List[Dict], config: Dict) -> int:
        output_dir = config["path"]
        filename_field = config.get("filename_field", "_source_name")
        extension = config.get("extension", ".txt")
        encoding = config.get("encoding", "utf-8")
        content_field = config.get("content_field", "content")
        template = config.get("template", "")

        os.makedirs(output_dir, exist_ok=True)

        written = 0
        for record in records:
            # 确定文件名
            filename = str(record.get(filename_field, f"output_{written}"))
            if not filename.endswith(extension):
                filename += extension

            output_path = os.path.join(output_dir, filename)

            # 确定内容
            if template:
                import re
                def replacer(match):
                    field = match.group(1).strip()
                    val = record.get(field, "")
                    return str(val) if val is not None else ""
                content = re.sub(r'\{\{(.+?)\}\}', replacer, template)
            else:
                content = str(record.get(content_field, json.dumps(record, ensure_ascii=False, default=str)))

            with open(output_path, "w", encoding=encoding) as f:
                f.write(content)

            written += 1

        return written

    def validate(self, config: Dict) -> bool:
        path = config.get("path", "")
        return bool(path)

    def describe(self, config: Dict) -> Dict:
        return {"type": "directory", "path": config.get("path", "")}


# ─── 输出适配器注册表 ─────────────────────────────────────────
SINK_ADAPTERS: Dict[str, DataSinkAdapter] = {
    "file": FileSinkAdapter(),
    "json": JSONSinkAdapter(),
    "csv": CSVSinkAdapter(),
    "sqlite": SQLiteSinkAdapter(),
    "directory": DirectorySinkAdapter(),
}


def get_sink_adapter(sink_type: str) -> DataSinkAdapter:
    """根据类型获取对应的输出适配器"""
    adapter = SINK_ADAPTERS.get(sink_type)
    if adapter is None:
        raise ValueError(
            f"Unsupported sink type: '{sink_type}'. "
            f"Supported types: {list(SINK_ADAPTERS.keys())}"
        )
    return adapter
