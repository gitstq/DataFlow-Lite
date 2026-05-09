"""
数据转换模块 — 内置多种数据转换操作
Data transform module — built-in data transformation operations
"""

import copy
import json
import os
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union


class TransformOperation(ABC):
    """数据转换操作基类 Base transform operation"""

    @property
    @abstractmethod
    def name(self) -> str:
        """操作名称"""
        pass

    @abstractmethod
    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        """
        对单条记录执行转换

        Args:
            record: 输入数据记录
            config: 转换配置参数

        Returns:
            转换后的记录，返回None表示过滤掉该记录
        """
        pass

    def transform_batch(self, records: List[Dict], config: Dict) -> List[Dict]:
        """批量转换记录"""
        results = []
        for record in records:
            result = self.transform(record, config)
            if result is not None:
                results.append(result)
        return results


class FilterTransform(TransformOperation):
    """过滤转换 — 根据条件过滤记录"""

    @property
    def name(self) -> str:
        return "filter"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        field_name = config.get("field", "")
        operator = config.get("operator", "equals")
        value = config.get("value")

        if not field_name:
            return record

        actual_value = self._get_nested_value(record, field_name)
        if actual_value is None:
            return None  # 字段不存在则过滤

        if self._compare(actual_value, operator, value):
            return record
        return None

    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """获取嵌套字段值（支持 dot.notation）"""
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        """执行比较操作"""
        try:
            if operator == "equals":
                return actual == expected
            elif operator == "not_equals":
                return actual != expected
            elif operator == "contains":
                return str(expected).lower() in str(actual).lower()
            elif operator == "not_contains":
                return str(expected).lower() not in str(actual).lower()
            elif operator == "greater_than":
                return float(actual) > float(expected)
            elif operator == "less_than":
                return float(actual) < float(expected)
            elif operator == "regex":
                return bool(re.search(str(expected), str(actual)))
            elif operator == "exists":
                return actual is not None
            elif operator == "not_exists":
                return actual is None
            elif operator == "in":
                return actual in expected if isinstance(expected, list) else False
            elif operator == "starts_with":
                return str(actual).startswith(str(expected))
            elif operator == "ends_with":
                return str(actual).endswith(str(expected))
            else:
                return True  # 未知操作符默认通过
        except (ValueError, TypeError):
            return False


class MapTransform(TransformOperation):
    """映射转换 — 对字段值进行映射/计算"""

    @property
    def name(self) -> str:
        return "map"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        mappings = config.get("mappings", {})
        record = copy.deepcopy(record)

        for target_field, mapping in mappings.items():
            if isinstance(mapping, dict):
                source_field = mapping.get("from", target_field)
                operation = mapping.get("operation", "copy")
                default = mapping.get("default", "")

                source_value = self._get_nested_value(record, source_field)
                if source_value is None:
                    source_value = default

                if operation == "copy":
                    self._set_nested_value(record, target_field, source_value)
                elif operation == "upper":
                    self._set_nested_value(record, target_field, str(source_value).upper())
                elif operation == "lower":
                    self._set_nested_value(record, target_field, str(source_value).lower())
                elif operation == "strip":
                    self._set_nested_value(record, target_field, str(source_value).strip())
                elif operation == "int":
                    try:
                        self._set_nested_value(record, target_field, int(source_value))
                    except (ValueError, TypeError):
                        self._set_nested_value(record, target_field, default)
                elif operation == "float":
                    try:
                        self._set_nested_value(record, target_field, float(source_value))
                    except (ValueError, TypeError):
                        self._set_nested_value(record, target_field, default)
                elif operation == "template":
                    template = mapping.get("template", "")
                    self._set_nested_value(
                        record, target_field,
                        self._apply_template(template, record)
                    )
                elif operation == "rename":
                    # rename操作：从source_field读取值，写入target_field，删除source_field
                    if source_field in record:
                        record[target_field] = record.pop(source_field)
            elif isinstance(mapping, str):
                # 简写形式: "field_name" 表示从该字段复制值
                source_value = self._get_nested_value(record, mapping)
                if source_value is not None:
                    self._set_nested_value(record, target_field, source_value)

        return record

    def _get_nested_value(self, data: Dict, path: str) -> Any:
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _set_nested_value(self, data: Dict, path: str, value: Any) -> None:
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def _apply_template(self, template: str, record: Dict) -> str:
        """简单的模板渲染（支持 {{field}} 语法）"""
        def replacer(match):
            field = match.group(1).strip()
            val = self._get_nested_value(record, field)
            return str(val) if val is not None else match.group(0)
        return re.sub(r'\{\{(.+?)\}\}', replacer, template)


class RenameTransform(TransformOperation):
    """重命名转换 — 批量重命名字段"""

    @property
    def name(self) -> str:
        return "rename"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        rename_map = config.get("fields", {})
        record = copy.deepcopy(record)

        for old_name, new_name in rename_map.items():
            if old_name in record:
                record[new_name] = record.pop(old_name)

        return record


class SortTransform(TransformOperation):
    """排序转换 — 对批量记录排序（在transform_batch中生效）"""

    @property
    def name(self) -> str:
        return "sort"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        return record  # 单条记录不做处理

    def transform_batch(self, records: List[Dict], config: Dict) -> List[Dict]:
        field = config.get("field", "")
        reverse = config.get("reverse", False)

        if not field:
            return records

        def sort_key(r):
            val = r.get(field)
            if val is None:
                return (1, "")  # None值排到最后
            try:
                return (0, float(val))
            except (ValueError, TypeError):
                return (0, str(val))

        return sorted(records, key=sort_key, reverse=reverse)


class DeduplicateTransform(TransformOperation):
    """去重转换 — 根据指定字段去重"""

    @property
    def name(self) -> str:
        return "deduplicate"

    def __init__(self):
        self._seen: set = set()

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        fields = config.get("fields", [])
        if not fields:
            fields = list(record.keys())

        key = tuple(str(record.get(f, "")) for f in fields)
        if key in self._seen:
            return None
        self._seen.add(key)
        return record

    def transform_batch(self, records: List[Dict], config: Dict) -> List[Dict]:
        self._seen = set()  # 重置已见记录
        return super().transform_batch(records, config)


class FlattenTransform(TransformOperation):
    """扁平化转换 — 将嵌套结构扁平化"""

    @property
    def name(self) -> str:
        return "flatten"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        separator = config.get("separator", "_")
        max_depth = config.get("max_depth", 10)
        record = copy.deepcopy(record)
        return self._flatten(record, separator, max_depth)

    def _flatten(self, data: Dict, separator: str, max_depth: int, prefix: str = "") -> Dict:
        result = {}
        for key, value in data.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key
            if isinstance(value, dict) and max_depth > 0:
                nested = self._flatten(value, separator, max_depth - 1, new_key)
                result.update(nested)
            elif isinstance(value, list):
                result[new_key] = json.dumps(value, ensure_ascii=False)
            else:
                result[new_key] = value
        return result


class ExtractTransform(TransformOperation):
    """提取转换 — 从文本字段中提取信息"""

    @property
    def name(self) -> str:
        return "extract"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        source_field = config.get("from", "content")
        patterns = config.get("patterns", {})
        record = copy.deepcopy(record)

        source_value = str(record.get(source_field, ""))

        for target_field, pattern_config in patterns.items():
            if isinstance(pattern_config, str):
                pattern = pattern_config
                group = 0
            elif isinstance(pattern_config, dict):
                pattern = pattern_config.get("pattern", "")
                group = pattern_config.get("group", 0)
            else:
                continue

            match = re.search(pattern, source_value)
            if match:
                try:
                    record[target_field] = match.group(group)
                except IndexError:
                    record[target_field] = match.group(0)
            else:
                record[target_field] = ""

        return record


class AggregateTransform(TransformOperation):
    """聚合转换 — 对批量记录进行聚合统计"""

    @property
    def name(self) -> str:
        return "aggregate"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        return record

    def transform_batch(self, records: List[Dict], config: Dict) -> List[Dict]:
        if not records:
            return []

        aggregations = config.get("operations", [])
        group_by = config.get("group_by", "")

        if not group_by:
            # 无分组，全局聚合
            result = {"_aggregate_type": "global"}
            for agg in aggregations:
                field = agg.get("field", "")
                op = agg.get("operation", "count")
                target = agg.get("as", f"{op}_{field}")

                values = [r.get(field) for r in records if r.get(field) is not None]
                result[target] = self._compute_aggregate(values, op)

            return [result]
        else:
            # 按字段分组聚合
            groups: Dict[str, List[Dict]] = {}
            for r in records:
                key = str(r.get(group_by, "unknown"))
                groups.setdefault(key, []).append(r)

            results = []
            for key, group_records in groups.items():
                result = {"_aggregate_type": "grouped", group_by: key, "_count": len(group_records)}
                for agg in aggregations:
                    field = agg.get("field", "")
                    op = agg.get("operation", "count")
                    target = agg.get("as", f"{op}_{field}")

                    values = [r.get(field) for r in group_records if r.get(field) is not None]
                    result[target] = self._compute_aggregate(values, op)

                results.append(result)

            return results

    def _compute_aggregate(self, values: List, operation: str) -> Any:
        """执行聚合计算"""
        if not values:
            return 0

        if operation == "count":
            return len(values)
        elif operation == "sum":
            try:
                return sum(float(v) for v in values)
            except (ValueError, TypeError):
                return 0
        elif operation == "avg":
            try:
                return sum(float(v) for v in values) / len(values)
            except (ValueError, TypeError):
                return 0
        elif operation == "min":
            try:
                return min(float(v) for v in values)
            except (ValueError, TypeError):
                return min(str(v) for v in values)
        elif operation == "max":
            try:
                return max(float(v) for v in values)
            except (ValueError, TypeError):
                return max(str(v) for v in values)
        elif operation == "unique_count":
            return len(set(str(v) for v in values))
        elif operation == "concat":
            return " ".join(str(v) for v in values)
        else:
            return len(values)


class MergeTransform(TransformOperation):
    """合并转换 — 合并多个字段或添加新字段"""

    @property
    def name(self) -> str:
        return "merge"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        record = copy.deepcopy(record)
        additions = config.get("fields", {})

        for field_name, value in additions.items():
            if isinstance(value, str) and "{{" in value:
                # 模板渲染
                def replacer(match):
                    f = match.group(1).strip()
                    return str(record.get(f, match.group(0)))
                value = re.sub(r'\{\{(.+?)\}\}', replacer, value)
            record[field_name] = value

        return record


class TemplateTransform(TransformOperation):
    """模板转换 — 使用模板生成新内容"""

    @property
    def name(self) -> str:
        return "template"

    def transform(self, record: Dict, config: Dict) -> Optional[Dict]:
        template = config.get("template", "")
        output_field = config.get("output", "_template_output")
        record = copy.deepcopy(record)

        def replacer(match):
            field = match.group(1).strip()
            val = record.get(field)
            if val is None:
                # 尝试嵌套访问
                parts = field.split(".")
                current = record
                for p in parts:
                    if isinstance(current, dict) and p in current:
                        current = current[p]
                    else:
                        return match.group(0)
                return str(current)
            return str(val)

        record[output_field] = re.sub(r'\{\{(.+?)\}\}', replacer, template)
        return record


# ─── 转换操作注册表 ──────────────────────────────────────────
TRANSFORM_OPERATIONS: Dict[str, TransformOperation] = {
    "filter": FilterTransform(),
    "map": MapTransform(),
    "rename": RenameTransform(),
    "sort": SortTransform(),
    "deduplicate": DeduplicateTransform(),
    "flatten": FlattenTransform(),
    "extract": ExtractTransform(),
    "aggregate": AggregateTransform(),
    "merge": MergeTransform(),
    "template": TemplateTransform(),
}


def get_transform(name: str) -> TransformOperation:
    """根据名称获取转换操作"""
    op = TRANSFORM_OPERATIONS.get(name)
    if op is None:
        raise ValueError(
            f"Unsupported transform: '{name}'. "
            f"Supported: {list(TRANSFORM_OPERATIONS.keys())}"
        )
    return op
