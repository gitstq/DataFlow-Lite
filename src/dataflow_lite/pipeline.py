"""
管线定义与解析模块 — YAML声明式管线配置解析
Pipeline definition and parsing module — YAML declarative pipeline configuration
"""

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ─── 简易YAML解析器（零外部依赖）───────────────────────────────

def parse_yaml_simple(text: str) -> Any:
    """
    轻量级YAML解析器，支持常见YAML子集语法
    Lightweight YAML parser supporting common YAML subset syntax

    支持:
    - 键值对 (key: value)
    - 嵌套缩进
    - 列表 (- item)
    - 字符串/数字/布尔值/null
    - 多行字符串
    - 注释
    """
    lines = text.split("\n")
    return _parse_block(lines, 0, 0)[0]


def _get_indent(line: str) -> int:
    """获取行的缩进级别"""
    return len(line) - len(line.lstrip())


def _parse_value(value_str: str) -> Any:
    """解析单个值"""
    stripped = value_str.strip()

    if not stripped or stripped == "null" or stripped == "~":
        return None
    elif stripped == "true" or stripped == "yes" or stripped == "on":
        return True
    elif stripped == "false" or stripped == "no" or stripped == "off":
        return False

    # 尝试解析数字（但保留看起来像版本号的值如 "1.0.0" 为字符串）
    if stripped not in ("1.0", "0.0"):
        try:
            if "." in stripped and not re.match(r'^\d+\.\d+\.\d+', stripped):
                return float(stripped)
            elif stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
                return int(stripped)
        except ValueError:
            pass

    # 字符串：去除引号
    if (stripped.startswith('"') and stripped.endswith('"')) or \
       (stripped.startswith("'") and stripped.endswith("'")):
        return stripped[1:-1]

    return stripped


def _parse_block(lines: List[str], start: int, base_indent: int):
    """解析一个YAML块"""
    result = None
    i = start

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # 跳过空行和注释
        if not stripped or stripped.startswith("#"):
            i += 1
            continue

        current_indent = _get_indent(line)

        # 如果缩进回退，结束当前块
        if current_indent < base_indent:
            break

        # 列表项
        if stripped.startswith("- "):
            if result is None:
                result = []
            item_content = stripped[2:].strip()
            item_indent = current_indent

            # 检查列表项是否有子块
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                next_stripped = next_line.strip()
                if next_stripped and not next_stripped.startswith("#"):
                    next_indent = _get_indent(next_line)
                    if next_indent > item_indent:
                        # 列表项是字典
                        if ":" in item_content:
                            sub_value, sub_end = _parse_block(
                                [item_content] + lines[i + 1:],
                                0, item_indent
                            )
                            if isinstance(sub_value, dict):
                                result.append(sub_value)
                            i += sub_end
                            continue
                        else:
                            sub_value, sub_end = _parse_block(
                                lines, i + 1, next_indent
                            )
                            if isinstance(sub_value, list):
                                result.extend(sub_value)
                            i = i + 1 + sub_end
                            continue

            # 简单列表项值
            if ":" in item_content:
                # 列表项中的内联字典
                key, _, val = item_content.partition(":")
                result.append({key.strip(): _parse_value(val)})
            else:
                result.append(_parse_value(item_content))

            i += 1
            continue

        # 键值对
        if ":" in stripped:
            if result is None:
                result = {}

            key, _, value_str = stripped.partition(":")
            key = key.strip()
            value_str = value_str.strip()

            # 检查是否有子块
            if not value_str or value_str.startswith("#"):
                # 值是子块
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    next_stripped = next_line.strip()
                    if not next_stripped or next_stripped.startswith("#"):
                        i += 1
                        continue

                    next_indent = _get_indent(next_line)
                    if next_indent > current_indent:
                        sub_value, sub_end = _parse_block(lines, i + 1, next_indent)
                        result[key] = sub_value
                        i = i + 1 + sub_end
                        continue

                result[key] = None
            elif value_str.startswith("|") or value_str.startswith(">"):
                # 多行字符串
                multiline_lines = []
                i += 1
                while i < len(lines):
                    ml = lines[i]
                    if not ml.strip():
                        multiline_lines.append("")
                    elif _get_indent(ml) > current_indent:
                        multiline_lines.append(ml.strip())
                    else:
                        break
                    i += 1
                if value_str.startswith("|"):
                    result[key] = "\n".join(multiline_lines)
                else:
                    result[key] = " ".join(multiline_lines)
                continue
            elif value_str.startswith("["):
                # 内联列表
                items = re.findall(r'[^,\[\]]+', value_str)
                result[key] = [_parse_value(item.strip()) for item in items]
            elif value_str.startswith("{"):
                # 内联字典
                pairs = re.findall(r'(\w+)\s*:\s*([^,}]+)', value_str)
                result[key] = {k.strip(): _parse_value(v.strip()) for k, v in pairs}
            else:
                result[key] = _parse_value(value_str)

        i += 1

    return result, i - start


def load_pipeline_config(path: str) -> Dict:
    """从YAML文件加载管线配置"""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # 环境变量替换
    content = _resolve_env_vars(content)

    config = parse_yaml_simple(content)
    if not isinstance(config, dict):
        raise ValueError(f"Invalid pipeline config: root must be a mapping, got {type(config).__name__}")

    return config


def _resolve_env_vars(text: str) -> str:
    """替换YAML中的环境变量引用 ${ENV_VAR} """
    def replacer(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, match.group(0))
        return value

    return re.sub(r'\$\{(\w+)\}', replacer, text)


# ─── 管线数据结构 ─────────────────────────────────────────────

@dataclass
class PipelineStep:
    """管线步骤定义"""
    name: str
    step_type: str  # source | transform | sink
    config: Dict = field(default_factory=dict)


@dataclass
class Pipeline:
    """完整管线定义"""
    name: str
    description: str = ""
    version: str = "1.0.0"
    steps: List[PipelineStep] = field(default_factory=list)
    settings: Dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> "Pipeline":
        """从字典创建管线"""
        pipeline = cls(
            name=data.get("name", "unnamed"),
            description=data.get("description", ""),
            version=data.get("version", "1.0.0"),
            settings=data.get("settings", {}),
        )

        steps = data.get("steps", data.get("pipeline", []))
        if isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict):
                    # 自动推断步骤类型
                    step_type = step.get("type", "")
                    if not step_type:
                        if "source" in step or "path" in step:
                            step_type = "source"
                        elif "transform" in step or "operation" in step:
                            step_type = "transform"
                        elif "sink" in step or "target" in step:
                            step_type = "sink"

                    pipeline.steps.append(PipelineStep(
                        name=step.get("name", f"step_{len(pipeline.steps)}"),
                        step_type=step_type,
                        config=step,
                    ))

        return pipeline

    def validate(self) -> List[str]:
        """验证管线配置，返回错误列表"""
        errors = []
        has_source = False
        has_sink = False

        for i, step in enumerate(self.steps):
            if step.step_type == "source":
                has_source = True
                if "path" not in step.config and "paths" not in step.config:
                    errors.append(f"Step '{step.name}': source step missing 'path' or 'paths'")
            elif step.step_type == "transform":
                if "operation" not in step.config:
                    errors.append(f"Step '{step.name}': transform step missing 'operation'")
            elif step.step_type == "sink":
                has_sink = True
                if "path" not in step.config:
                    errors.append(f"Step '{step.name}': sink step missing 'path'")

        if not has_source:
            errors.append("Pipeline must have at least one source step")
        if not has_sink:
            errors.append("Pipeline must have at least one sink step")

        return errors
