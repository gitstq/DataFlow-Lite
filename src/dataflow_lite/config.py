"""
核心配置模块 — 管理引擎全局配置与常量定义
Core configuration module — manages engine-wide settings and constants
"""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ─── 默认配置常量 ────────────────────────────────────────────
DEFAULT_STATE_DIR = ".dataflow"
DEFAULT_STATE_DB = "state.db"
DEFAULT_CONFIG_FILE = "dataflow.yaml"
DEFAULT_LOG_DIR = "logs"
MAX_RETRIES = 3
HASH_ALGORITHM = "sha256"
CHUNK_SIZE = 8192  # 文件hash计算分块大小

# 支持的数据源类型
SUPPORTED_SOURCES = ["file", "csv", "json", "markdown", "directory"]

# 支持的输出目标类型
SUPPORTED_TARGETS = ["file", "csv", "json", "sqlite", "directory"]

# 支持的转换操作
SUPPORTED_TRANSFORMS = [
    "filter", "map", "rename", "sort", "deduplicate",
    "flatten", "merge", "extract", "aggregate", "template"
]


@dataclass
class EngineConfig:
    """引擎运行配置 Engine runtime configuration"""

    # 状态存储目录
    state_dir: str = DEFAULT_STATE_DIR

    # 状态数据库名称
    state_db: str = DEFAULT_STATE_DB

    # 日志目录
    log_dir: str = DEFAULT_LOG_DIR

    # 是否启用详细日志
    verbose: bool = False

    # 并发处理数
    workers: int = 1

    # 是否强制全量重建（忽略增量）
    force_full: bool = False

    # 文件变更检测策略: hash | mtime | size
    change_detection: str = "hash"

    # 自定义环境变量映射
    env_vars: Dict[str, str] = field(default_factory=dict)

    # 排除的文件模式（glob格式）
    exclude_patterns: List[str] = field(default_factory=lambda: [
        ".git", "__pycache__", "*.pyc", ".DS_Store",
        "node_modules", ".dataflow", "*.tmp"
    ])

    def get_state_db_path(self) -> str:
        """获取状态数据库完整路径"""
        return os.path.join(self.state_dir, self.state_db)

    def get_log_dir_path(self) -> str:
        """获取日志目录完整路径"""
        return os.path.join(self.state_dir, self.log_dir)

    def to_dict(self) -> dict:
        """序列化为字典"""
        return {
            "state_dir": self.state_dir,
            "state_db": self.state_db,
            "log_dir": self.log_dir,
            "verbose": self.verbose,
            "workers": self.workers,
            "force_full": self.force_full,
            "change_detection": self.change_detection,
            "env_vars": self.env_vars,
            "exclude_patterns": self.exclude_patterns,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EngineConfig":
        """从字典反序列化"""
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_file(cls, path: str) -> "EngineConfig":
        """从JSON配置文件加载"""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)

    def save(self, path: str) -> None:
        """保存配置到JSON文件"""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
