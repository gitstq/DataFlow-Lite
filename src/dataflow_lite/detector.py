"""
增量检测模块 — 智能检测数据源变更，支持多种检测策略
Incremental detection module — intelligently detects data source changes with multiple strategies
"""

import hashlib
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class FileFingerprint:
    """文件指纹 — 用于变更检测"""
    path: str
    hash: str = ""
    size: int = 0
    mtime: float = 0.0
    last_processed: float = 0.0

    def to_tuple(self) -> tuple:
        return (self.path, self.hash, self.size, self.mtime, self.last_processed)


@dataclass
class ChangeResult:
    """变更检测结果"""
    added: List[str] = None      # 新增文件
    modified: List[str] = None   # 修改文件
    deleted: List[str] = None    # 删除文件
    unchanged: List[str] = None  # 未变更文件
    total_scanned: int = 0       # 总扫描文件数
    scan_time: float = 0.0       # 扫描耗时（秒）

    def __post_init__(self):
        self.added = self.added or []
        self.modified = self.modified or []
        self.deleted = self.deleted or []
        self.unchanged = self.unchanged or []

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.modified or self.deleted)

    @property
    def change_count(self) -> int:
        return len(self.added) + len(self.modified) + len(self.deleted)

    def summary(self) -> str:
        parts = []
        if self.added:
            parts.append(f"+{len(self.added)} added")
        if self.modified:
            parts.append(f"~{len(self.modified)} modified")
        if self.deleted:
            parts.append(f"-{len(self.deleted)} deleted")
        if self.unchanged:
            parts.append(f"={len(self.unchanged)} unchanged")
        return ", ".join(parts) if parts else "No changes"


class IncrementalDetector:
    """增量变更检测器 Incremental Change Detector"""

    def __init__(self, db_path: str, strategy: str = "hash"):
        """
        初始化检测器

        Args:
            db_path: 状态数据库路径
            strategy: 检测策略 — hash(哈希比对) | mtime(修改时间) | size(文件大小)
        """
        self.db_path = db_path
        self.strategy = strategy
        self._init_db()

    def _init_db(self) -> None:
        """初始化指纹存储表"""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_fingerprints (
                    path TEXT PRIMARY KEY,
                    hash TEXT DEFAULT '',
                    size INTEGER DEFAULT 0,
                    mtime REAL DEFAULT 0.0,
                    last_processed REAL DEFAULT 0.0
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_fp_path
                ON file_fingerprints(path)
            """)
            conn.commit()

    def compute_hash(self, file_path: str, algorithm: str = "sha256") -> str:
        """计算文件内容哈希值"""
        h = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def get_fingerprint(self, file_path: str) -> Optional[FileFingerprint]:
        """获取已存储的文件指纹"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT path, hash, size, mtime, last_processed FROM file_fingerprints WHERE path = ?",
                (file_path,)
            ).fetchone()

        if row:
            return FileFingerprint(path=row[0], hash=row[1], size=row[2],
                                   mtime=row[3], last_processed=row[4])
        return None

    def save_fingerprint(self, fp: FileFingerprint) -> None:
        """保存文件指纹"""
        fp.last_processed = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO file_fingerprints
                (path, hash, size, mtime, last_processed)
                VALUES (?, ?, ?, ?, ?)
            """, fp.to_tuple())
            conn.commit()

    def save_fingerprints_batch(self, fps: List[FileFingerprint]) -> None:
        """批量保存文件指纹"""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            for fp in fps:
                fp.last_processed = now
                conn.execute("""
                    INSERT OR REPLACE INTO file_fingerprints
                    (path, hash, size, mtime, last_processed)
                    VALUES (?, ?, ?, ?, ?)
                """, fp.to_tuple())
            conn.commit()

    def get_all_stored_paths(self) -> Set[str]:
        """获取所有已存储的文件路径"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT path FROM file_fingerprints").fetchall()
        return {r[0] for r in rows}

    def is_changed(self, file_path: str) -> bool:
        """检测单个文件是否发生变更"""
        if not os.path.exists(file_path):
            return True

        stored = self.get_fingerprint(file_path)
        if stored is None:
            return True  # 新文件

        stat = os.stat(file_path)

        if self.strategy == "hash":
            current_hash = self.compute_hash(file_path)
            return current_hash != stored.hash
        elif self.strategy == "mtime":
            return stat.st_mtime != stored.mtime
        elif self.strategy == "size":
            return stat.st_size != stored.size
        else:
            # 默认使用hash
            current_hash = self.compute_hash(file_path)
            return current_hash != stored.hash

    def detect_changes(self, paths: List[str]) -> ChangeResult:
        """
        检测一组文件的变更情况

        Args:
            paths: 待检测的文件路径列表

        Returns:
            ChangeResult 变更检测结果
        """
        start_time = time.time()
        result = ChangeResult()
        current_paths = set()

        for path in paths:
            if not os.path.exists(path):
                continue
            current_paths.add(path)

            stored = self.get_fingerprint(path)
            stat = os.stat(path)

            if stored is None:
                # 新文件
                result.added.append(path)
            else:
                # 检测是否变更
                changed = False
                if self.strategy == "hash":
                    current_hash = self.compute_hash(path)
                    changed = current_hash != stored.hash
                    if changed:
                        stored.hash = current_hash
                elif self.strategy == "mtime":
                    changed = stat.st_mtime != stored.mtime
                elif self.strategy == "size":
                    changed = stat.st_size != stored.size

                if changed:
                    result.modified.append(path)
                else:
                    result.unchanged.append(path)

            # 更新指纹
            new_fp = FileFingerprint(
                path=path,
                hash=self.compute_hash(path) if self.strategy == "hash" or stored is None else stored.hash,
                size=stat.st_size,
                mtime=stat.st_mtime,
            )
            self.save_fingerprint(new_fp)

        # 检测已删除的文件
        stored_paths = self.get_all_stored_paths()
        deleted = stored_paths - current_paths
        result.deleted = list(deleted)

        # 清理已删除文件的指纹记录
        if deleted:
            with sqlite3.connect(self.db_path) as conn:
                placeholders = ",".join("?" * len(deleted))
                conn.execute(
                    f"DELETE FROM file_fingerprints WHERE path IN ({placeholders})",
                    list(deleted)
                )
                conn.commit()

        result.total_scanned = len(current_paths)
        result.scan_time = time.time() - start_time
        return result

    def scan_directory(
        self,
        directory: str,
        exclude_patterns: Optional[List[str]] = None,
        file_extensions: Optional[List[str]] = None
    ) -> List[str]:
        """
        递归扫描目录，返回所有匹配的文件路径

        Args:
            directory: 目标目录
            exclude_patterns: 排除的文件模式列表
            file_extensions: 只包含的文件扩展名列表（如 [".py", ".md"]）

        Returns:
            匹配的文件路径列表
        """
        import fnmatch

        exclude_patterns = exclude_patterns or []
        matched_files = []

        for root, dirs, files in os.walk(directory):
            # 应用排除模式到目录
            dirs[:] = [
                d for d in dirs
                if not any(fnmatch.fnmatch(d, pat) for pat in exclude_patterns)
            ]

            for fname in files:
                # 跳过排除的文件
                if any(fnmatch.fnmatch(fname, pat) for pat in exclude_patterns):
                    continue

                # 过滤扩展名
                if file_extensions:
                    _, ext = os.path.splitext(fname)
                    if ext.lower() not in [e.lower() for e in file_extensions]:
                        continue

                full_path = os.path.join(root, fname)
                matched_files.append(full_path)

        return matched_files

    def reset(self) -> int:
        """重置所有指纹记录，返回删除数量"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM file_fingerprints")
            conn.commit()
            return cursor.rowcount

    def get_stats(self) -> Dict:
        """获取指纹存储统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM file_fingerprints").fetchone()[0]
            latest = conn.execute(
                "SELECT MAX(last_processed) FROM file_fingerprints"
            ).fetchone()[0]
        return {
            "total_files_tracked": total,
            "last_processed": latest or 0,
            "detection_strategy": self.strategy,
        }
