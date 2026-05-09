"""
数据血缘追踪模块 — 记录和管理数据依赖关系图
Data lineage tracking module — records and manages data dependency graphs
"""

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class LineageNode:
    """血缘节点 — 代表一个数据实体"""
    id: str
    name: str
    node_type: str  # source | transform | target
    pipeline_id: str = ""
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class LineageEdge:
    """血缘边 — 代表数据流向关系"""
    source_id: str
    target_id: str
    edge_type: str = "data_flow"  # data_flow | dependency | derived_from
    transform_name: str = ""
    metadata: Dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class LineageTracker:
    """数据血缘追踪器 Data Lineage Tracker"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._nodes_cache: Dict[str, LineageNode] = {}
        self._edges_cache: List[LineageEdge] = []
        self._init_db()

    def _init_db(self) -> None:
        """初始化血缘数据库表"""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lineage_nodes (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    node_type TEXT NOT NULL,
                    pipeline_id TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    created_at REAL,
                    updated_at REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lineage_edges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    edge_type TEXT DEFAULT 'data_flow',
                    transform_name TEXT DEFAULT '',
                    metadata TEXT DEFAULT '{}',
                    created_at REAL,
                    FOREIGN KEY (source_id) REFERENCES lineage_nodes(id),
                    FOREIGN KEY (target_id) REFERENCES lineage_nodes(id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_edges_source
                ON lineage_edges(source_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_edges_target
                ON lineage_edges(target_id)
            """)
            conn.commit()

    def add_node(self, node: LineageNode) -> None:
        """添加或更新血缘节点"""
        node.updated_at = time.time()
        self._nodes_cache[node.id] = node
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO lineage_nodes
                (id, name, node_type, pipeline_id, metadata, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                node.id, node.name, node.node_type, node.pipeline_id,
                json.dumps(node.metadata, ensure_ascii=False),
                node.created_at, node.updated_at
            ))
            conn.commit()

    def add_edge(self, edge: LineageEdge) -> None:
        """添加血缘边（数据流向关系）"""
        self._edges_cache.append(edge)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO lineage_edges
                (source_id, target_id, edge_type, transform_name, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                edge.source_id, edge.target_id, edge.edge_type,
                edge.transform_name,
                json.dumps(edge.metadata, ensure_ascii=False),
                edge.created_at
            ))
            conn.commit()

    def get_upstream(self, node_id: str) -> List[LineageNode]:
        """获取某节点的所有上游节点（数据来源）"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT n.id, n.name, n.node_type, n.pipeline_id, n.metadata, n.created_at, n.updated_at
                FROM lineage_nodes n
                JOIN lineage_edges e ON n.id = e.source_id
                WHERE e.target_id = ?
            """, (node_id,)).fetchall()

        return [
            LineageNode(
                id=r[0], name=r[1], node_type=r[2], pipeline_id=r[3],
                metadata=json.loads(r[4]) if r[4] else {},
                created_at=r[5], updated_at=r[6]
            )
            for r in rows
        ]

    def get_downstream(self, node_id: str) -> List[LineageNode]:
        """获取某节点的所有下游节点（数据去向）"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT n.id, n.name, n.node_type, n.pipeline_id, n.metadata, n.created_at, n.updated_at
                FROM lineage_nodes n
                JOIN lineage_edges e ON n.id = e.target_id
                WHERE e.source_id = ?
            """, (node_id,)).fetchall()

        return [
            LineageNode(
                id=r[0], name=r[1], node_type=r[2], pipeline_id=r[3],
                metadata=json.loads(r[4]) if r[4] else {},
                created_at=r[5], updated_at=r[6]
            )
            for r in rows
        ]

    def get_full_lineage(self, node_id: str) -> Dict:
        """获取某节点的完整血缘链路（上游+下游）"""
        return {
            "node_id": node_id,
            "upstream": [
                {"id": n.id, "name": n.name, "type": n.node_type}
                for n in self.get_upstream(node_id)
            ],
            "downstream": [
                {"id": n.id, "name": n.name, "type": n.node_type}
                for n in self.get_downstream(node_id)
            ],
        }

    def get_pipeline_lineage(self, pipeline_id: str) -> Dict:
        """获取某条管线的完整血缘图"""
        with sqlite3.connect(self.db_path) as conn:
            nodes = conn.execute("""
                SELECT id, name, node_type, pipeline_id, metadata, created_at, updated_at
                FROM lineage_nodes WHERE pipeline_id = ?
            """, (pipeline_id,)).fetchall()

            edges = conn.execute("""
                SELECT e.source_id, e.target_id, e.edge_type, e.transform_name
                FROM lineage_edges e
                JOIN lineage_nodes n ON e.source_id = n.id
                WHERE n.pipeline_id = ?
            """, (pipeline_id,)).fetchall()

        return {
            "pipeline_id": pipeline_id,
            "nodes": [
                {
                    "id": r[0], "name": r[1], "type": r[2],
                    "metadata": json.loads(r[4]) if r[4] else {}
                }
                for r in nodes
            ],
            "edges": [
                {"from": r[0], "to": r[1], "type": r[2], "transform": r[3]}
                for r in edges
            ],
            "stats": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
            }
        }

    def get_all_pipelines(self) -> List[str]:
        """获取所有管线ID列表"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT pipeline_id FROM lineage_nodes WHERE pipeline_id != ''"
            ).fetchall()
        return [r[0] for r in rows]

    def export_lineage_json(self, output_path: str) -> None:
        """导出完整血缘图为JSON文件"""
        all_data = {"pipelines": {}}
        for pid in self.get_all_pipelines():
            all_data["pipelines"][pid] = self.get_pipeline_lineage(pid)

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

    def clear_pipeline(self, pipeline_id: str) -> int:
        """清除指定管线的所有血缘数据，返回删除的节点数"""
        with sqlite3.connect(self.db_path) as conn:
            # 先删除相关边
            conn.execute("""
                DELETE FROM lineage_edges
                WHERE source_id IN (SELECT id FROM lineage_nodes WHERE pipeline_id = ?)
                   OR target_id IN (SELECT id FROM lineage_nodes WHERE pipeline_id = ?)
            """, (pipeline_id, pipeline_id))
            # 再删除节点
            cursor = conn.execute(
                "DELETE FROM lineage_nodes WHERE pipeline_id = ?",
                (pipeline_id,)
            )
            conn.commit()
            return cursor.rowcount
