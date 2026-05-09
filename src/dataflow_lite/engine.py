"""
核心引擎模块 — 管线执行引擎，协调数据源、转换和输出
Core engine module — pipeline execution engine coordinating sources, transforms, and sinks
"""

import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .config import EngineConfig
from .detector import IncrementalDetector, ChangeResult
from .lineage import LineageTracker, LineageNode, LineageEdge
from .sources import get_source_adapter
from .transforms import get_transform
from .sinks import get_sink_adapter
from .pipeline import Pipeline, PipelineStep, load_pipeline_config


@dataclass
class StepResult:
    """单步执行结果"""
    step_name: str
    step_type: str
    status: str = "success"  # success | skipped | error
    input_count: int = 0
    output_count: int = 0
    error_message: str = ""
    duration: float = 0.0
    details: Dict = field(default_factory=dict)


@dataclass
class PipelineResult:
    """管线执行结果"""
    pipeline_name: str
    status: str = "success"
    total_steps: int = 0
    successful_steps: int = 0
    skipped_steps: int = 0
    failed_steps: int = 0
    total_records_in: int = 0
    total_records_out: int = 0
    duration: float = 0.0
    step_results: List[StepResult] = field(default_factory=list)
    change_result: Optional[ChangeResult] = None
    error_message: str = ""

    def summary(self) -> str:
        """生成执行摘要"""
        lines = [
            f"Pipeline: {self.pipeline_name}",
            f"Status: {self.status}",
            f"Duration: {self.duration:.2f}s",
            f"Steps: {self.successful_steps}/{self.total_steps} succeeded",
            f"Records: {self.total_records_in} in → {self.total_records_out} out",
        ]
        if self.change_result:
            lines.append(f"Changes: {self.change_result.summary()}")
        return " | ".join(lines)


class DataFlowEngine:
    """数据流引擎核心 DataFlow Engine Core"""

    def __init__(self, config: Optional[EngineConfig] = None):
        self.config = config or EngineConfig()
        self._detector: Optional[IncrementalDetector] = None
        self._lineage: Optional[LineageTracker] = None
        self._ensure_state_dir()

    def _ensure_state_dir(self) -> None:
        """确保状态目录存在"""
        os.makedirs(self.config.state_dir, exist_ok=True)
        os.makedirs(self.config.get_log_dir_path(), exist_ok=True)

    @property
    def detector(self) -> IncrementalDetector:
        """懒加载增量检测器"""
        if self._detector is None:
            db_path = self.config.get_state_db_path()
            self._detector = IncrementalDetector(
                db_path=db_path,
                strategy=self.config.change_detection
            )
        return self._detector

    @property
    def lineage(self) -> LineageTracker:
        """懒加载血缘追踪器"""
        if self._lineage is None:
            db_path = self.config.get_state_db_path()
            self._lineage = LineageTracker(db_path=db_path)
        return self._lineage

    def run_pipeline(self, pipeline: Pipeline) -> PipelineResult:
        """
        执行完整管线

        Args:
            pipeline: 管线定义

        Returns:
            PipelineResult 执行结果
        """
        start_time = time.time()
        result = PipelineResult(
            pipeline_name=pipeline.name,
            total_steps=len(pipeline.steps),
        )

        # 验证管线
        errors = pipeline.validate()
        if errors:
            result.status = "error"
            result.error_message = "; ".join(errors)
            return result

        # 注册血缘节点
        self._register_pipeline_lineage(pipeline)

        # 收集所有源文件路径（用于增量检测）
        source_paths = self._collect_source_paths(pipeline)

        # 增量检测
        change_result = None
        if source_paths and not self.config.force_full:
            change_result = self.detector.detect_changes(source_paths)
            result.change_result = change_result

            if not change_result.has_changes:
                result.status = "skipped"
                result.duration = time.time() - start_time
                result.skipped_steps = len(pipeline.steps)
                return result

        # 执行管线步骤
        current_data: List[Dict] = []

        for step in pipeline.steps:
            step_start = time.time()
            step_result = StepResult(
                step_name=step.name,
                step_type=step.step_type,
            )

            try:
                if step.step_type == "source":
                    current_data = self._execute_source(step, change_result)
                    step_result.output_count = len(current_data)

                elif step.step_type == "transform":
                    if change_result and not self.config.force_full:
                        # 增量模式：只处理变更的数据
                        current_data = self._execute_transform(step, current_data)
                    else:
                        current_data = self._execute_transform(step, current_data)
                    step_result.input_count = step_result.input_count or len(current_data) + step_result.output_count
                    step_result.output_count = len(current_data)

                elif step.step_type == "sink":
                    written = self._execute_sink(step, current_data)
                    step_result.input_count = len(current_data)
                    step_result.output_count = written

                step_result.status = "success"
                result.successful_steps += 1

            except Exception as e:
                step_result.status = "error"
                step_result.error_message = str(e)
                result.failed_steps += 1
                result.status = "error"
                result.error_message = f"Step '{step.name}' failed: {e}"

            step_result.duration = time.time() - step_start
            result.step_results.append(step_result)
            result.total_records_out = len(current_data)

            # 如果出错且非容错模式，终止管线
            if step_result.status == "error":
                break

        result.total_records_in = result.step_results[0].output_count if result.step_results else 0
        result.total_records_out = len(current_data)
        result.duration = time.time() - start_time

        if result.failed_steps == 0:
            result.status = "success"

        return result

    def run_from_file(self, config_path: str) -> PipelineResult:
        """从YAML配置文件加载并执行管线"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Pipeline config not found: {config_path}")

        config_data = load_pipeline_config(config_path)
        pipeline = Pipeline.from_dict(config_data)
        return self.run_pipeline(pipeline)

    def _collect_source_paths(self, pipeline: Pipeline) -> List[str]:
        """收集管线中所有源文件的路径"""
        paths = []
        for step in pipeline.steps:
            if step.step_type == "source":
                source_config = step.config
                source_type = source_config.get("source", source_config.get("type", "file"))

                if source_type == "directory":
                    dir_path = source_config.get("path", "")
                    if os.path.isdir(dir_path):
                        paths.extend(self.detector.scan_directory(
                            dir_path,
                            exclude_patterns=self.config.exclude_patterns,
                            file_extensions=source_config.get("extensions"),
                        ))
                else:
                    path = source_config.get("path", "")
                    if path and os.path.exists(path):
                        paths.append(path)

        return paths

    def _execute_source(self, step: PipelineStep, change_result: Optional[ChangeResult]) -> List[Dict]:
        """执行数据源步骤"""
        source_config = step.config
        source_type = source_config.get("source", source_config.get("type", "file"))
        adapter = get_source_adapter(source_type)

        # 如果是增量模式且有变更信息，过滤未变更的文件
        if change_result and not self.config.force_full and source_type in ("file", "markdown"):
            path = source_config.get("path", "")
            if path not in change_result.modified and path not in change_result.added:
                return []  # 文件未变更，跳过

        records = list(adapter.read(source_config))
        return records

    def _execute_transform(self, step: PipelineStep, records: List[Dict]) -> List[Dict]:
        """执行转换步骤"""
        transform_config = step.config
        operation_name = transform_config.get("operation", transform_config.get("transform", ""))
        transform = get_transform(operation_name)

        # 提取转换参数（排除元数据字段）
        params = {k: v for k, v in transform_config.items()
                  if k not in ("name", "type", "operation", "transform")}

        # 判断是否需要批量处理
        if operation_name in ("sort", "aggregate", "deduplicate"):
            return transform.transform_batch(records, params)
        else:
            return transform.transform_batch(records, params)

    def _execute_sink(self, step: PipelineStep, records: List[Dict]) -> int:
        """执行输出步骤"""
        sink_config = step.config
        sink_type = sink_config.get("sink", sink_config.get("type", sink_config.get("target", "file")))
        adapter = get_sink_adapter(sink_type)

        # 提取输出参数
        params = {k: v for k, v in sink_config.items()
                  if k not in ("name", "type", "sink", "target")}

        return adapter.write(records, params)

    def _register_pipeline_lineage(self, pipeline: Pipeline) -> None:
        """注册管线的血缘节点"""
        for step in pipeline.steps:
            node = LineageNode(
                id=f"{pipeline.name}.{step.name}",
                name=step.name,
                node_type=step.step_type,
                pipeline_id=pipeline.name,
                metadata={"config": {k: v for k, v in step.config.items()
                                     if not isinstance(v, (dict, list))}},
            )
            self.lineage.add_node(node)

        # 注册边（数据流向）
        for i in range(len(pipeline.steps) - 1):
            edge = LineageEdge(
                source_id=f"{pipeline.name}.{pipeline.steps[i].name}",
                target_id=f"{pipeline.name}.{pipeline.steps[i + 1].name}",
            )
            self.lineage.add_edge(edge)

    def get_stats(self) -> Dict:
        """获取引擎统计信息"""
        return {
            "detector": self.detector.get_stats(),
            "pipelines": self.lineage.get_all_pipelines(),
            "state_dir": self.config.state_dir,
        }

    def reset_state(self) -> Dict:
        """重置引擎状态"""
        detector_reset = self.detector.reset()
        return {
            "fingerprints_cleared": detector_reset,
            "status": "success",
        }
