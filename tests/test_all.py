# DataFlow-Lite Test Suite

import os
import sys
import json
import tempfile
import unittest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dataflow_lite.config import EngineConfig
from src.dataflow_lite.detector import IncrementalDetector, ChangeResult
from src.dataflow_lite.lineage import LineageTracker, LineageNode, LineageEdge
from src.dataflow_lite.sources import (
    get_source_adapter, FileSourceAdapter, CSVSourceAdapter,
    JSONSourceAdapter, DirectorySourceAdapter
)
from src.dataflow_lite.transforms import (
    get_transform, FilterTransform, MapTransform, RenameTransform,
    SortTransform, DeduplicateTransform, FlattenTransform,
    AggregateTransform, MergeTransform, TemplateTransform
)
from src.dataflow_lite.sinks import get_sink_adapter, JSONSinkAdapter, CSVSinkAdapter, FileSinkAdapter
from src.dataflow_lite.pipeline import parse_yaml_simple, load_pipeline_config, Pipeline
from src.dataflow_lite.engine import DataFlowEngine


class TestConfig(unittest.TestCase):
    """测试配置模块"""

    def test_default_config(self):
        config = EngineConfig()
        self.assertEqual(config.state_dir, ".dataflow")
        self.assertEqual(config.change_detection, "hash")
        self.assertFalse(config.force_full)

    def test_config_serialization(self):
        config = EngineConfig(verbose=True, workers=4)
        d = config.to_dict()
        self.assertTrue(d["verbose"])
        self.assertEqual(d["workers"], 4)

    def test_config_from_dict(self):
        data = {"verbose": True, "workers": 8, "change_detection": "mtime"}
        config = EngineConfig.from_dict(data)
        self.assertTrue(config.verbose)
        self.assertEqual(config.workers, 8)
        self.assertEqual(config.change_detection, "mtime")


class TestYAMLParser(unittest.TestCase):
    """测试YAML解析器"""

    def test_simple_key_value(self):
        yaml_text = "name: test\nversion: 1.0"
        result = parse_yaml_simple(yaml_text)
        self.assertEqual(result["name"], "test")
        self.assertEqual(result["version"], "1.0")

    def test_nested_structure(self):
        yaml_text = "name: test\nsettings:\n  key: value\n  num: 42"
        result = parse_yaml_simple(yaml_text)
        self.assertEqual(result["settings"]["key"], "value")
        self.assertEqual(result["settings"]["num"], 42)

    def test_list(self):
        yaml_text = "items:\n  - apple\n  - banana\n  - cherry"
        result = parse_yaml_simple(yaml_text)
        self.assertEqual(result["items"], ["apple", "banana", "cherry"])

    def test_boolean_values(self):
        yaml_text = "enabled: true\ndisabled: false"
        result = parse_yaml_simple(yaml_text)
        self.assertTrue(result["enabled"])
        self.assertFalse(result["disabled"])

    def test_comments_and_empty_lines(self):
        yaml_text = "# comment\nname: test\n\n# another comment\nversion: 1.0"
        result = parse_yaml_simple(yaml_text)
        self.assertEqual(result["name"], "test")
        self.assertEqual(result["version"], "1.0")


class TestDetector(unittest.TestCase):
    """测试增量检测模块"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.detector = IncrementalDetector(self.db_path, strategy="hash")

    def test_new_file_detected(self):
        # 创建新文件
        test_file = os.path.join(self.tmpdir, "new.txt")
        with open(test_file, "w") as f:
            f.write("hello world")

        result = self.detector.detect_changes([test_file])
        self.assertTrue(result.has_changes)
        self.assertEqual(len(result.added), 1)

    def test_unchanged_file(self):
        test_file = os.path.join(self.tmpdir, "stable.txt")
        with open(test_file, "w") as f:
            f.write("stable content")

        # 第一次检测
        self.detector.detect_changes([test_file])
        # 第二次检测
        result = self.detector.detect_changes([test_file])
        self.assertFalse(result.has_changes)
        self.assertEqual(len(result.unchanged), 1)

    def test_modified_file(self):
        test_file = os.path.join(self.tmpdir, "modify.txt")
        with open(test_file, "w") as f:
            f.write("version 1")

        self.detector.detect_changes([test_file])

        # 修改文件
        with open(test_file, "w") as f:
            f.write("version 2")

        result = self.detector.detect_changes([test_file])
        self.assertTrue(result.has_changes)
        self.assertEqual(len(result.modified), 1)

    def test_deleted_file(self):
        test_file = os.path.join(self.tmpdir, "delete.txt")
        with open(test_file, "w") as f:
            f.write("to be deleted")

        self.detector.detect_changes([test_file])
        os.remove(test_file)

        result = self.detector.detect_changes([])
        self.assertTrue(len(result.deleted) >= 1)

    def test_directory_scan(self):
        # 创建测试目录结构
        subdir = os.path.join(self.tmpdir, "src")
        os.makedirs(subdir)
        for name in ["a.py", "b.py", "c.txt"]:
            with open(os.path.join(self.tmpdir, name), "w") as f:
                f.write("content")
            with open(os.path.join(subdir, name), "w") as f:
                f.write("content")

        files = self.detector.scan_directory(self.tmpdir, file_extensions=[".py"])
        self.assertEqual(len(files), 4)  # 2 in root + 2 in subdir

    def test_mtime_detection(self):
        mtime_detector = IncrementalDetector(
            os.path.join(self.tmpdir, "mtime.db"), strategy="mtime"
        )
        test_file = os.path.join(self.tmpdir, "mtime_test.txt")
        with open(test_file, "w") as f:
            f.write("content")

        mtime_detector.detect_changes([test_file])
        result = mtime_detector.detect_changes([test_file])
        self.assertFalse(result.has_changes)


class TestLineage(unittest.TestCase):
    """测试数据血缘模块"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "lineage.db")
        self.tracker = LineageTracker(self.db_path)

    def test_add_and_query_node(self):
        node = LineageNode(id="test_node", name="Test", node_type="source", pipeline_id="p1")
        self.tracker.add_node(node)

        lineage = self.tracker.get_full_lineage("test_node")
        self.assertEqual(lineage["node_id"], "test_node")

    def test_upstream_downstream(self):
        self.tracker.add_node(LineageNode(id="src", name="Source", node_type="source", pipeline_id="p1"))
        self.tracker.add_node(LineageNode(id="tfm", name="Transform", node_type="transform", pipeline_id="p1"))
        self.tracker.add_node(LineageNode(id="snk", name="Sink", node_type="sink", pipeline_id="p1"))

        self.tracker.add_edge(LineageEdge(source_id="src", target_id="tfm"))
        self.tracker.add_edge(LineageEdge(source_id="tfm", target_id="snk"))

        upstream = self.tracker.get_upstream("snk")
        self.assertEqual(len(upstream), 1)
        self.assertEqual(upstream[0].id, "tfm")

        downstream = self.tracker.get_downstream("src")
        self.assertEqual(len(downstream), 1)
        self.assertEqual(downstream[0].id, "tfm")

    def test_pipeline_lineage(self):
        self.tracker.add_node(LineageNode(id="p1.s1", name="S1", node_type="source", pipeline_id="p1"))
        self.tracker.add_node(LineageNode(id="p1.s2", name="S2", node_type="sink", pipeline_id="p1"))
        self.tracker.add_edge(LineageEdge(source_id="p1.s1", target_id="p1.s2"))

        lineage = self.tracker.get_pipeline_lineage("p1")
        self.assertEqual(lineage["stats"]["total_nodes"], 2)
        self.assertEqual(lineage["stats"]["total_edges"], 1)

    def test_export_json(self):
        self.tracker.add_node(LineageNode(id="n1", name="N1", node_type="source", pipeline_id="p1"))
        export_path = os.path.join(self.tmpdir, "lineage.json")
        self.tracker.export_lineage_json(export_path)
        self.assertTrue(os.path.exists(export_path))

        with open(export_path) as f:
            data = json.load(f)
        self.assertIn("p1", data["pipelines"])


class TestTransforms(unittest.TestCase):
    """测试数据转换模块"""

    def test_filter_equals(self):
        op = FilterTransform()
        record = {"name": "Alice", "status": "active"}
        result = op.transform(record, {"field": "status", "operator": "equals", "value": "active"})
        self.assertIsNotNone(result)

        result = op.transform(record, {"field": "status", "operator": "equals", "value": "inactive"})
        self.assertIsNone(result)

    def test_filter_contains(self):
        op = FilterTransform()
        record = {"title": "Hello World Python"}
        result = op.transform(record, {"field": "title", "operator": "contains", "value": "python"})
        self.assertIsNotNone(result)

    def test_map_upper(self):
        op = MapTransform()
        record = {"name": "alice", "email": "ALICE@EXAMPLE.COM"}
        result = op.transform(record, {
            "mappings": {
                "name_upper": {"from": "name", "operation": "upper"},
                "email_lower": {"from": "email", "operation": "lower"},
            }
        })
        self.assertEqual(result["name_upper"], "ALICE")
        self.assertEqual(result["email_lower"], "alice@example.com")

    def test_rename(self):
        op = RenameTransform()
        record = {"old_name": "value", "keep": "same"}
        result = op.transform(record, {"fields": {"old_name": "new_name"}})
        self.assertEqual(result["new_name"], "value")
        self.assertNotIn("old_name", result)

    def test_sort(self):
        op = SortTransform()
        records = [
            {"name": "Charlie", "age": "30"},
            {"name": "Alice", "age": "25"},
            {"name": "Bob", "age": "35"},
        ]
        result = op.transform_batch(records, {"field": "age"})
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[2]["name"], "Bob")

    def test_deduplicate(self):
        op = DeduplicateTransform()
        records = [
            {"id": "1", "name": "A"},
            {"id": "2", "name": "B"},
            {"id": "1", "name": "A"},
            {"id": "3", "name": "C"},
        ]
        result = op.transform_batch(records, {"fields": ["id"]})
        self.assertEqual(len(result), 3)

    def test_flatten(self):
        op = FlattenTransform()
        record = {"user": {"name": "Alice", "age": 30}, "active": True}
        result = op.transform(record, {"separator": "_"})
        self.assertIn("user_name", result)
        self.assertIn("user_age", result)

    def test_aggregate(self):
        op = AggregateTransform()
        records = [
            {"type": "A", "value": "10"},
            {"type": "A", "value": "20"},
            {"type": "B", "value": "30"},
        ]
        result = op.transform_batch(records, {
            "group_by": "type",
            "operations": [
                {"field": "value", "operation": "sum", "as": "total"},
                {"field": "value", "operation": "count", "as": "cnt"},
            ]
        })
        self.assertEqual(len(result), 2)

    def test_merge(self):
        op = MergeTransform()
        record = {"name": "Alice"}
        result = op.transform(record, {
            "fields": {
                "status": "active",
                "display": "{{name}} (active)",
            }
        })
        self.assertEqual(result["status"], "active")
        self.assertEqual(result["display"], "Alice (active)")

    def test_template(self):
        op = TemplateTransform()
        record = {"title": "Hello", "count": 42}
        result = op.transform(record, {
            "template": "Title: {{title}}, Count: {{count}}",
            "output": "formatted",
        })
        self.assertEqual(result["formatted"], "Title: Hello, Count: 42")


class TestSourcesAndSinks(unittest.TestCase):
    """测试数据源和输出模块"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_file_source(self):
        test_file = os.path.join(self.tmpdir, "test.txt")
        with open(test_file, "w") as f:
            f.write("Hello World")

        adapter = FileSourceAdapter()
        records = list(adapter.read({"path": test_file}))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["content"], "Hello World")

    def test_csv_source(self):
        csv_file = os.path.join(self.tmpdir, "test.csv")
        with open(csv_file, "w", newline="") as f:
            f.write("name,age\nAlice,30\nBob,25\n")

        adapter = CSVSourceAdapter()
        records = list(adapter.read({"path": csv_file}))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["name"], "Alice")

    def test_json_source(self):
        json_file = os.path.join(self.tmpdir, "test.json")
        with open(json_file, "w") as f:
            json.dump([{"name": "A"}, {"name": "B"}], f)

        adapter = JSONSourceAdapter()
        records = list(adapter.read({"path": json_file}))
        self.assertEqual(len(records), 2)

    def test_directory_source(self):
        os.makedirs(os.path.join(self.tmpdir, "sub"))
        for name in ["a.txt", "b.md"]:
            with open(os.path.join(self.tmpdir, name), "w") as f:
                f.write("content")
        with open(os.path.join(self.tmpdir, "sub", "c.txt"), "w") as f:
            f.write("content")

        adapter = DirectorySourceAdapter()
        records = list(adapter.read({"path": self.tmpdir, "recursive": True}))
        self.assertEqual(len(records), 3)

    def test_json_sink(self):
        output_path = os.path.join(self.tmpdir, "out.json")
        adapter = JSONSinkAdapter()
        count = adapter.write(
            [{"name": "A", "value": 1}, {"name": "B", "value": 2}],
            {"path": output_path}
        )
        self.assertEqual(count, 2)
        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as f:
            data = json.load(f)
        self.assertEqual(len(data), 2)

    def test_csv_sink(self):
        output_path = os.path.join(self.tmpdir, "out.csv")
        adapter = CSVSinkAdapter()
        count = adapter.write(
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            {"path": output_path}
        )
        self.assertEqual(count, 2)

    def test_file_sink(self):
        output_path = os.path.join(self.tmpdir, "out.txt")
        adapter = FileSinkAdapter()
        count = adapter.write(
            [{"content": "Line 1"}, {"content": "Line 2"}],
            {"path": output_path, "template": "{{content}}"}
        )
        self.assertEqual(count, 2)

        with open(output_path) as f:
            lines = f.read().strip().split("\n")
        self.assertEqual(lines, ["Line 1", "Line 2"])


class TestPipeline(unittest.TestCase):
    """测试管线解析与执行"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_pipeline_from_dict(self):
        data = {
            "name": "test_pipeline",
            "steps": [
                {"name": "s1", "type": "source", "source": "file", "path": "/tmp/test.txt"},
                {"name": "s2", "type": "sink", "sink": "file", "path": "/tmp/out.txt"},
            ]
        }
        pipeline = Pipeline.from_dict(data)
        self.assertEqual(pipeline.name, "test_pipeline")
        self.assertEqual(len(pipeline.steps), 2)

    def test_pipeline_validation(self):
        # 缺少source和sink
        data = {
            "name": "bad_pipeline",
            "steps": [
                {"name": "t1", "type": "transform", "operation": "map"},
            ]
        }
        pipeline = Pipeline.from_dict(data)
        errors = pipeline.validate()
        self.assertTrue(len(errors) > 0)

    def test_full_pipeline_execution(self):
        # 创建输入文件
        input_file = os.path.join(self.tmpdir, "input.txt")
        with open(input_file, "w") as f:
            f.write("hello world")

        output_file = os.path.join(self.tmpdir, "output.txt")

        # 创建管线配置
        config_data = {
            "name": "test_exec",
            "steps": [
                {"name": "read", "type": "source", "source": "file", "path": input_file},
                {"name": "upper", "type": "transform", "operation": "map",
                 "mappings": {"content": {"from": "content", "operation": "upper"}}},
                {"name": "write", "type": "sink", "sink": "file",
                 "path": output_file, "template": "{{content}}"},
            ]
        }
        pipeline = Pipeline.from_dict(config_data)

        engine = DataFlowEngine(EngineConfig(state_dir=os.path.join(self.tmpdir, ".dataflow")))
        result = engine.run_pipeline(pipeline)

        self.assertEqual(result.status, "success")
        self.assertTrue(os.path.exists(output_file))

        with open(output_file) as f:
            content = f.read().strip()
        self.assertEqual(content, "HELLO WORLD")

    def test_csv_pipeline(self):
        # 创建CSV输入
        csv_file = os.path.join(self.tmpdir, "data.csv")
        with open(csv_file, "w", newline="") as f:
            f.write("name,status,value\nAlice,active,100\nBob,inactive,200\nCharlie,active,150\n")

        json_file = os.path.join(self.tmpdir, "result.json")

        config_data = {
            "name": "csv_test",
            "steps": [
                {"name": "read_csv", "type": "source", "source": "csv", "path": csv_file},
                {"name": "filter", "type": "transform", "operation": "filter",
                 "field": "status", "operator": "equals", "value": "active"},
                {"name": "export", "type": "sink", "sink": "json", "path": json_file},
            ]
        }
        pipeline = Pipeline.from_dict(config_data)
        engine = DataFlowEngine(EngineConfig(state_dir=os.path.join(self.tmpdir, ".df")))
        result = engine.run_pipeline(pipeline)

        self.assertEqual(result.status, "success")

        with open(json_file) as f:
            data = json.load(f)
        self.assertEqual(len(data), 2)  # Alice and Charlie

    def test_incremental_skip(self):
        # 创建输入文件
        input_file = os.path.join(self.tmpdir, "stable.txt")
        with open(input_file, "w") as f:
            f.write("unchanged content")

        output_file = os.path.join(self.tmpdir, "out.txt")
        state_dir = os.path.join(self.tmpdir, ".df")

        config_data = {
            "name": "incremental_test",
            "steps": [
                {"name": "read", "type": "source", "source": "file", "path": input_file},
                {"name": "write", "type": "sink", "sink": "file",
                 "path": output_file, "template": "{{content}}"},
            ]
        }
        pipeline = Pipeline.from_dict(config_data)
        engine = DataFlowEngine(EngineConfig(state_dir=state_dir))

        # 第一次执行
        result1 = engine.run_pipeline(pipeline)
        self.assertEqual(result1.status, "success")

        # 第二次执行（应跳过）
        result2 = engine.run_pipeline(pipeline)
        self.assertEqual(result2.status, "skipped")


if __name__ == "__main__":
    unittest.main()
