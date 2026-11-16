from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from cloud_analytics.anomalies import score_observation
from cloud_analytics.bronze import inspect_csv
from cloud_analytics.config import Settings
from cloud_analytics.contracts import CONTRACTS
from cloud_analytics.demo import DemoError, begin_work, clean, generate, read_manifest, workspace_for
from cloud_analytics.usage_stream import lock_at


class DemoTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.settings = Settings(self.root / "original", self.root / "lake", self.root / "evidence",
                                 "local[2]", "1g", 2, "UTC")

    def generate(self, demo_id="fixture", settings=None):
        workspace = workspace_for(settings or self.settings, demo_id)
        with lock_at(workspace.lock):
            self.assertEqual(generate(workspace)[0], "created")
        return workspace

    def test_reproducible_bytes_and_contracts(self):
        one = self.generate()
        two = self.generate(settings=replace(self.settings, lake_root=self.root / "other-lake"))
        manifest = read_manifest(one)
        self.assertEqual(manifest, read_manifest(two))
        for path in ["manifest.json", *(entry["path"] for entry in manifest["files"])]:
            self.assertEqual((one.dataset / path).read_bytes(), (two.dataset / path).read_bytes())
        landing = one.dataset / "datalake" / "landing"
        for source, contract in CONTRACTS.items():
            self.assertEqual(inspect_csv(landing / f"{source}.csv", contract)["input_rows"], manifest["csv_rows"][source])
        events = [json.loads(line) for path in sorted((landing / "usage_events_stream").glob("*.jsonl"))
                  for line in path.read_text().splitlines()]
        self.assertEqual(len(events), 17)
        self.assertEqual({row["schema_version"] for row in events}, {1, 2})
        self.assertEqual(len({row["event_id"] for row in events}), 16)
        self.assertTrue(all(row["org_id"] == manifest["queries"]["org_id"] for row in events))
        peak = next(row for row in events if row["timestamp"].startswith(manifest["cases"]["peak_date"]))
        baseline = [Decimal(row["cost_usd_increment"]) for row in events[:8]]
        result = score_observation(Decimal(peak["cost_usd_increment"]), baseline, None, 7, Decimal("3.5"))
        self.assertEqual(result[:2], ("above_threshold", True))
        self.assertEqual(generate(one)[0], "reused")
        (landing / "users.csv").write_text("changed")
        with self.assertRaisesRegex(DemoError, "demo_dataset_mismatch"):
            generate(one)

    def test_dry_run_apply_and_restart_preserve_other_data(self):
        demo = self.generate()
        another = self.generate("independent")
        begin_work(demo)
        (demo.work / "checkpoint").write_text("state")
        demo.reports.mkdir()
        (demo.reports / "first.json").write_text("record")
        for root in (self.settings.dataset_root, self.settings.evidence_root, self.settings.lake_root / "integration"):
            root.mkdir()
            (root / "guardian").write_text("keep")
        before = read_manifest(demo)
        self.assertEqual(clean(demo)["delete_local"], [str(demo.work)])
        self.assertTrue((demo.work / "checkpoint").exists())
        plan = clean(demo, apply=True)
        self.assertFalse(demo.work.exists())
        self.assertIn("preserved", plan["astra"])
        self.assertEqual(clean(demo, apply=True)["delete_local"], [])
        self.assertEqual(read_manifest(demo), before)
        self.assertEqual((demo.reports / "first.json").read_text(), "record")
        self.assertTrue(another.dataset.exists())
        for root in (self.settings.dataset_root, self.settings.evidence_root, self.settings.lake_root / "integration"):
            self.assertEqual((root / "guardian").read_text(), "keep")
        begin_work(demo)
        self.assertEqual(list(demo.work.iterdir()), [])

    def test_reject_invalid_unowned_and_linked_targets(self):
        for demo_id in ("../integration", "/lake", "usage_v1", ".locks"):
            with self.subTest(demo_id=demo_id), self.assertRaisesRegex(DemoError, "invalid_demo_id"):
                workspace_for(self.settings, demo_id)
        demo = self.generate()
        another = self.generate("second-demo")
        with self.assertRaisesRegex(DemoError, "demo_manifest_missing"):
            clean(workspace_for(self.settings, "unknown"), apply=True)
        demo.work.mkdir()
        with self.assertRaisesRegex(DemoError, "demo_work_not_owned"):
            clean(demo, apply=True)
        demo.work.rmdir()
        demo.work.symlink_to(another.dataset, target_is_directory=True)
        with self.assertRaisesRegex(DemoError, "unsafe_demo_path"):
            workspace_for(self.settings, demo.demo_id)
        demo.work.unlink()
        begin_work(demo)
        (demo.work / "external").symlink_to(another.dataset, target_is_directory=True)
        with self.assertRaisesRegex(DemoError, "unsafe_demo_work_tree"):
            clean(demo, apply=True)
        self.assertTrue(another.dataset.exists())
        overlap = replace(self.settings, dataset_root=demo.dataset)
        with self.assertRaisesRegex(DemoError, "demo_overlaps_protected_path"):
            workspace_for(overlap, demo.demo_id)
