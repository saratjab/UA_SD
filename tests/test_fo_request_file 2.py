from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from WM_FO.request_file import load_requests


class RequestFileTests(unittest.TestCase):
    def load_text(self, content: str):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.jsonl"
            path.write_text(content, encoding="utf-8")
            return load_requests(path)

    def test_accepts_each_mode_and_no_mode(self) -> None:
        requests = self.load_text(
            '{"ws_id":"WS_A","duration_seconds":3}\n'
            '{"ws_id":"WS_B","target_volume_liters":1.25}\n'
            '{"ws_id":"WS_C"}\n'
        )
        self.assertEqual(requests[0]["duration_seconds"], 3)
        self.assertEqual(requests[1]["target_volume_liters"], 1.25)
        self.assertNotIn("duration_seconds", requests[2])

    def test_accepts_legacy_json_array(self) -> None:
        self.assertEqual(self.load_text('[{"ws_id":"WS_A","duration_seconds":2}]')[0]["ws_id"], "WS_A")

    def test_rejects_malformed_records_unknown_fields_and_invalid_modes(self) -> None:
        cases = [
            ('{"ws_id":"WS_A"}\nnot-json\n', "line 2"),
            ('{"ws_id":"WS_A","wat":1}', "unknown field"),
            ('{"ws_id":"WS_A","duration_seconds":2,"target_volume_liters":1}', "choose"),
            ('{"ws_id":"WS_A","duration_seconds":0}', "positive whole-number"),
            ('{"ws_id":"WS_A","target_volume_liters":NaN}', "positive target_volume"),
            ('{"ws_id":"WS_A","duration_seconds":null}', "positive whole-number"),
            ('# comment only\n\n', "at least one"),
        ]
        for content, expected_error in cases:
            with self.subTest(content=content), self.assertRaisesRegex(ValueError, expected_error):
                self.load_text(content)

    def test_committed_sample_parses(self) -> None:
        sample = Path(__file__).resolve().parents[1] / "examples" / "automated_irrigation_requests.jsonl"
        self.assertEqual(len(load_requests(sample)), 3)


if __name__ == "__main__":
    unittest.main()
