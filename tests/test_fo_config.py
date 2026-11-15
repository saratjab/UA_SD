from __future__ import annotations

import unittest
from unittest import mock

from WM_FO.config import parse_args

BASE = ["--kafka-host", "broker", "--kafka-port", "9092"]


class FieldOperatorConfigTests(unittest.TestCase):
    def test_reserved_identities_cannot_be_used_by_a_field_operator(self) -> None:
        for reserved in ("CENTRAL", "LOCAL", "local"):
            with self.subTest(operator_id=reserved), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                parse_args(BASE + ["--operator-id", reserved])

    def test_registered_operator_id_is_accepted(self) -> None:
        self.assertEqual(parse_args(BASE + ["--operator-id", "OP_001"]).operator_id, "OP_001")


if __name__ == "__main__":
    unittest.main()
