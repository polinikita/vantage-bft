#!/usr/bin/env python3

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("recovery_report.py")
SPEC = importlib.util.spec_from_file_location("recovery_report", MODULE_PATH)
RECOVERY_REPORT = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(RECOVERY_REPORT)


def event(epoch_ms):
    return {"epoch_ms": epoch_ms}


class SustainedAttackDynamicsTests(unittest.TestCase):
    def test_service_catches_up_with_a_preexisting_backlog(self):
        open_maps = {
            0: {1: event(19_800), 2: event(25_000), 3: event(35_000)},
            1: {1: event(19_900), 2: event(25_100), 3: event(35_100)},
        }
        seal_maps = {
            0: {1: event(22_000), 2: event(30_000), 3: event(40_000)},
            1: {1: event(22_100), 2: event(30_100), 3: event(40_100)},
        }

        result = RECOVERY_REPORT.sustained_attack_dynamics(
            {1, 2, 3}, open_maps, seal_maps, 0, 60_000, 1_000
        )

        self.assertEqual(result["window_start_ms"], 20_000)
        self.assertEqual(result["window_end_ms"], 59_000)
        self.assertEqual(result["arrivals"], 2)
        self.assertEqual(result["all_correct_seals"], 3)
        self.assertEqual(result["backlog_at_start"], 1)
        self.assertEqual(result["backlog_at_end"], 0)
        self.assertEqual(result["backlog_net_growth"], -1)
        self.assertEqual(result["backlog_accounting_error"], 0)

    def test_missing_services_expose_backlog_growth(self):
        open_maps = {
            0: {1: event(25_000), 2: event(35_000)},
            1: {1: event(25_100), 2: event(35_100)},
        }
        seal_maps = {0: {}, 1: {}}

        result = RECOVERY_REPORT.sustained_attack_dynamics(
            {1, 2}, open_maps, seal_maps, 0, 60_000, 1_000
        )

        self.assertEqual(result["arrivals"], 2)
        self.assertEqual(result["all_correct_seals"], 0)
        self.assertEqual(result["backlog_at_start"], 0)
        self.assertEqual(result["backlog_at_end"], 2)
        self.assertEqual(result["backlog_net_growth"], 2)
        self.assertEqual(result["backlog_accounting_error"], 0)


if __name__ == "__main__":
    unittest.main()
