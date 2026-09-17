import tempfile
import unittest
from adaptive_offline_kv import AdaptiveKV


class TestAdaptiveKV(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.kv = AdaptiveKV(self.tmp.name)

    def tearDown(self):
        self.kv.close()
        self.tmp.cleanup()

    def test_put_get(self):
        self.kv.put("a", {"x": 1})
        self.assertEqual(self.kv.get("a"), {"x": 1})

    def test_delete(self):
        self.kv.put("b", 42)
        self.kv.delete("b")
        self.assertIsNone(self.kv.get("b"))

    def test_persistence(self):
        self.kv.put("persist", "hello")
        self.kv.close()
        kv2 = AdaptiveKV(self.tmp.name)
        self.assertEqual(kv2.get("persist"), "hello")
        kv2.close()


if __name__ == "__main__":
    unittest.main()
