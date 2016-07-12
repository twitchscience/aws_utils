import json
import logging
import unittest

import context_logging

try:
    from StringIO import StringIO  # Python 2
except ImportError:
    from io import StringIO  # Python 3


class TestJsonLogging(unittest.TestCase):
    def test_formatting_and_adapter(self):
        logger = logging.getLogger('logging-test')
        logger.setLevel(logging.DEBUG)
        buffer = StringIO()

        log_handler = logging.StreamHandler(buffer)
        log_handler.setFormatter(context_logging._JsonLevelFormatter())
        logger.addHandler(log_handler)

        adapted_logger = context_logging.Adapter(logger)

        msg = "testing logging format"
        adapted_logger.info(msg)
        log_json = json.loads(buffer.getvalue())

        self.assertEqual(log_json["message"], msg)
        self.assertEqual(log_json["levelname"], "INFO")
        self.assertTrue(log_json["caller"].startswith("test_context_logging.py"))
        for k in ["env", "pid", "host", "timestamp"]:
            self.assertIn(k, log_json)

if __name__ == "__main__":
    unittest.main()
