from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import mkdtemp

from app.rag.evaluation import SynonymDictionary


class RagSynonymTests(unittest.TestCase):
    def test_default_synonym_config_contains_business_terms(self) -> None:
        path = Path("config/synonyms.json")
        self.assertTrue(path.exists())

        synonyms = SynonymDictionary.from_file(path)
        rewritten = synonyms.rewrite("count employees by department")
        self.assertIn("员工", rewritten)
        self.assertIn("部门", rewritten)

    def test_synonym_dictionary_supports_connection_and_domain_scopes(self) -> None:
        path = Path(mkdtemp(prefix="rag_synonyms_test_")) / "synonyms.json"
        path.write_text(
            json.dumps(
                {
                    "global": {"用户": ["customer"]},
                    "connections": {"sales-db": {"订单": ["purchase"]}},
                    "domains": {"hr": {"员工": ["staff"]}},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        synonyms = SynonymDictionary.from_file(path)
        self.assertEqual(synonyms.rewrite("customer overview"), "用户 overview")
        self.assertEqual(synonyms.rewrite("purchase amount", connection_id="sales-db"), "订单 amount")
        self.assertEqual(synonyms.rewrite("staff count", domain="hr"), "员工 count")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
