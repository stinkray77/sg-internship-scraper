import unittest

from categories import classify_job_categories, primary_category
import main


class CategoryTests(unittest.TestCase):
    def test_returns_all_matches_in_fixed_order(self):
        categories = classify_job_categories({
            "title": "Quant Machine Learning Software Engineer Intern",
        })

        self.assertEqual(categories, ["QUANT", "AI/ML", "SWE"])

    def test_quant_firm_tag_classifies_non_quant_technical_role(self):
        categories = classify_job_categories({
            "title": "Data Engineering Intern",
            "source_tags": ["QUANT"],
        })

        self.assertEqual(categories, ["QUANT", "DATA"])

    def test_unmatched_technical_title_uses_fallback(self):
        self.assertEqual(
            classify_job_categories({"title": "Technology Summer Analyst"}),
            ["TECH"],
        )

    def test_ai_does_not_match_inside_normal_words(self):
        self.assertEqual(
            classify_job_categories({"title": "Retail Technology Intern"}),
            ["TECH"],
        )

    def test_primary_category_uses_display_order(self):
        self.assertEqual(
            primary_category({"categories": ["SWE", "QUANT", "AI/ML"]}),
            "QUANT",
        )

    def test_registry_recognizes_quant_firm_name_variants(self):
        self.assertTrue(main.is_known_quant_company("Citadel Securities"))
        self.assertTrue(main.is_known_quant_company("Point72"))
        self.assertFalse(main.is_known_quant_company("Stripe"))


if __name__ == "__main__":
    unittest.main()
