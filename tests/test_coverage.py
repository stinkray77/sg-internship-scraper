import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock
import main
from eligibility import assess_eligibility
from job_content import overseas_eligible,ambiguous_technical_student
from source_adapters import fetch_source,load_source_registry

ROOT=Path(__file__).parent


class CoverageTests(unittest.TestCase):
    def test_saved_live_board_contracts_normalize(self):
        registry={s['id']:s for s in load_source_registry()}
        employers=json.loads((ROOT.parent/'employers.json').read_text())['employers']
        self.assertEqual(sum(e['industry']=='fund' for e in employers),25)
        self.assertEqual(sum(e['industry']=='tech' for e in employers),25)
        for row in employers:
            if row['coverage']!='direct monitoring':continue
            with self.subTest(company=row['company']):
                fixture=json.loads((ROOT/'fixtures'/'coverage'/(row['id']+'.json')).read_text())
                response=MagicMock();response.json.return_value=fixture['response']
                result=fetch_source(registry[row['source_id']],MagicMock(return_value=response))
                self.assertIsNone(result.error)
                self.assertTrue(result.candidates)
                self.assertTrue(all(c.title and c.job_url for c in result.candidates))

    def test_thirty_verified_posting_benchmark(self):
        rows=json.loads((ROOT/'fixtures'/'coverage_benchmark.json').read_text())
        self.assertGreaterEqual(len(rows),30)
        registry={s['id']:s for s in load_source_registry()}
        for row in rows:
            job=dict(row['job']);title=job['title'];source=registry[job['source_id']]
            with self.subTest(company=job['company'],title=title):
                quant='QUANT' in source.get('tags',[])
                role=(main.is_quant_intern_role(title) if quant else main.is_target_role(title)) or (not main.excluded_ambiguous_title(title) and ambiguous_technical_student(title,job.get('description')))
                undergrad=assess_eligibility(title,job.get('description')).verdict!='ineligible'
                support=overseas_eligible(job,main.is_singapore_job(job),quant)
                self.assertEqual(role,row['expected_role'])
                self.assertEqual(undergrad,row['expected_undergrad'])
                self.assertEqual(role and undergrad and support,row['expected_alert'])
