import datetime as dt
import unittest
from job_content import ambiguous_technical_student, extract_content, overseas_eligible, support_evidence


class ContentTests(unittest.TestCase):
    def test_ambiguous_role_requires_technical_task(self):
        self.assertTrue(ambiguous_technical_student('Summer Analyst', 'Develop software applications using Python.'))
        self.assertFalse(ambiguous_technical_student('Summer Analyst', 'We are a technology company. Excel skills required.'))
        self.assertFalse(ambiguous_technical_student('Software Engineer', 'Develop software applications.'))

    def test_relocation_is_not_visa_support(self):
        job = dict(location='Sydney', description='We provide relocation assistance.', job_url='https://firm.example/job')
        self.assertFalse(overseas_eligible(job, False, False))
        self.assertEqual(job['visa_support'], 'unknown')
        self.assertEqual(job['support_evidence'][0]['kind'], 'relocation')

    def test_conditional_sponsorship_is_labeled(self):
        job = dict(location='Hong Kong', description='Visa sponsorship may be available subject to eligibility.')
        self.assertTrue(overseas_eligible(job, False, False))
        self.assertEqual(job['visa_support'], 'conditional')

    def test_negative_posting_overrides_programme_policy(self):
        now = dt.datetime.now(dt.timezone.utc)
        policy = dict(company='Firm', programme_terms=['intern'], offices=['Sydney'], url='https://firm.example/policy',
                      verified_at=now.isoformat(), evidence=[dict(kind='visa', status='available', excerpt='Visa sponsorship is available')])
        job = dict(company='Firm', title='Software Intern', location='Sydney', description='We do not sponsor visas.')
        self.assertFalse(overseas_eligible(job, False, False, [policy]))

    def test_quant_remains_broad(self):
        job = dict(location='London', description='')
        self.assertTrue(overseas_eligible(job, False, True))
        self.assertEqual(job['visa_support'], 'unknown')

    def test_policy_scope_and_expiry(self):
        policy = dict(company='Firm', programme_terms=['intern'], offices=['Sydney'], url='https://firm.example/policy',
                      verified_at='2020-01-01T00:00:00+00:00', evidence=[dict(kind='visa', status='available')])
        self.assertFalse(overseas_eligible(dict(company='Firm', title='Intern', location='Sydney'), False, False, [policy]))
        policy['verified_at'] = dt.datetime.now(dt.timezone.utc).isoformat()
        self.assertFalse(overseas_eligible(dict(company='Firm', title='Intern', location='London'), False, False, [policy]))
        self.assertTrue(overseas_eligible(dict(company='Firm', title='Intern', location='Sydney'), False, False, [policy]))

    def test_excerpts_preserve_source_wording(self):
        content = extract_content('<p>Develop software applications using Python and SQL.</p><p>Apply by 30 November 2026.</p>')
        self.assertEqual(content['responsibilities'], ['Develop software applications using Python and SQL.'])
        self.assertEqual(content['skills'], ['Python', 'SQL'])
        self.assertIn('30 November', content['application_deadline'])
