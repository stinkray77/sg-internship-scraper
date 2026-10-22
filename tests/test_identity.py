import unittest
from identity import aliases_for, canonical_url, compatible, identity_metadata, observation_identity


class IdentityTests(unittest.TestCase):
    def test_tracking_does_not_change_application_identity(self):
        self.assertEqual(canonical_url('http://example.com/job/1?utm_source=x&jobId=2'),
                         'https://example.com/job/1?jobId=2')

    def test_distinct_requisitions_do_not_merge_by_title(self):
        base = dict(company='Firm', title='Software Intern', location='Singapore', source_id='greenhouse_firm')
        self.assertFalse(compatible(identity_metadata({**base, 'external_id': '1'}),
                                    identity_metadata({**base, 'external_id': '2'})))

    def test_jobspy_ids_distinguish_same_title(self):
        self.assertNotEqual(observation_identity({'site': 'indeed', 'id': '1'}),
                            observation_identity({'site': 'indeed', 'id': '2'}))

    def test_location_fallback_is_conservative(self):
        self.assertFalse(compatible({'company': 'a', 'location': 'london'},
                                    {'company': 'a', 'location': 'singapore'}))

    def test_distinct_application_links_are_not_collapsed_by_title(self):
        first = dict(company='firm', title='intern', url='https://firm.example/jobs/1')
        self.assertFalse(compatible(first, {**first, 'url':'https://firm.example/jobs/2'}))
