from unittest import TestCase

from gentleman.helpers import prepare_query

class TestPrepareQuery(TestCase):

    def test_bool_to_int(self):
        d = {"test": True}
        prepare_query(d)
        self.assertEqual(d["test"], 1)
        self.assertEqual(type(d["test"]), int)
