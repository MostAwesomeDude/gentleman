from unittest import TestCase

from gentleman.helpers import itemgetters, prepare_query

class TestItemGetters(TestCase):

    def test_get_id(self):
        l = [
            {"id": 1, "unused": 2},
            {"id": 3, "unused": 4},
        ]

        self.assertEqual(itemgetters("id")(l), [1, 3])

class TestPrepareQuery(TestCase):

    def test_bool_to_int(self):
        d = {"test": True}
        prepare_query(d)
        self.assertEqual(d["test"], 1)
        self.assertEqual(type(d["test"]), int)
