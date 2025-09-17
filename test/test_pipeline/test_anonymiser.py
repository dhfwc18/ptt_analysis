# test/test_pipeline/test_anonymiser.py
"""Unit tests for the UserAnonymiser class."""

import os
import tempfile
import unittest

import pandas as pd

from ptt_crawler.anonymiser import UserAnonymiser


class TestUserAnonymiser(unittest.TestCase):
    def setUp(self):
        self.anonymiser = UserAnonymiser(seed=42)

    def test_hash_method(self):
        name = "test_user"
        anon_id = self.anonymiser.anonymise_name(name)
        self.assertTrue(anon_id.startswith("user_"))
        self.assertEqual(len(anon_id), 13)  # "user_" + 8 chars
        self.assertEqual(self.anonymiser.deanonymise_id(anon_id), name)

    def test_random_string_method(self):
        anonymiser = UserAnonymiser(method="random_string", seed=42)
        name = "test_user"
        anon_id = anonymiser.anonymise_name(name)
        self.assertTrue(anon_id.startswith("user_"))
        self.assertEqual(len(anon_id), 13)
        self.assertEqual(anonymiser.deanonymise_id(anon_id), name)

    def test_uuid_method(self):
        anonymiser = UserAnonymiser(method="uuid", seed=42)
        name = "test_user"
        anon_id = anonymiser.anonymise_name(name)
        self.assertTrue(anon_id.startswith("user_"))
        self.assertEqual(len(anon_id), 13)
        self.assertEqual(anonymiser.deanonymise_id(anon_id), name)

    def test_sequential_method(self):
        anonymiser = UserAnonymiser(method="sequential", seed=42)
        names = ["user1", "user2", "user3"]
        anon_ids = [anonymiser.anonymise_name(name) for name in names]
        self.assertEqual(anon_ids, ["user_00000001", "user_00000002", "user_00000003"])

    def test_numeric_method(self):
        anonymiser = UserAnonymiser(method="numeric", seed=42)
        name = "test_user"
        anon_id = anonymiser.anonymise_name(name)
        self.assertEqual(len(anon_id), 8)
        self.assertTrue(anon_id.isdigit())

    def test_unknown_values(self):
        self.assertEqual(self.anonymiser.anonymise_name(None), "UNKNOWN")
        self.assertEqual(self.anonymiser.anonymise_name(""), "UNKNOWN")
        self.assertEqual(self.anonymiser.anonymise_name(pd.NA), "UNKNOWN")

    def test_dataframe_anonymisation(self):
        df = pd.DataFrame({"username": ["user1", "user2", "user3"], "data": [1, 2, 3]})
        anon_df = self.anonymiser.anonymise_dataframe(df, "username")
        self.assertFalse(anon_df["username"].equals(df["username"]))
        deanon_df = self.anonymiser.deanonymise_dataframe(anon_df, "username")
        self.assertTrue(deanon_df["username"].equals(df["username"]))

    def test_mapping_persistence(self):

        name = "test_user"
        anon_id = self.anonymiser.anonymise_name(name)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tf:
            self.anonymiser.save_mapping(tf.name)
            new_anonymiser = UserAnonymiser()
            new_anonymiser.load_mapping(tf.name)

        os.unlink(tf.name)

        self.assertEqual(new_anonymiser.anonymise_name(name), anon_id)
        self.assertEqual(new_anonymiser.deanonymise_id(anon_id), name)

    def test_invalid_method(self):
        with self.assertRaises(ValueError):
            UserAnonymiser(method="invalid")

    def test_consistency(self):
        name = "test_user"
        anon_id1 = self.anonymiser.anonymise_name(name)
        anon_id2 = self.anonymiser.anonymise_name(name)
        self.assertEqual(anon_id1, anon_id2)


if __name__ == "__main__":
    unittest.main()
