import unittest
from dict_merge import merge_dictionaries


class TestMerge(unittest.TestCase):
    json_text = {"a": 1, "b": 2, "c": [1, 2]}
    json_text_2 = {"a": 1, "b": 2, "c": [1]}

    def setUp(self):
        self.merged_dict = merge_dictionaries(self.json_text_2, self.json_text)

    def test_merge(self):
        self.assertEqual(type(self.merged_dict), dict)


if __name__ == "__main__":
    unittest.main()
