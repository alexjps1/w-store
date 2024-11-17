from lstore.bplus_tree import *
from lstore.index import Index
from lstore.table import Table

from shutil import rmtree
from pathlib import Path
# import necessary libraries for unit testing
import unittest
from random import randint

class TestInsertion(unittest.TestCase):

    def setUp(self):
        self.table = Table("test_table", Path("test_table"), 1, 0)
        self.index = Index(self.table, use_bplus=True)

    def tearDown(self) -> None:
        # delete the created disk diretory
        rmtree("./disk")

    def test_insert_external_correctness(self):
        self.index.add_record_to_index(0, 5, RID(35225))
        self.assertEqual(self.index.locate(0, 5), [RID(35225)])

    def test_insert_tree_entry_correctness(self):
        """
        Inserted tree entry has the correct RID, version, and prev/next version keys
        """
        # insert two records of same key
        self.index.add_record_to_index(0, 13, RID(59275))
        self.index.add_record_to_index(0, 13, RID(14524))

        # check that entries for both are correctly set
        leaf = self.index.indices[0]._find_leaf(self.index.indices[0].root, 13)
        entries = leaf.tree_entry_lists[leaf.keys.index(13)]
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].rid, RID(59275))
        self.assertEqual(entries[0].abs_ver, 0)
        self.assertEqual(entries[0].prev_ver_key, None)
        self.assertEqual(entries[0].next_ver_key, None)
        self.assertEqual(entries[1].rid, RID(14524))
        self.assertEqual(entries[1].abs_ver, 0)
        self.assertEqual(entries[1].prev_ver_key, None)
        self.assertEqual(entries[1].next_ver_key, None)



class TestUpdate(unittest.TestCase):
    def setUp(self):
        self.table = Table("test_table", Path("test_table"), 1, 0)
        self.index = Index(self.table, use_bplus=True)

        # add initial record
        self.index.add_record_to_index(0, 5, RID(35225))
        original_leaf = self.index.indices[0]._find_leaf(self.index.indices[0].root, 5)
        original_entry = original_leaf.tree_entry_lists[original_leaf.keys.index(5)][0]

    def tearDown(self) -> None:
        # delete the created disk diretory
        rmtree("./disk")

    def test_update_tree_entry_correctness(self):
        # get the entry before it was updated
        original_leaf = self.index.indices[0]._find_leaf(self.index.indices[0].root, 5)
        original_entry = original_leaf.tree_entry_lists[original_leaf.keys.index(5)][0]

        # update record
        # self.index.update_record_in_index(10, 5, RID(35225))
        self.index.indices[0].update(10, 5, RID(35225))

        # fetch new entry
        leaf = self.index.indices[0]._find_leaf(self.index.indices[0].root, 10)
        entry = leaf.tree_entry_lists[leaf.keys.index(10)][0]

        print(f"The original entry (after update) looks like {original_entry.__str__()}")
        print(f"The newly-inserted entry looks like {entry.__str__()}")
        self.assertEqual(entry.abs_ver, 1)
        self.assertEqual(entry.prev_ver_key, 5)
        self.assertEqual(entry.next_ver_key, None)
        self.assertEqual(entry.rid, RID(35225))

        # fetch old entry and check that next_ver_val is updated to 10
        self.assertEqual(original_entry.abs_ver, 0)
        self.assertEqual(original_entry.next_ver_key, 10)
        self.assertEqual(original_entry.prev_ver_key, None)

# run unit tests
if __name__ == '__main__':
    unittest.main()
