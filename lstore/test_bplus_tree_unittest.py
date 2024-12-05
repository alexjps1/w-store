# started writing with unittest
"""
def mass_insert(t: BPlusTree, num_inserts: int) -> List[int]:
    # Insert a large number of random keys into the tree
    # Return the list of keys inserted
    for i in range(num_inserts):
        t.insert(randint(0, num_inserts // 10), rand_rid())

class TestInsertion()

    def test_insert_stability(self):
        for max_degree in range(3, 6):
            t = BPlusTree(max_degree)
            mass_insert(t, 1000000)
            assert searchable(t)
            assert nodes_fit_degree(t)
"""
