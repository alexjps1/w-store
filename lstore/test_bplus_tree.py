import unittest
from bplus_tree import *
from random import randint, choice
import time

def rand_rid():
   return RID(randint(100000000, 999999999))

print("Creating tree with max degree 3")
t = BPlusTree(3)

print("\nTEST: Tree should be empty")
print(t)

print("\nTEST: Inserting something")
t.insert(4, rand_rid())
print(t)

print("\nTEST: Inserting another")
t.insert(13, rand_rid())
print(t)

print("\nTEST: Inserting another, make sure a split correctly occurs")
t.insert(6, rand_rid())
print(t)

print("\nTEST: Insert new RID but of existing key (4)")
t.insert(4, rand_rid())
print(t)

print("\nTEST: Cause a split to occur at the root (this time root is InternalNode)")
print("Inserting keys to cause a split at the root")
t.insert(7, rand_rid())
t.insert(8, rand_rid())
print(t)

print("\nTEST: Doing 5 more inserts. Make sure everything matches the simulator")
t.insert(36, rand_rid())
t.insert(20, rand_rid())
t.insert(21, rand_rid())
t.insert(3, rand_rid())
t.insert(14, rand_rid())
print(t)

print("\nTEST: Deleting a key that doesn't exist")
if t.delete(100, rid=None) is False:
    print("Returned False, which is the correct answer")
else:
    print("Returned True, which is incorrect")
    exit()

print("\nTEST: Deleting a key from a leaf node with two keys")
if t.delete(21, rid=None) is True:
    print("Returned True. So far so good, but check the tree")
else:
    print("Returned False, which is incorrect")
    exit()
print(t)

print("\nTEST: Deleting five existing keys at random and checking that the tree search still works")
available_choices = [13, 6, 4, 7, 8, 36, 20, 3, 14]
for i in range(5):
    chosen = choice(available_choices)
    available_choices.remove(chosen)
    t.delete(chosen, rid=None)
print(t)
for i in available_choices:
    assert isinstance(t.root, InternalNode)
    try:
        assert i in t._find_leaf(t.root, i).keys
    except:
        print(f"Tree search did not work while trying to find {i}")
        print(t)
        exit()
print("Verified that tree search still works properly after deletions")

print("\nTEST: Do a massive amount of inserts")
start_time = time.time()
NUM_INSERTS = 1000000
inserted_keys = set()
for i in range(NUM_INSERTS):
    random_int = randint(0, NUM_INSERTS // 10)
    inserted_keys.add(random_int)
    t.insert(random_int, rand_rid())
end_time = time.time()
print(f"Did {NUM_INSERTS} inserts in {end_time - start_time} seconds")
if input("View resulting tree structure? [y/N]").lower().startswith("y"):
    print(t)

print("\nTEST: Delete about half of inserted nodes, checking that tree search still works")
start_time = time.time()
inserted_keys = list(inserted_keys)
num_deletions_to_do = len(inserted_keys) // 2
for i in range(num_deletions_to_do):
    chosen_idx = randint(0, len(inserted_keys) - 1)
    t.delete(inserted_keys.pop(chosen_idx), rid=None)
end_time = time.time()
print(f"Did {num_deletions_to_do} deletions in {end_time - start_time} seconds")
for key in inserted_keys:
    assert isinstance(t.root, InternalNode)
    try:
        assert key in t._find_leaf(t.root, key).keys
    except:
        print(f"Tree search did not work while trying to find {key}")
        print(t)
        exit()
print("Verified that tree search still works properly after massive deletion")
if input("View resulting tree structure? [y/N]").lower().startswith("y"):
    print(t)

print("\nTEST: Insert another massive amount of keys to ensure that deletion did not corrupt the tree")
start_time = time.time()
inserted_keys = set()
for i in range(NUM_INSERTS):
    random_int = randint(0, NUM_INSERTS // 10)
    inserted_keys.add(random_int)
    t.insert(random_int, rand_rid())
end_time = time.time()
print(f"Did another {NUM_INSERTS} inserts in {end_time - start_time} seconds")
for key in inserted_keys:
    assert isinstance(t.root, InternalNode)
    try:
        assert key in t._find_leaf(t.root, key).keys
    except:
        print(f"Tree search did not work while trying to find {key}")
        print(t)
        exit()
print("Verified that tree search still works properly after massive deletion")
if input("View resulting tree structure? [y/N]").lower().startswith("y"):
    print(t)
