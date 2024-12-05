"""

B Plus Tree
"""

from sys import base_prefix
from typing import NewType, List, Union, Tuple
from lstore.config import debug_print as print

# NOTE: Assuming RIDs are integers for typing purposes
RID = NewType('RID', int)

class TreeEntry:
    def __init__(self, rid: RID, abs_ver=0, prev_ver_key=None, next_ver_key=None):
        self.rid = rid
        self.prev_ver_key = prev_ver_key
        self.next_ver_key = next_ver_key
        self.abs_ver = abs_ver

    def __str__(self):
        rid_str = str(self.rid)
        if len(rid_str) > 3:
            rid_str = "..." + rid_str[-3:]
        return f"({rid_str}, {self.abs_ver}, {self.prev_ver_key}, {self.next_ver_key})"

    def set_next_ver_key(self, next_ver_key):
        self.next_ver_key = next_ver_key

class Node:

    def __init__(self, is_root: bool = False) -> None:
        """
        Abstract class for a node in the B+ tree.
        In subclasses, distinguish between InternalNode and LeafNode.
        """
        self.keys: List[int] = []
        self.parent: Union[InternalNode, None] = None
        self.is_root: bool = is_root

    def _pretty_print_keys(self) -> str:
        """
        Print keys of a node in a compact format.
        """
        if len(self.keys) == 0:
            return "no keys"
        elif len(self.keys) <= 5:
            return ", ".join(map(str, self.keys))
        else:
            return ", ".join(map(str, self.keys[:3])) + ", ... " + str(self.keys[-1])

class InternalNode(Node):

    def __init__(self, is_root: bool = False) -> None:
        super().__init__(is_root=is_root)
        self.children: List[Node] = []

    def __str__(self, indent_level: int = 0) -> str:
        """
        Print the internal node in a pretty format.
        """
        str_rep = f"{'  ' * indent_level}Internal ({self._pretty_print_keys()})\n"
        for child in self.children:
            assert isinstance(child, InternalNode) or isinstance(child, LeafNode)
            str_rep += child.__str__(indent_level=indent_level+1)
        return str_rep


class LeafNode(Node):

    def __init__(self, is_root: bool = False) -> None:
        super().__init__(is_root=is_root)
        self.next: Union[LeafNode, None] = None
        self.prev: Union[LeafNode, None] = None
        self.tree_entry_lists: List[List[TreeEntry]] = []

    def __str__(self, indent_level: int = 0) -> str:
        """
        Print the leaf node in a pretty format.
        """
        str_rep = f"{'  ' * indent_level}Leaf ({self._pretty_print_keys()})\n"
        for i, key in enumerate(self.keys):
            str_rep += f"{'  ' * (indent_level + 1)}Key {self.keys[i]}\n"
            for j, entry in enumerate(self.tree_entry_lists[i]):
                str_rep += f"{'  ' * (indent_level + 2)}RID {entry.rid}\n"
        return str_rep


    def insert_entry(self, key: int, rid: RID, abs_ver=0, prev_ver_key=None) -> None:
        """
        Insert a new entry (rid, abs_ver, prev_ver_val, next_ver_val) into the leaf node.
        If the key already exists, the entry is added to the list corresponding to that key.
        If not, a new key is inserted in sorted order.

        Warning: This allows the key to go over the max degree.
        Splits should be handled by the tree's insert func.
        """
        if key in self.keys:
            # add entry to the existing list for that key
            self.tree_entry_lists[self.keys.index(key)].append(TreeEntry(rid, abs_ver, prev_ver_key))
            return

        # find the right place, maintaining sorted order
        i = 0
        while i < len(self.keys) and key > self.keys[i]:
            i += 1
        self.keys.insert(i, key)
        self.tree_entry_lists.insert(i, [TreeEntry(rid, abs_ver, prev_ver_key)])

    def update_entry_next_ver_key(self, key, rid, next_ver_key) -> int:
        """
        Update an entry next_val to a new pointer.
        Return absolute version of the updated entry.
        """

        if key not in self.keys:
            return -1 # bad key lookup

        for entry in self.tree_entry_lists[self.keys.index(key)]:
            if entry.rid == rid:
                entry.next_ver_key = next_ver_key
                return entry.abs_ver
        # print(f"key::{key}, RID::{rid}, next_ver_key::{next_ver_key}")
        raise ValueError

    def remove_entry(self, key: int, rid: RID, abs_ver: int = 0) -> int | None:
        """
        Remove the entry with the given key, rid, and absolute version.
        Return the entry's prev_ver_key if it exists,
        or None if that entry represents the first version of its record.
        """
        entry_list = self.tree_entry_lists[self.keys.index(key)]
        for i, entry in enumerate(entry_list):
            # make sure rid matches, and ensure the absolute version matches
            if entry.rid == rid and entry.abs_ver == abs_ver:
                old_entry = entry_list.pop(i)
                return old_entry.prev_ver_key

    def remove_latest_entry(self, key: int, rid: RID) -> Tuple[Union[int, None], int]:
        """
        Remove the entry with the given key and rid whose next_ver_key is None (i.e. newest such entry).
        Return a (prev_ver_key, abs_ver) tuple, which enables finding its predecessor.
        """
        entry_list = self.tree_entry_lists[self.keys.index(key)]
        for i, entry in enumerate(entry_list):
            if entry.rid == rid and entry.next_ver_key is None:
                old_entry = entry_list.pop(i)
                return old_entry.prev_ver_key, old_entry.abs_ver
        raise ValueError("There was no latest entry to delete. Possibly mistaken call to delete() in BPlusTree.")


    def get_raw_latest_rids(self, key) -> List[RID]:
        """
        Get the raw RIDs with no next pointer associated with versions,
        i.e. the RID is up to date.
        """
        rids: List[RID] = []
        for tree_entry in self.tree_entry_lists[self.keys.index(key)]:
            if tree_entry.next_ver_key is None:
                rids.append(tree_entry.rid)

        return rids

class BPlusTree:

    # magic methods

    def __init__(self, max_degree: int) -> None:
        assert max_degree >= 3
        self.root: Union[InternalNode, LeafNode, None] = None
        self.max_degree = max_degree

    def __str__(self) -> str:
        """
        Create a pretty print of the whole tree.
        """
        if not self.root:
            return "Empty tree."
        return self.root.__str__()

    # public methods

    def insert(self, key: int, rid: RID, abs_ver=0, prev_ver_key=None) -> None:
        """
        Insert a key, RID pair in the tree.
        If the key already exists, the RID is added to the list of RIDs for that key.
        If not, a new key is inserted in tradition B+ tree fashion.
        """
        if self.root is None:
            # empty tree, create and insert into the root
            self.root = LeafNode(is_root=True)
            self.root.insert_entry(key, rid)
            return

        # find leaf and insert
        leaf = self._find_leaf(self.root, key)
        leaf.insert_entry(key, rid, abs_ver, prev_ver_key)

        if len(leaf.keys) > self.max_degree - 1:
            self._split_leaf(leaf)

    def update(self, new_ver_key: int, prev_ver_key: int, rid: RID) -> None:
        if new_ver_key == prev_ver_key:
            # do not add to the index for redundant updates
            return
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, prev_ver_key)

        # update previous record with new pointer to current value
        prev_abs_ver = leaf.update_entry_next_ver_key(prev_ver_key, rid, new_ver_key)

        # insert new record
        self.insert(new_ver_key, rid, prev_abs_ver + 1, prev_ver_key)

    def delete(self, key: int, rid: RID):
        """
        Delete an entry and its predecessors from the B+ Tree (or remove the key entirely).
        Warning: This method does not handle rebalancing the tree.
        Warning: Passing None unique_val deletes all RIDs associated with the passed key
        """
        assert self.root is not None

        # find base leaf - delete from this leaf, save value of prev_pointer, backtrack and remove entries
        deletion_leaf = self._find_leaf(self.root, key)
        assert isinstance(deletion_leaf, LeafNode)

        if not deletion_leaf or key not in deletion_leaf.keys:
           return False # key doesn't exist, return False to indicate an invalid delete query

        # now perform the deletion of the entry and its preceding entries (versions) from the index
        prev_ver_key, abs_ver = deletion_leaf.remove_latest_entry(key, rid)
        # NOTE this method should confirm that there is exactly one entry with this particular RID whose next_ver_key is None
        # it should then delete that entry from the tree_entries_list and return prev_ver_key and abs_ver of that entry

        while (prev_ver_key is not None):
            abs_ver -= 1
            assert abs_ver < 0
            deletion_leaf = self._find_leaf(self.root, prev_ver_key)
            prev_ver_key = deletion_leaf.remove_entry(prev_ver_key, rid, abs_ver)

        return True

    def point_query(self, key: int) -> List[RID]:
        """
        Return the RIDs of records with the specified key in their latest version.
        """
        if self.root is None:
            return []
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            return leaf.get_raw_latest_rids(key)
        # key not found
        return []

    def range_query(self, key_start: int, key_end: int) -> List[RID]:
        """
        Return the RIDs associated with keys in the range [key_start, key_end].
        """
        if self.root is None:
            return []
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, key_start)
        rids = []
        while leaf is not None:
            for curr_key in leaf.keys:
                if key_start <= curr_key <= key_end:
                    rids.extend(leaf.get_raw_latest_rids(curr_key))
            if leaf.keys[-1] >= key_end:
                break
            leaf = leaf.next
        return rids

    def __get_relative_entry_version(self, base_entry: TreeEntry) -> int:
        assert isinstance(self.root, Node)

        # extract value and search for RID
        base_rid = base_entry.rid
        next_key = base_entry.next_ver_key
        abs_ver = base_entry.abs_ver
        rel_ver = 0

        while (next_key is not None):
            abs_ver += 1
            next_leaf = self._find_leaf(self.root, next_key)
            curr_entry = None

            if not (next_leaf and next_key in next_leaf.keys):
                raise ValueError("Invalid next version key.")

            for entry in next_leaf.tree_entry_lists[next_leaf.keys.index(next_key)]:
                if entry.rid == base_rid and entry.abs_ver == abs_ver:
                    curr_entry = entry # found entry associated with base RID at key
                                        # i.e. confirmed valid next_val pointer
                    break # stop searching for next entry in curr leaf

            if not curr_entry:
                # print(curr_entry)
                raise ValueError("Invalid next version key") # rid not associated with value, next pointer is bad

            if curr_entry and curr_entry.next_ver_key is not None:
                next_key = curr_entry.next_ver_key # iterate to determine next hops
                rel_ver -= 1

        return rel_ver



    def version_query(self, key: int, rel_ver: int) -> List[RID]:
        """
        Return the RIDs corresponding to records which had the given key at the given relative version.
        """
        if self.root is None:
            return []
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, key)
        assert rel_ver <= 0

        rids = []
        if key in leaf.keys:
            for entry in leaf.tree_entry_lists[leaf.keys.index(key)]:
                if self.__get_relative_entry_version(entry) == rel_ver:
                    rids.append(entry.rid)

        # key not found
        return rids

    # private methods

    def _find_leaf(self, start_node: Node, key: int) -> LeafNode:
        """
        Traverse the tree to find a leaf node.
        Helpful for finding where a key should be inserted, for example.
        """
        if isinstance(start_node, LeafNode):
            return start_node
        assert isinstance(start_node, InternalNode)
        for i, k in enumerate(start_node.keys):
            if key < k:
                return self._find_leaf(start_node.children[i], key)
        return self._find_leaf(start_node.children[-1], key)

    def _split_leaf(self, old_leaf: LeafNode) -> None:
        """
        Split a leaf node into two leaf nodes.
        The new leaf will contain the higher keys.
        Handles case where root is leaf node (i.e. one node in tree).
        """
        # Split the leaf into two and handle the case where the root is a leaf node
        new_leaf = LeafNode()
        midpoint = len(old_leaf.keys) // 2

        # new leaf gets second, smaller half of keys and vals
        new_leaf.keys = old_leaf.keys[midpoint:]
        new_leaf.tree_entry_lists = old_leaf.tree_entry_lists[midpoint:]

        # old leaf gets first, bigger half of keys and vals
        old_leaf.keys = old_leaf.keys[:midpoint]
        old_leaf.tree_entry_lists = old_leaf.tree_entry_lists[:midpoint]

        # Update the linked list pointers
        new_leaf.next = old_leaf.next
        if new_leaf.next:
            new_leaf.next.prev = new_leaf
        new_leaf.prev = old_leaf
        old_leaf.next = new_leaf

        # If the leaf is the root, create a new root
        if old_leaf.is_root:
            new_root = InternalNode(is_root=True)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [old_leaf, new_leaf]
            old_leaf.is_root = False
            old_leaf.parent = new_root
            new_leaf.parent = new_root
            self.root = new_root
        else:
            # Insert the new key and new leaf into the parent
            self._insert_into_parent(old_leaf, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, old_node: Union[InternalNode, LeafNode], key: int, new_node: Union[InternalNode, LeafNode]) -> None:
        """
        Insert a new key and node into the parent of the split node.
        If the parent is also full, recursively split the parent.
        """
        parent = old_node.parent

        if parent is None:
            # If there is no parent, create a new root
            new_root = InternalNode(is_root=True)
            new_root.keys = [key]
            new_root.children = [old_node, new_node]
            old_node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
            return

        # Insert the new key and new node into the parent
        insert_index = 0
        while insert_index < len(parent.keys) and parent.keys[insert_index] < key:
            insert_index += 1

        parent.keys.insert(insert_index, key)
        parent.children.insert(insert_index + 1, new_node)
        new_node.parent = parent

        # If the parent is overfull, split it
        if len(parent.keys) > self.max_degree - 1:
            self._split_internal(parent)

    def _split_internal(self, old_node: InternalNode) -> None:
        """
        Split an internal node into two internal nodes.
        The new internal node will contain the higher keys.
        """
        new_node = InternalNode()
        midpoint = len(old_node.keys) // 2
        middle_key = old_node.keys[midpoint]

        # new node gets second, larger half of keys and children
        new_node.keys = old_node.keys[midpoint + 1:]
        new_node.children = old_node.children[midpoint + 1:]

        # old node keeps first, smaller half of keys and children
        old_node.keys = old_node.keys[:midpoint]
        old_node.children = old_node.children[:midpoint + 1]

        # Update parent references for children of new node
        for child in new_node.children:
            child.parent = new_node

        if old_node.is_root:
            new_root = InternalNode(is_root=True)
            new_root.keys = [middle_key]
            new_root.children = [old_node, new_node]
            old_node.is_root = False
            old_node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
        else:
            self._insert_into_parent(old_node, middle_key, new_node)
