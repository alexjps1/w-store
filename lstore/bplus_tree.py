"""
B Plus Tree
"""

from typing import NewType, List, Union

# NOTE: Assuming RIDs are integers for typing purposes
RID = NewType('RID', int)


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
        self.rid_lists: List[List[RID]] = []

    def __str__(self, indent_level: int = 0) -> str:
        """
        Print the leaf node in a pretty format.
        """
        str_rep = f"{'  ' * indent_level}Leaf ({self._pretty_print_keys()})\n"
        for i, key in enumerate(self.keys):
            str_rep += f"{'  ' * (indent_level + 1)}Key {self.keys[i]}\n"
            for j, rid in enumerate(self.rid_lists[i]):
                str_rep += f"{'  ' * (indent_level + 2)}RID {rid}\n"
        return str_rep


    def insert_rid(self, key: int, rid: RID) -> None:
        """
        Insert a key, value pair into the leaf node.
        If the key already exists, the RID is added to the list of RIDs for that key.
        If not, a new key is inserted in sorted order.

        Warning: This allows the key to go over the max degree.
        Splits should be handled by the tree's insert func.
        """
        if key in self.keys:
            # add RID to the existing list for that key
            self.rid_lists[self.keys.index(key)].append(rid)
            return

        # find the right place, maintaining sorted order
        i = 0
        while i < len(self.keys) and key > self.keys[i]:
            i += 1
        self.keys.insert(i, key)
        self.rid_lists.insert(i, [rid])

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

    def insert(self, key: int, rid: RID) -> None:
        """
        Insert a key, RID pair in the tree.
        If the key already exists, the RID is added to the list of RIDs for that key.
        If not, a new key is inserted in tradition B+ tree fashion.
        """
        if self.root is None:
            # empty tree, create and insert into the root
            self.root = LeafNode(is_root=True)
            self.root.insert_rid(key, rid)
            return

        # find leaf and insert
        leaf = self._find_leaf(self.root, key)
        leaf.insert_rid(key, rid)
        if len(leaf.keys) > self.max_degree - 1:
            self._split_leaf(leaf)

    def delete(self, key, rid):
        """
        Delete an RID from a key in the B+ tree (or remove the key entirely).
        Warning: This method does not handle rebalancing the tree.
        Warning: Passing None unique_val deletes all RIDs associated with the passed key
        """
        assert self.root is not None

        deletion_leaf = self._find_leaf(self.root, key)
        assert isinstance(deletion_leaf, LeafNode)

        if not deletion_leaf or key not in deletion_leaf.keys:
           return False # key doesn't exist

        key_index = deletion_leaf.keys.index(key)

        if rid is None:
            # delete the key entirely
            deletion_leaf.keys.pop(key_index)
            deletion_leaf.rid_lists.pop(key_index)
            return True
        elif rid in deletion_leaf.rid_lists[key_index]:
            # delete only the rid
            deletion_leaf.rid_lists[key_index].remove(rid)
        else:
            return False # unique identifier doesn't exist under this key

        # delete the key entirely if that was the last rid in it
        if len(deletion_leaf.rid_lists[key_index]) == 0:
           deletion_leaf.keys.pop(key_index)
           deletion_leaf.rid_lists.pop(key_index)
        return True

    def point_query(self, key: int) -> List[RID]:
        """
        Return the RIDs associated with the key.
        """
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            return leaf.rid_lists[leaf.keys.index(key)]
        # key not found
        return []

    def range_query(self, key_start: int, key_end: int) -> List[RID]:
        """
        Return the RIDs associated with keys in the range [key_start, key_end].
        """
        assert isinstance(self.root, Node)
        leaf = self._find_leaf(self.root, key_start)
        rids = []
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                if key_start <= k <= key_end:
                    rids.extend(leaf.rid_lists[i])
            if leaf.keys[-1] >= key_end:
                break
            leaf = leaf.next
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
        new_leaf.rid_lists = old_leaf.rid_lists[midpoint:]

        # old leaf gets first, bigger half of keys and vals
        old_leaf.keys = old_leaf.keys[:midpoint]
        old_leaf.rid_lists = old_leaf.rid_lists[:midpoint]

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
