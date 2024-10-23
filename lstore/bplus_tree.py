"""
B Plus Tree
"""

import math


class Node:
    def __init__(self, max_degree: int) -> None:
        self.min_degree = math.ceil(max_degree / 2)
        self.keys = []
        self.children = []
        self.parent = None


class RootNode(Node):
    def __init__(self, max_degree: int) -> None:
        super().__init__(max_degree)
        self.min_degree = 0


class InternalNode(Node):
    def __init__(self, max_degree: int) -> None:
        super().__init__(max_degree)


class LeafNode(Node):
    def __init__(self, max_degree: int) -> None:
        super().__init__(max_degree)
        self.next = None
        self.prev = None
        self.values = []


class BPlusTree:
    def __init__(self, max_degree: int) -> None:
        self.root = RootNode(max_degree)
        self.max_degree = max_degree

    def insert(self, key: int, value: int) -> None:
        """
        Find the leaf node to which the key should be inserted.
        Insert the key, splitting the node if necessary.
        """
        leaf = self._find_leaf(self.root, key)
        leaf.insert(key, value)
        if len(leaf.keys) > self.max_degree - 1:
            self._split_leaf(leaf)

    def _find_leaf(self, start_node: Node, key: int) -> LeafNode:
        """
        Traverse the tree to find a leaf node to which the key should be inserted.
        """
        if isinstance(start_node, LeafNode):
            return start_node
        for i, k in enumerate(start_node.keys):
            if key < k:
                return self._find_leaf(start_node.children[i], key)
        return self._find_leaf(start_node.children[-1], key)

    def _split_leaf(self, leaf: "LeafNode") -> None:
        """
        Split a leaf node into two leaf nodes.
        The new leaf will contain the higher keys.
        """
        new_leaf = LeafNode(self.max_degree)
        mid = len(leaf.keys) // 2
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        new_leaf.next = leaf.next
        leaf.next = new_leaf
        new_leaf.prev = leaf
        if new_leaf.next:
            new_leaf.next.prev = new_leaf
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)


    def _insert_into_parent(self, old_node: Node, key: int, new_node: Node) -> None:
        """
        Insert a key and child node into the parent node.
        """
        if old_node == self.root:
            new_root = RootNode(self.max_degree)
            new_root.keys = [key]
            new_root.children = [old_node, new_node]
            old_node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
            return


