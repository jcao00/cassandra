/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class Nodes
{
    private AtomicReference<BTree> btree;

    public Nodes()
    {
        btree = new AtomicReference<>(new BTree(null, 0));
    }

    public boolean add(Node node)
    {
        BTree curBTree, nextBTree;
        do
        {
            curBTree = btree.get();

            // catch for case where we did not update the tree (because the node is already in the tree, or we didn't
            // update it)
            Node previous = curBTree.find(node.address);
            if (previous != null && node.equals(previous))
                return false;

            nextBTree = curBTree.insert(node);
        } while (!btree.compareAndSet(curBTree, nextBTree));

        return true;
    }

    public Node find(InetAddress addr)
    {
        return btree.get().find(addr);
    }


    public Iterator<GossipDigest> digests()
    {
        BTree curBTree = btree.get();
        return new BTreeIterator<>(curBTree, Node::digest);
    }

    // produces an iterator over a stable (i.e. non-mutating) tree,
    // so it's safe for concurrent access
    class BTreeIterator<T> implements Iterator<T>
    {
        private final Stack<BTreeNode> nodes;
        private final Function<Node, T> function;
        private BTreeNode cur;

        BTreeIterator(BTree btree, Function<Node, T> function)
        {
            this.nodes = new Stack<>();
            nodes.push(btree.root);
            this.function = function;

        }

        // TODO actually implement the iterator
        public boolean hasNext()
        {
            return false;
        }

        public T next()
        {
            return null;
        }
    }

//    // yeah, yeah, this thing could be made generic
//    static class BTree
//    {
//        private final BTreeNode root;
//        private int size;
//
//        BTree(BTreeNode root, int size)
//        {
//            this.root = root;
//            this.size = size;
//        }
//
//        public boolean isEmpty()
//        {
//            return size == 0;
//        }
//
//        public Node find(InetAddress addr)
//        {
//            if (isEmpty())
//                return null;
//
//            BTreeNode cur = root;
//            while (true)
//            {
//                int comp = compare(addr, cur.node.address);
//                if (comp == 0)
//                    return cur.node;
//                if (comp < 0)
//                {
//                    if (cur.left != null)
//                        cur = cur.left;
//                    else
//                        return null;
//                }
//                if (comp > 0)
//                {
//                    if (cur.right != null)
//                        cur = cur.right;
//                    else
//                        return null;
//                }
//            }
//        }
//
//        static int compare(InetAddress a, InetAddress b)
//        {
//            // assume addrs are the same type (IPv4 or IPv6) ...
//            byte[] addrA = a.getAddress();
//            byte[] addrB = b.getAddress();
//            assert addrA.length == addrB.length;
//
//            for (int i = 0; i < addrA.length; i++)
//            {
//                int diff = addrA[i] - addrB[i];
//                if (diff < 0)
//                    return -1;
//                if (diff > 0)
//                    return 1;
//            }
//
//            return 0;
//        }
//
//        public BTree insert(Node node)
//        {
//            // optimization for an empty tree
//            if (isEmpty())
//            {
//                BTreeNode bTreeNode = new BTreeNode(node.address, node, null, null);
//                return new BTree(bTreeNode, 1);
//            }
//
//            // recurse until a match (and update the node), or we hit a leaf node.
//            // even with a b tree, a depth of 16 allow us 64k nodes in a cluster, more than i need to worry about for now.
//            // switch this to a better data structure later, but a stack is good enough for now
//            Stack<BTreeNode> parents = new Stack<>();
//
//            BTreeNode curNode = root;
//            while (true)
//            {
//                int comp = compare(node.address, curNode.addr);
//                if (comp == 0)
//                {
//                    // full node is already in the tree - lucky case
//                    if (node.equals(curNode.node))
//                        return this;
//
//                    Node mergedNode = node.merge(curNode.node);
//                    BTreeNode newBtreeNode = new BTreeNode(mergedNode.address, mergedNode, curNode.left, curNode.right);
//
//                    BTreeNode newRoot = buildNewTree(newBtreeNode, parents);
//                    return new BTree(newRoot, size);
//                }
//                else if (comp < 1)
//                {
//                    if (curNode.left == null)
//                    {
//                        BTreeNode newBtreeNode = new BTreeNode(node.address, node, null, null);
//                        BTreeNode newRoot = buildNewTree(newBtreeNode, parents);
//                        return new BTree(newRoot, size + 1);
//                    }
//                    else
//                    {
//                        parents.push(curNode);
//                        curNode = curNode.left;
//                        //continue
//                    }
//                }
//                else if (comp > 1)
//                {
//                    if (curNode.right == null)
//                    {
//                        BTreeNode newBtreeNode = new BTreeNode(node.address, node, null, null);
//                        BTreeNode newRoot = buildNewTree(newBtreeNode, parents);
//                        return new BTree(newRoot, size + 1);
//                    }
//                    else
//                    {
//                        parents.push(curNode);
//                        curNode = curNode.right;
//                    }
//                }
//            }
//        }
//
//        private BTreeNode buildNewTree(BTreeNode newBtreeNode, Stack<BTreeNode> parents)
//        {
//            BTreeNode cur = newBtreeNode;
//            BTreeNode parent;
//            while (!parents.empty())
//            {
//                parent = parents.pop();
//                // compare the cur IP addr with the parent; if greater, we are the right child, else less
//                int diff = compare(cur.addr, parent.addr);
//                if (diff < 0)
//                {
//                    BTreeNode newParent = new BTreeNode(parent.addr, parent.node, cur, parent.right);
//                    cur = newParent;
//                }
//                else if (diff > 0)
//                {
//                    BTreeNode newParent = new BTreeNode(parent.addr, parent.node, parent.left, cur);
//                    cur = newParent;
//                }
//                else
//                {
//                    throw new IllegalStateException("should not get here!");
//                }
//            }
//            return cur;
//        }
//    }
//
//    static class BTreeNode
//    {
//        final InetAddress addr;
//        final Node node;
//        final BTreeNode left;
//        final BTreeNode right;
//
//        BTreeNode(InetAddress addr, Node node, BTreeNode left, BTreeNode right)
//        {
//            this.addr = addr;
//            this.node = node;
//            this.left = left;
//            this.right = right;
//        }
//    }
}
