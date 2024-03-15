"use client";

import { TreeNode } from 'rc-tree';
import DirectoryTree from './DirectoryTree';
import TreePure from './Tree';
const Tree = TreePure;
Tree.DirectoryTree = DirectoryTree;
Tree.TreeNode = TreeNode;
export default Tree;