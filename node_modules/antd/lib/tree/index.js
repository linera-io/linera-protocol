"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _rcTree = require("rc-tree");
var _DirectoryTree = _interopRequireDefault(require("./DirectoryTree"));
var _Tree = _interopRequireDefault(require("./Tree"));
const Tree = _Tree.default;
Tree.DirectoryTree = _DirectoryTree.default;
Tree.TreeNode = _rcTree.TreeNode;
var _default = exports.default = Tree;