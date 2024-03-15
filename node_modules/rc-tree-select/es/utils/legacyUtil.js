import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["children", "value"];
import * as React from 'react';
import toArray from "rc-util/es/Children/toArray";
import warning from "rc-util/es/warning";
import TreeNode from "../TreeNode";
export function convertChildrenToData(nodes) {
  return toArray(nodes).map(function (node) {
    if (! /*#__PURE__*/React.isValidElement(node) || !node.type) {
      return null;
    }
    var _ref = node,
      key = _ref.key,
      _ref$props = _ref.props,
      children = _ref$props.children,
      value = _ref$props.value,
      restProps = _objectWithoutProperties(_ref$props, _excluded);
    var data = _objectSpread({
      key: key,
      value: value
    }, restProps);
    var childData = convertChildrenToData(children);
    if (childData.length) {
      data.children = childData;
    }
    return data;
  }).filter(function (data) {
    return data;
  });
}
export function fillLegacyProps(dataNode) {
  if (!dataNode) {
    return dataNode;
  }
  var cloneNode = _objectSpread({}, dataNode);
  if (!('props' in cloneNode)) {
    Object.defineProperty(cloneNode, 'props', {
      get: function get() {
        warning(false, 'New `rc-tree-select` not support return node instance as argument anymore. Please consider to remove `props` access.');
        return cloneNode;
      }
    });
  }
  return cloneNode;
}
export function fillAdditionalInfo(extra, triggerValue, checkedValues, treeData, showPosition, fieldNames) {
  var triggerNode = null;
  var nodeList = null;
  function generateMap() {
    function dig(list) {
      var level = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '0';
      var parentIncluded = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
      return list.map(function (option, index) {
        var pos = "".concat(level, "-").concat(index);
        var value = option[fieldNames.value];
        var included = checkedValues.includes(value);
        var children = dig(option[fieldNames.children] || [], pos, included);
        var node = /*#__PURE__*/React.createElement(TreeNode, option, children.map(function (child) {
          return child.node;
        }));

        // Link with trigger node
        if (triggerValue === value) {
          triggerNode = node;
        }
        if (included) {
          var checkedNode = {
            pos: pos,
            node: node,
            children: children
          };
          if (!parentIncluded) {
            nodeList.push(checkedNode);
          }
          return checkedNode;
        }
        return null;
      }).filter(function (node) {
        return node;
      });
    }
    if (!nodeList) {
      nodeList = [];
      dig(treeData);

      // Sort to keep the checked node length
      nodeList.sort(function (_ref2, _ref3) {
        var val1 = _ref2.node.props.value;
        var val2 = _ref3.node.props.value;
        var index1 = checkedValues.indexOf(val1);
        var index2 = checkedValues.indexOf(val2);
        return index1 - index2;
      });
    }
  }
  Object.defineProperty(extra, 'triggerNode', {
    get: function get() {
      warning(false, '`triggerNode` is deprecated. Please consider decoupling data with node.');
      generateMap();
      return triggerNode;
    }
  });
  Object.defineProperty(extra, 'allCheckedNodes', {
    get: function get() {
      warning(false, '`allCheckedNodes` is deprecated. Please consider decoupling data with node.');
      generateMap();
      if (showPosition) {
        return nodeList;
      }
      return nodeList.map(function (_ref4) {
        var node = _ref4.node;
        return node;
      });
    }
  });
}