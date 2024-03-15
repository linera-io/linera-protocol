"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.convertDataToEntities = convertDataToEntities;
exports.convertNodePropsToEventData = convertNodePropsToEventData;
exports.convertTreeToData = convertTreeToData;
exports.fillFieldNames = fillFieldNames;
exports.flattenTreeData = flattenTreeData;
exports.getKey = getKey;
exports.getPosition = getPosition;
exports.getTreeNodeProps = getTreeNodeProps;
exports.isTreeNode = isTreeNode;
exports.traverseDataNodes = traverseDataNodes;
exports.warningWithoutKey = warningWithoutKey;
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _keyUtil = _interopRequireDefault(require("./keyUtil"));
var _excluded = ["children"];
function getPosition(level, index) {
  return "".concat(level, "-").concat(index);
}
function isTreeNode(node) {
  return node && node.type && node.type.isTreeNode;
}
function getKey(key, pos) {
  if (key !== null && key !== undefined) {
    return key;
  }
  return pos;
}
function fillFieldNames(fieldNames) {
  var _ref = fieldNames || {},
    title = _ref.title,
    _title = _ref._title,
    key = _ref.key,
    children = _ref.children;
  var mergedTitle = title || 'title';
  return {
    title: mergedTitle,
    _title: _title || [mergedTitle],
    key: key || 'key',
    children: children || 'children'
  };
}

/**
 * Warning if TreeNode do not provides key
 */
function warningWithoutKey(treeData, fieldNames) {
  var keys = new Map();
  function dig(list) {
    var path = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';
    (list || []).forEach(function (treeNode) {
      var key = treeNode[fieldNames.key];
      var children = treeNode[fieldNames.children];
      (0, _warning.default)(key !== null && key !== undefined, "Tree node must have a certain key: [".concat(path).concat(key, "]"));
      var recordKey = String(key);
      (0, _warning.default)(!keys.has(recordKey) || key === null || key === undefined, "Same 'key' exist in the Tree: ".concat(recordKey));
      keys.set(recordKey, true);
      dig(children, "".concat(path).concat(recordKey, " > "));
    });
  }
  dig(treeData);
}

/**
 * Convert `children` of Tree into `treeData` structure.
 */
function convertTreeToData(rootNodes) {
  function dig(node) {
    var treeNodes = (0, _toArray.default)(node);
    return treeNodes.map(function (treeNode) {
      // Filter invalidate node
      if (!isTreeNode(treeNode)) {
        (0, _warning.default)(!treeNode, 'Tree/TreeNode can only accept TreeNode as children.');
        return null;
      }
      var key = treeNode.key;
      var _treeNode$props = treeNode.props,
        children = _treeNode$props.children,
        rest = (0, _objectWithoutProperties2.default)(_treeNode$props, _excluded);
      var dataNode = (0, _objectSpread2.default)({
        key: key
      }, rest);
      var parsedChildren = dig(children);
      if (parsedChildren.length) {
        dataNode.children = parsedChildren;
      }
      return dataNode;
    }).filter(function (dataNode) {
      return dataNode;
    });
  }
  return dig(rootNodes);
}

/**
 * Flat nest tree data into flatten list. This is used for virtual list render.
 * @param treeNodeList Origin data node list
 * @param expandedKeys
 * need expanded keys, provides `true` means all expanded (used in `rc-tree-select`).
 */
function flattenTreeData(treeNodeList, expandedKeys, fieldNames) {
  var _fillFieldNames = fillFieldNames(fieldNames),
    fieldTitles = _fillFieldNames._title,
    fieldKey = _fillFieldNames.key,
    fieldChildren = _fillFieldNames.children;
  var expandedKeySet = new Set(expandedKeys === true ? [] : expandedKeys);
  var flattenList = [];
  function dig(list) {
    var parent = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : null;
    return list.map(function (treeNode, index) {
      var pos = getPosition(parent ? parent.pos : '0', index);
      var mergedKey = getKey(treeNode[fieldKey], pos);

      // Pick matched title in field title list
      var mergedTitle;
      for (var i = 0; i < fieldTitles.length; i += 1) {
        var fieldTitle = fieldTitles[i];
        if (treeNode[fieldTitle] !== undefined) {
          mergedTitle = treeNode[fieldTitle];
          break;
        }
      }

      // Add FlattenDataNode into list
      var flattenNode = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, (0, _omit.default)(treeNode, [].concat((0, _toConsumableArray2.default)(fieldTitles), [fieldKey, fieldChildren]))), {}, {
        title: mergedTitle,
        key: mergedKey,
        parent: parent,
        pos: pos,
        children: null,
        data: treeNode,
        isStart: [].concat((0, _toConsumableArray2.default)(parent ? parent.isStart : []), [index === 0]),
        isEnd: [].concat((0, _toConsumableArray2.default)(parent ? parent.isEnd : []), [index === list.length - 1])
      });
      flattenList.push(flattenNode);

      // Loop treeNode children
      if (expandedKeys === true || expandedKeySet.has(mergedKey)) {
        flattenNode.children = dig(treeNode[fieldChildren] || [], flattenNode);
      } else {
        flattenNode.children = [];
      }
      return flattenNode;
    });
  }
  dig(treeNodeList);
  return flattenList;
}
/**
 * Traverse all the data by `treeData`.
 * Please not use it out of the `rc-tree` since we may refactor this code.
 */
function traverseDataNodes(dataNodes, callback,
// To avoid too many params, let use config instead of origin param
config) {
  var mergedConfig = {};
  if ((0, _typeof2.default)(config) === 'object') {
    mergedConfig = config;
  } else {
    mergedConfig = {
      externalGetKey: config
    };
  }
  mergedConfig = mergedConfig || {};

  // Init config
  var _mergedConfig = mergedConfig,
    childrenPropName = _mergedConfig.childrenPropName,
    externalGetKey = _mergedConfig.externalGetKey,
    fieldNames = _mergedConfig.fieldNames;
  var _fillFieldNames2 = fillFieldNames(fieldNames),
    fieldKey = _fillFieldNames2.key,
    fieldChildren = _fillFieldNames2.children;
  var mergeChildrenPropName = childrenPropName || fieldChildren;

  // Get keys
  var syntheticGetKey;
  if (externalGetKey) {
    if (typeof externalGetKey === 'string') {
      syntheticGetKey = function syntheticGetKey(node) {
        return node[externalGetKey];
      };
    } else if (typeof externalGetKey === 'function') {
      syntheticGetKey = function syntheticGetKey(node) {
        return externalGetKey(node);
      };
    }
  } else {
    syntheticGetKey = function syntheticGetKey(node, pos) {
      return getKey(node[fieldKey], pos);
    };
  }

  // Process
  function processNode(node, index, parent, pathNodes) {
    var children = node ? node[mergeChildrenPropName] : dataNodes;
    var pos = node ? getPosition(parent.pos, index) : '0';
    var connectNodes = node ? [].concat((0, _toConsumableArray2.default)(pathNodes), [node]) : [];

    // Process node if is not root
    if (node) {
      var key = syntheticGetKey(node, pos);
      var _data = {
        node: node,
        index: index,
        pos: pos,
        key: key,
        parentPos: parent.node ? parent.pos : null,
        level: parent.level + 1,
        nodes: connectNodes
      };
      callback(_data);
    }

    // Process children node
    if (children) {
      children.forEach(function (subNode, subIndex) {
        processNode(subNode, subIndex, {
          node: node,
          pos: pos,
          level: parent ? parent.level + 1 : -1
        }, connectNodes);
      });
    }
  }
  processNode(null);
}
/**
 * Convert `treeData` into entity records.
 */
function convertDataToEntities(dataNodes) {
  var _ref2 = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
    initWrapper = _ref2.initWrapper,
    processEntity = _ref2.processEntity,
    onProcessFinished = _ref2.onProcessFinished,
    externalGetKey = _ref2.externalGetKey,
    childrenPropName = _ref2.childrenPropName,
    fieldNames = _ref2.fieldNames;
  var /** @deprecated Use `config.externalGetKey` instead */
  legacyExternalGetKey = arguments.length > 2 ? arguments[2] : undefined;
  // Init config
  var mergedExternalGetKey = externalGetKey || legacyExternalGetKey;
  var posEntities = {};
  var keyEntities = {};
  var wrapper = {
    posEntities: posEntities,
    keyEntities: keyEntities
  };
  if (initWrapper) {
    wrapper = initWrapper(wrapper) || wrapper;
  }
  traverseDataNodes(dataNodes, function (item) {
    var node = item.node,
      index = item.index,
      pos = item.pos,
      key = item.key,
      parentPos = item.parentPos,
      level = item.level,
      nodes = item.nodes;
    var entity = {
      node: node,
      nodes: nodes,
      index: index,
      key: key,
      pos: pos,
      level: level
    };
    var mergedKey = getKey(key, pos);
    posEntities[pos] = entity;
    keyEntities[mergedKey] = entity;

    // Fill children
    entity.parent = posEntities[parentPos];
    if (entity.parent) {
      entity.parent.children = entity.parent.children || [];
      entity.parent.children.push(entity);
    }
    if (processEntity) {
      processEntity(entity, wrapper);
    }
  }, {
    externalGetKey: mergedExternalGetKey,
    childrenPropName: childrenPropName,
    fieldNames: fieldNames
  });
  if (onProcessFinished) {
    onProcessFinished(wrapper);
  }
  return wrapper;
}
/**
 * Get TreeNode props with Tree props.
 */
function getTreeNodeProps(key, _ref3) {
  var expandedKeys = _ref3.expandedKeys,
    selectedKeys = _ref3.selectedKeys,
    loadedKeys = _ref3.loadedKeys,
    loadingKeys = _ref3.loadingKeys,
    checkedKeys = _ref3.checkedKeys,
    halfCheckedKeys = _ref3.halfCheckedKeys,
    dragOverNodeKey = _ref3.dragOverNodeKey,
    dropPosition = _ref3.dropPosition,
    keyEntities = _ref3.keyEntities;
  var entity = (0, _keyUtil.default)(keyEntities, key);
  var treeNodeProps = {
    eventKey: key,
    expanded: expandedKeys.indexOf(key) !== -1,
    selected: selectedKeys.indexOf(key) !== -1,
    loaded: loadedKeys.indexOf(key) !== -1,
    loading: loadingKeys.indexOf(key) !== -1,
    checked: checkedKeys.indexOf(key) !== -1,
    halfChecked: halfCheckedKeys.indexOf(key) !== -1,
    pos: String(entity ? entity.pos : ''),
    // [Legacy] Drag props
    // Since the interaction of drag is changed, the semantic of the props are
    // not accuracy, I think it should be finally removed
    dragOver: dragOverNodeKey === key && dropPosition === 0,
    dragOverGapTop: dragOverNodeKey === key && dropPosition === -1,
    dragOverGapBottom: dragOverNodeKey === key && dropPosition === 1
  };
  return treeNodeProps;
}
function convertNodePropsToEventData(props) {
  var data = props.data,
    expanded = props.expanded,
    selected = props.selected,
    checked = props.checked,
    loaded = props.loaded,
    loading = props.loading,
    halfChecked = props.halfChecked,
    dragOver = props.dragOver,
    dragOverGapTop = props.dragOverGapTop,
    dragOverGapBottom = props.dragOverGapBottom,
    pos = props.pos,
    active = props.active,
    eventKey = props.eventKey;
  var eventData = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, data), {}, {
    expanded: expanded,
    selected: selected,
    checked: checked,
    loaded: loaded,
    loading: loading,
    halfChecked: halfChecked,
    dragOver: dragOver,
    dragOverGapTop: dragOverGapTop,
    dragOverGapBottom: dragOverGapBottom,
    pos: pos,
    active: active,
    key: eventKey
  });
  if (!('props' in eventData)) {
    Object.defineProperty(eventData, 'props', {
      get: function get() {
        (0, _warning.default)(false, 'Second param return from event is node data instead of TreeNode instance. Please read value directly instead of reading from `props`.');
        return props;
      }
    });
  }
  return eventData;
}