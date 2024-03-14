"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
var _FileOutlined = _interopRequireDefault(require("@ant-design/icons/FileOutlined"));
var _FolderOpenOutlined = _interopRequireDefault(require("@ant-design/icons/FolderOpenOutlined"));
var _FolderOutlined = _interopRequireDefault(require("@ant-design/icons/FolderOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _util = require("rc-tree/lib/util");
var _treeUtil = require("rc-tree/lib/utils/treeUtil");
var _configProvider = require("../config-provider");
var _Tree = _interopRequireDefault(require("./Tree"));
var _dictUtil = require("./utils/dictUtil");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
function getIcon(props) {
  const {
    isLeaf,
    expanded
  } = props;
  if (isLeaf) {
    return /*#__PURE__*/React.createElement(_FileOutlined.default, null);
  }
  return expanded ? /*#__PURE__*/React.createElement(_FolderOpenOutlined.default, null) : /*#__PURE__*/React.createElement(_FolderOutlined.default, null);
}
function getTreeData(_ref) {
  let {
    treeData,
    children
  } = _ref;
  return treeData || (0, _treeUtil.convertTreeToData)(children);
}
const DirectoryTree = (_a, ref) => {
  var {
      defaultExpandAll,
      defaultExpandParent,
      defaultExpandedKeys
    } = _a,
    props = __rest(_a, ["defaultExpandAll", "defaultExpandParent", "defaultExpandedKeys"]);
  // Shift click usage
  const lastSelectedKey = React.useRef();
  const cachedSelectedKeys = React.useRef();
  const getInitExpandedKeys = () => {
    const {
      keyEntities
    } = (0, _treeUtil.convertDataToEntities)(getTreeData(props));
    let initExpandedKeys;
    // Expanded keys
    if (defaultExpandAll) {
      initExpandedKeys = Object.keys(keyEntities);
    } else if (defaultExpandParent) {
      initExpandedKeys = (0, _util.conductExpandParent)(props.expandedKeys || defaultExpandedKeys || [], keyEntities);
    } else {
      initExpandedKeys = props.expandedKeys || defaultExpandedKeys || [];
    }
    return initExpandedKeys;
  };
  const [selectedKeys, setSelectedKeys] = React.useState(props.selectedKeys || props.defaultSelectedKeys || []);
  const [expandedKeys, setExpandedKeys] = React.useState(() => getInitExpandedKeys());
  React.useEffect(() => {
    if ('selectedKeys' in props) {
      setSelectedKeys(props.selectedKeys);
    }
  }, [props.selectedKeys]);
  React.useEffect(() => {
    if ('expandedKeys' in props) {
      setExpandedKeys(props.expandedKeys);
    }
  }, [props.expandedKeys]);
  const onExpand = (keys, info) => {
    var _a;
    if (!('expandedKeys' in props)) {
      setExpandedKeys(keys);
    }
    // Call origin function
    return (_a = props.onExpand) === null || _a === void 0 ? void 0 : _a.call(props, keys, info);
  };
  const onSelect = (keys, event) => {
    var _a;
    const {
      multiple,
      fieldNames
    } = props;
    const {
      node,
      nativeEvent
    } = event;
    const {
      key = ''
    } = node;
    const treeData = getTreeData(props);
    // const newState: DirectoryTreeState = {};
    // We need wrap this event since some value is not same
    const newEvent = Object.assign(Object.assign({}, event), {
      selected: true
    });
    // Windows / Mac single pick
    const ctrlPick = (nativeEvent === null || nativeEvent === void 0 ? void 0 : nativeEvent.ctrlKey) || (nativeEvent === null || nativeEvent === void 0 ? void 0 : nativeEvent.metaKey);
    const shiftPick = nativeEvent === null || nativeEvent === void 0 ? void 0 : nativeEvent.shiftKey;
    // Generate new selected keys
    let newSelectedKeys;
    if (multiple && ctrlPick) {
      // Control click
      newSelectedKeys = keys;
      lastSelectedKey.current = key;
      cachedSelectedKeys.current = newSelectedKeys;
      newEvent.selectedNodes = (0, _dictUtil.convertDirectoryKeysToNodes)(treeData, newSelectedKeys, fieldNames);
    } else if (multiple && shiftPick) {
      // Shift click
      newSelectedKeys = Array.from(new Set([].concat((0, _toConsumableArray2.default)(cachedSelectedKeys.current || []), (0, _toConsumableArray2.default)((0, _dictUtil.calcRangeKeys)({
        treeData,
        expandedKeys,
        startKey: key,
        endKey: lastSelectedKey.current,
        fieldNames
      })))));
      newEvent.selectedNodes = (0, _dictUtil.convertDirectoryKeysToNodes)(treeData, newSelectedKeys, fieldNames);
    } else {
      // Single click
      newSelectedKeys = [key];
      lastSelectedKey.current = key;
      cachedSelectedKeys.current = newSelectedKeys;
      newEvent.selectedNodes = (0, _dictUtil.convertDirectoryKeysToNodes)(treeData, newSelectedKeys, fieldNames);
    }
    (_a = props.onSelect) === null || _a === void 0 ? void 0 : _a.call(props, newSelectedKeys, newEvent);
    if (!('selectedKeys' in props)) {
      setSelectedKeys(newSelectedKeys);
    }
  };
  const {
    getPrefixCls,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const {
      prefixCls: customizePrefixCls,
      className,
      showIcon = true,
      expandAction = 'click'
    } = props,
    otherProps = __rest(props, ["prefixCls", "className", "showIcon", "expandAction"]);
  const prefixCls = getPrefixCls('tree', customizePrefixCls);
  const connectClassName = (0, _classnames.default)(`${prefixCls}-directory`, {
    [`${prefixCls}-directory-rtl`]: direction === 'rtl'
  }, className);
  return /*#__PURE__*/React.createElement(_Tree.default, Object.assign({
    icon: getIcon,
    ref: ref,
    blockNode: true
  }, otherProps, {
    showIcon: showIcon,
    expandAction: expandAction,
    prefixCls: prefixCls,
    className: connectClassName,
    expandedKeys: expandedKeys,
    selectedKeys: selectedKeys,
    onSelect: onSelect,
    onExpand: onExpand
  }));
};
const ForwardDirectoryTree = /*#__PURE__*/React.forwardRef(DirectoryTree);
if (process.env.NODE_ENV !== 'production') {
  ForwardDirectoryTree.displayName = 'DirectoryTree';
}
var _default = exports.default = ForwardDirectoryTree;