"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.MotionEntity = exports.MOTION_KEY = void 0;
exports.getMinimumRangeTransitionRange = getMinimumRangeTransitionRange;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectDestructuringEmpty2 = _interopRequireDefault(require("@babel/runtime/helpers/objectDestructuringEmpty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _rcVirtualList = _interopRequireDefault(require("rc-virtual-list"));
var React = _interopRequireWildcard(require("react"));
var _MotionTreeNode = _interopRequireDefault(require("./MotionTreeNode"));
var _diffUtil = require("./utils/diffUtil");
var _treeUtil = require("./utils/treeUtil");
var _excluded = ["prefixCls", "data", "selectable", "checkable", "expandedKeys", "selectedKeys", "checkedKeys", "loadedKeys", "loadingKeys", "halfCheckedKeys", "keyEntities", "disabled", "dragging", "dragOverNodeKey", "dropPosition", "motion", "height", "itemHeight", "virtual", "focusable", "activeItem", "focused", "tabIndex", "onKeyDown", "onFocus", "onBlur", "onActiveChange", "onListChangeStart", "onListChangeEnd"];
/**
 * Handle virtual list of the TreeNodes.
 */
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var HIDDEN_STYLE = {
  width: 0,
  height: 0,
  display: 'flex',
  overflow: 'hidden',
  opacity: 0,
  border: 0,
  padding: 0,
  margin: 0
};
var noop = function noop() {};
var MOTION_KEY = exports.MOTION_KEY = "RC_TREE_MOTION_".concat(Math.random());
var MotionNode = {
  key: MOTION_KEY
};
var MotionEntity = exports.MotionEntity = {
  key: MOTION_KEY,
  level: 0,
  index: 0,
  pos: '0',
  node: MotionNode,
  nodes: [MotionNode]
};
var MotionFlattenData = {
  parent: null,
  children: [],
  pos: MotionEntity.pos,
  data: MotionNode,
  title: null,
  key: MOTION_KEY,
  /** Hold empty list here since we do not use it */
  isStart: [],
  isEnd: []
};
/**
 * We only need get visible content items to play the animation.
 */
function getMinimumRangeTransitionRange(list, virtual, height, itemHeight) {
  if (virtual === false || !height) {
    return list;
  }
  return list.slice(0, Math.ceil(height / itemHeight) + 1);
}
function itemKey(item) {
  var key = item.key,
    pos = item.pos;
  return (0, _treeUtil.getKey)(key, pos);
}
function getAccessibilityPath(item) {
  var path = String(item.data.key);
  var current = item;
  while (current.parent) {
    current = current.parent;
    path = "".concat(current.data.key, " > ").concat(path);
  }
  return path;
}
var NodeList = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    data = props.data,
    selectable = props.selectable,
    checkable = props.checkable,
    expandedKeys = props.expandedKeys,
    selectedKeys = props.selectedKeys,
    checkedKeys = props.checkedKeys,
    loadedKeys = props.loadedKeys,
    loadingKeys = props.loadingKeys,
    halfCheckedKeys = props.halfCheckedKeys,
    keyEntities = props.keyEntities,
    disabled = props.disabled,
    dragging = props.dragging,
    dragOverNodeKey = props.dragOverNodeKey,
    dropPosition = props.dropPosition,
    motion = props.motion,
    height = props.height,
    itemHeight = props.itemHeight,
    virtual = props.virtual,
    focusable = props.focusable,
    activeItem = props.activeItem,
    focused = props.focused,
    tabIndex = props.tabIndex,
    onKeyDown = props.onKeyDown,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    onActiveChange = props.onActiveChange,
    onListChangeStart = props.onListChangeStart,
    onListChangeEnd = props.onListChangeEnd,
    domProps = (0, _objectWithoutProperties2.default)(props, _excluded);

  // =============================== Ref ================================
  var listRef = React.useRef(null);
  var indentMeasurerRef = React.useRef(null);
  React.useImperativeHandle(ref, function () {
    return {
      scrollTo: function scrollTo(scroll) {
        listRef.current.scrollTo(scroll);
      },
      getIndentWidth: function getIndentWidth() {
        return indentMeasurerRef.current.offsetWidth;
      }
    };
  });

  // ============================== Motion ==============================
  var _React$useState = React.useState(expandedKeys),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    prevExpandedKeys = _React$useState2[0],
    setPrevExpandedKeys = _React$useState2[1];
  var _React$useState3 = React.useState(data),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    prevData = _React$useState4[0],
    setPrevData = _React$useState4[1];
  var _React$useState5 = React.useState(data),
    _React$useState6 = (0, _slicedToArray2.default)(_React$useState5, 2),
    transitionData = _React$useState6[0],
    setTransitionData = _React$useState6[1];
  var _React$useState7 = React.useState([]),
    _React$useState8 = (0, _slicedToArray2.default)(_React$useState7, 2),
    transitionRange = _React$useState8[0],
    setTransitionRange = _React$useState8[1];
  var _React$useState9 = React.useState(null),
    _React$useState10 = (0, _slicedToArray2.default)(_React$useState9, 2),
    motionType = _React$useState10[0],
    setMotionType = _React$useState10[1];

  // When motion end but data change, this will makes data back to previous one
  var dataRef = React.useRef(data);
  dataRef.current = data;
  function onMotionEnd() {
    var latestData = dataRef.current;
    setPrevData(latestData);
    setTransitionData(latestData);
    setTransitionRange([]);
    setMotionType(null);
    onListChangeEnd();
  }

  // Do animation if expanded keys changed
  // layoutEffect here to avoid blink of node removing
  (0, _useLayoutEffect.default)(function () {
    setPrevExpandedKeys(expandedKeys);
    var diffExpanded = (0, _diffUtil.findExpandedKeys)(prevExpandedKeys, expandedKeys);
    if (diffExpanded.key !== null) {
      if (diffExpanded.add) {
        var keyIndex = prevData.findIndex(function (_ref) {
          var key = _ref.key;
          return key === diffExpanded.key;
        });
        var rangeNodes = getMinimumRangeTransitionRange((0, _diffUtil.getExpandRange)(prevData, data, diffExpanded.key), virtual, height, itemHeight);
        var newTransitionData = prevData.slice();
        newTransitionData.splice(keyIndex + 1, 0, MotionFlattenData);
        setTransitionData(newTransitionData);
        setTransitionRange(rangeNodes);
        setMotionType('show');
      } else {
        var _keyIndex = data.findIndex(function (_ref2) {
          var key = _ref2.key;
          return key === diffExpanded.key;
        });
        var _rangeNodes = getMinimumRangeTransitionRange((0, _diffUtil.getExpandRange)(data, prevData, diffExpanded.key), virtual, height, itemHeight);
        var _newTransitionData = data.slice();
        _newTransitionData.splice(_keyIndex + 1, 0, MotionFlattenData);
        setTransitionData(_newTransitionData);
        setTransitionRange(_rangeNodes);
        setMotionType('hide');
      }
    } else if (prevData !== data) {
      // If whole data changed, we just refresh the list
      setPrevData(data);
      setTransitionData(data);
    }
  }, [expandedKeys, data]);

  // We should clean up motion if is changed by dragging
  React.useEffect(function () {
    if (!dragging) {
      onMotionEnd();
    }
  }, [dragging]);
  var mergedData = motion ? transitionData : data;
  var treeNodeRequiredProps = {
    expandedKeys: expandedKeys,
    selectedKeys: selectedKeys,
    loadedKeys: loadedKeys,
    loadingKeys: loadingKeys,
    checkedKeys: checkedKeys,
    halfCheckedKeys: halfCheckedKeys,
    dragOverNodeKey: dragOverNodeKey,
    dropPosition: dropPosition,
    keyEntities: keyEntities
  };
  return /*#__PURE__*/React.createElement(React.Fragment, null, focused && activeItem && /*#__PURE__*/React.createElement("span", {
    style: HIDDEN_STYLE,
    "aria-live": "assertive"
  }, getAccessibilityPath(activeItem)), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement("input", {
    style: HIDDEN_STYLE,
    disabled: focusable === false || disabled,
    tabIndex: focusable !== false ? tabIndex : null,
    onKeyDown: onKeyDown,
    onFocus: onFocus,
    onBlur: onBlur,
    value: "",
    onChange: noop,
    "aria-label": "for screen reader"
  })), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-treenode"),
    "aria-hidden": true,
    style: {
      position: 'absolute',
      pointerEvents: 'none',
      visibility: 'hidden',
      height: 0,
      overflow: 'hidden',
      border: 0,
      padding: 0
    }
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-indent")
  }, /*#__PURE__*/React.createElement("div", {
    ref: indentMeasurerRef,
    className: "".concat(prefixCls, "-indent-unit")
  }))), /*#__PURE__*/React.createElement(_rcVirtualList.default, (0, _extends2.default)({}, domProps, {
    data: mergedData,
    itemKey: itemKey,
    height: height,
    fullHeight: false,
    virtual: virtual,
    itemHeight: itemHeight,
    prefixCls: "".concat(prefixCls, "-list"),
    ref: listRef,
    onVisibleChange: function onVisibleChange(originList, fullList) {
      var originSet = new Set(originList);
      var restList = fullList.filter(function (item) {
        return !originSet.has(item);
      });

      // Motion node is not render. Skip motion
      if (restList.some(function (item) {
        return itemKey(item) === MOTION_KEY;
      })) {
        onMotionEnd();
      }
    }
  }), function (treeNode) {
    var pos = treeNode.pos,
      restProps = Object.assign({}, ((0, _objectDestructuringEmpty2.default)(treeNode.data), treeNode.data)),
      title = treeNode.title,
      key = treeNode.key,
      isStart = treeNode.isStart,
      isEnd = treeNode.isEnd;
    var mergedKey = (0, _treeUtil.getKey)(key, pos);
    delete restProps.key;
    delete restProps.children;
    var treeNodeProps = (0, _treeUtil.getTreeNodeProps)(mergedKey, treeNodeRequiredProps);
    return /*#__PURE__*/React.createElement(_MotionTreeNode.default, (0, _extends2.default)({}, restProps, treeNodeProps, {
      title: title,
      active: !!activeItem && key === activeItem.key,
      pos: pos,
      data: treeNode.data,
      isStart: isStart,
      isEnd: isEnd,
      motion: motion,
      motionNodes: key === MOTION_KEY ? transitionRange : null,
      motionType: motionType,
      onMotionStart: onListChangeStart,
      onMotionEnd: onMotionEnd,
      treeNodeRequiredProps: treeNodeRequiredProps,
      onMouseMove: function onMouseMove() {
        onActiveChange(null);
      }
    }));
  }));
});
NodeList.displayName = 'NodeList';
var _default = exports.default = NodeList;