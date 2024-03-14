"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useMergedState3 = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _react = _interopRequireDefault(require("react"));
var _useItems = _interopRequireDefault(require("./hooks/useItems"));
var _Panel = _interopRequireDefault(require("./Panel"));
function getActiveKeysArray(activeKey) {
  var currentActiveKey = activeKey;
  if (!Array.isArray(currentActiveKey)) {
    var activeKeyType = (0, _typeof2.default)(currentActiveKey);
    currentActiveKey = activeKeyType === 'number' || activeKeyType === 'string' ? [currentActiveKey] : [];
  }
  return currentActiveKey.map(function (key) {
    return String(key);
  });
}
var Collapse = /*#__PURE__*/_react.default.forwardRef(function (props, ref) {
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-collapse' : _props$prefixCls,
    _props$destroyInactiv = props.destroyInactivePanel,
    destroyInactivePanel = _props$destroyInactiv === void 0 ? false : _props$destroyInactiv,
    style = props.style,
    accordion = props.accordion,
    className = props.className,
    children = props.children,
    collapsible = props.collapsible,
    openMotion = props.openMotion,
    expandIcon = props.expandIcon,
    rawActiveKey = props.activeKey,
    defaultActiveKey = props.defaultActiveKey,
    _onChange = props.onChange,
    items = props.items;
  var collapseClassName = (0, _classnames.default)(prefixCls, className);
  var _useMergedState = (0, _useMergedState3.default)([], {
      value: rawActiveKey,
      onChange: function onChange(v) {
        return _onChange === null || _onChange === void 0 ? void 0 : _onChange(v);
      },
      defaultValue: defaultActiveKey,
      postState: getActiveKeysArray
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    activeKey = _useMergedState2[0],
    setActiveKey = _useMergedState2[1];
  var onItemClick = function onItemClick(key) {
    return setActiveKey(function () {
      if (accordion) {
        return activeKey[0] === key ? [] : [key];
      }
      var index = activeKey.indexOf(key);
      var isActive = index > -1;
      if (isActive) {
        return activeKey.filter(function (item) {
          return item !== key;
        });
      }
      return [].concat((0, _toConsumableArray2.default)(activeKey), [key]);
    });
  };

  // ======================== Children ========================
  (0, _warning.default)(!children, '[rc-collapse] `children` will be removed in next major version. Please use `items` instead.');
  var mergedChildren = (0, _useItems.default)(items, children, {
    prefixCls: prefixCls,
    accordion: accordion,
    openMotion: openMotion,
    expandIcon: expandIcon,
    collapsible: collapsible,
    destroyInactivePanel: destroyInactivePanel,
    onItemClick: onItemClick,
    activeKey: activeKey
  });

  // ======================== Render ========================
  return /*#__PURE__*/_react.default.createElement("div", {
    ref: ref,
    className: collapseClassName,
    style: style,
    role: accordion ? 'tablist' : undefined
  }, mergedChildren);
});
var _default = exports.default = Object.assign(Collapse, {
  /**
   * @deprecated use `items` instead, will be removed in `v4.0.0`
   */
  Panel: _Panel.default
});