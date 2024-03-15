"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _react = _interopRequireDefault(require("react"));
var _Panel = _interopRequireDefault(require("../Panel"));
var _excluded = ["children", "label", "key", "collapsible", "onItemClick", "destroyInactivePanel"];
var convertItemsToNodes = function convertItemsToNodes(items, props) {
  var prefixCls = props.prefixCls,
    accordion = props.accordion,
    collapsible = props.collapsible,
    destroyInactivePanel = props.destroyInactivePanel,
    onItemClick = props.onItemClick,
    activeKey = props.activeKey,
    openMotion = props.openMotion,
    expandIcon = props.expandIcon;
  return items.map(function (item, index) {
    var children = item.children,
      label = item.label,
      rawKey = item.key,
      rawCollapsible = item.collapsible,
      rawOnItemClick = item.onItemClick,
      rawDestroyInactivePanel = item.destroyInactivePanel,
      restProps = (0, _objectWithoutProperties2.default)(item, _excluded);

    // You may be puzzled why you want to convert them all into strings, me too.
    // Maybe: https://github.com/react-component/collapse/blob/aac303a8b6ff30e35060b4f8fecde6f4556fcbe2/src/Collapse.tsx#L15
    var key = String(rawKey !== null && rawKey !== void 0 ? rawKey : index);
    var mergeCollapsible = rawCollapsible !== null && rawCollapsible !== void 0 ? rawCollapsible : collapsible;
    var mergeDestroyInactivePanel = rawDestroyInactivePanel !== null && rawDestroyInactivePanel !== void 0 ? rawDestroyInactivePanel : destroyInactivePanel;
    var handleItemClick = function handleItemClick(value) {
      if (mergeCollapsible === 'disabled') return;
      onItemClick(value);
      rawOnItemClick === null || rawOnItemClick === void 0 || rawOnItemClick(value);
    };
    var isActive = false;
    if (accordion) {
      isActive = activeKey[0] === key;
    } else {
      isActive = activeKey.indexOf(key) > -1;
    }
    return /*#__PURE__*/_react.default.createElement(_Panel.default, (0, _extends2.default)({}, restProps, {
      prefixCls: prefixCls,
      key: key,
      panelKey: key,
      isActive: isActive,
      accordion: accordion,
      openMotion: openMotion,
      expandIcon: expandIcon,
      header: label,
      collapsible: mergeCollapsible,
      onItemClick: handleItemClick,
      destroyInactivePanel: mergeDestroyInactivePanel
    }), children);
  });
};

/**
 * @deprecated The next major version will be removed
 */
var getNewChild = function getNewChild(child, index, props) {
  if (!child) return null;
  var prefixCls = props.prefixCls,
    accordion = props.accordion,
    collapsible = props.collapsible,
    destroyInactivePanel = props.destroyInactivePanel,
    onItemClick = props.onItemClick,
    activeKey = props.activeKey,
    openMotion = props.openMotion,
    expandIcon = props.expandIcon;
  var key = child.key || String(index);
  var _child$props = child.props,
    header = _child$props.header,
    headerClass = _child$props.headerClass,
    childDestroyInactivePanel = _child$props.destroyInactivePanel,
    childCollapsible = _child$props.collapsible,
    childOnItemClick = _child$props.onItemClick;
  var isActive = false;
  if (accordion) {
    isActive = activeKey[0] === key;
  } else {
    isActive = activeKey.indexOf(key) > -1;
  }
  var mergeCollapsible = childCollapsible !== null && childCollapsible !== void 0 ? childCollapsible : collapsible;
  var handleItemClick = function handleItemClick(value) {
    if (mergeCollapsible === 'disabled') return;
    onItemClick(value);
    childOnItemClick === null || childOnItemClick === void 0 || childOnItemClick(value);
  };
  var childProps = {
    key: key,
    panelKey: key,
    header: header,
    headerClass: headerClass,
    isActive: isActive,
    prefixCls: prefixCls,
    destroyInactivePanel: childDestroyInactivePanel !== null && childDestroyInactivePanel !== void 0 ? childDestroyInactivePanel : destroyInactivePanel,
    openMotion: openMotion,
    accordion: accordion,
    children: child.props.children,
    onItemClick: handleItemClick,
    expandIcon: expandIcon,
    collapsible: mergeCollapsible
  };

  // https://github.com/ant-design/ant-design/issues/20479
  if (typeof child.type === 'string') {
    return child;
  }
  Object.keys(childProps).forEach(function (propName) {
    if (typeof childProps[propName] === 'undefined') {
      delete childProps[propName];
    }
  });
  return /*#__PURE__*/_react.default.cloneElement(child, childProps);
};
function useItems(items, rawChildren, props) {
  if (Array.isArray(items)) {
    return convertItemsToNodes(items, props);
  }
  return (0, _toArray.default)(rawChildren).map(function (child, index) {
    return getNewChild(child, index, props);
  });
}
var _default = exports.default = useItems;