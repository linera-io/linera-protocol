import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["showArrow", "headerClass", "isActive", "onItemClick", "forceRender", "className", "prefixCls", "collapsible", "accordion", "panelKey", "extra", "header", "expandIcon", "openMotion", "destroyInactivePanel", "children"];
import classNames from 'classnames';
import CSSMotion from 'rc-motion';
import KeyCode from "rc-util/es/KeyCode";
import React from 'react';
import PanelContent from "./PanelContent";
var CollapsePanel = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _classNames, _classNames2;
  var _props$showArrow = props.showArrow,
    showArrow = _props$showArrow === void 0 ? true : _props$showArrow,
    headerClass = props.headerClass,
    isActive = props.isActive,
    onItemClick = props.onItemClick,
    forceRender = props.forceRender,
    className = props.className,
    prefixCls = props.prefixCls,
    collapsible = props.collapsible,
    accordion = props.accordion,
    panelKey = props.panelKey,
    extra = props.extra,
    header = props.header,
    expandIcon = props.expandIcon,
    openMotion = props.openMotion,
    destroyInactivePanel = props.destroyInactivePanel,
    children = props.children,
    resetProps = _objectWithoutProperties(props, _excluded);
  var disabled = collapsible === 'disabled';
  var collapsibleHeader = collapsible === 'header';
  var collapsibleIcon = collapsible === 'icon';
  var ifExtraExist = extra !== null && extra !== undefined && typeof extra !== 'boolean';
  var handleItemClick = function handleItemClick() {
    onItemClick === null || onItemClick === void 0 || onItemClick(panelKey);
  };
  var handleKeyDown = function handleKeyDown(e) {
    if (e.key === 'Enter' || e.keyCode === KeyCode.ENTER || e.which === KeyCode.ENTER) {
      handleItemClick();
    }
  };

  // ======================== Icon ========================
  var iconNode = typeof expandIcon === 'function' ? expandIcon(props) : /*#__PURE__*/React.createElement("i", {
    className: "arrow"
  });
  if (iconNode) {
    iconNode = /*#__PURE__*/React.createElement("div", {
      className: "".concat(prefixCls, "-expand-icon"),
      onClick: ['header', 'icon'].includes(collapsible) ? handleItemClick : undefined
    }, iconNode);
  }
  var collapsePanelClassNames = classNames((_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-item"), true), _defineProperty(_classNames, "".concat(prefixCls, "-item-active"), isActive), _defineProperty(_classNames, "".concat(prefixCls, "-item-disabled"), disabled), _classNames), className);
  var headerClassName = classNames(headerClass, (_classNames2 = {}, _defineProperty(_classNames2, "".concat(prefixCls, "-header"), true), _defineProperty(_classNames2, "".concat(prefixCls, "-header-collapsible-only"), collapsibleHeader), _defineProperty(_classNames2, "".concat(prefixCls, "-icon-collapsible-only"), collapsibleIcon), _classNames2));

  // ======================== HeaderProps ========================
  var headerProps = {
    className: headerClassName,
    'aria-expanded': isActive,
    'aria-disabled': disabled,
    onKeyDown: handleKeyDown
  };
  if (!collapsibleHeader && !collapsibleIcon) {
    headerProps.onClick = handleItemClick;
    headerProps.role = accordion ? 'tab' : 'button';
    headerProps.tabIndex = disabled ? -1 : 0;
  }

  // ======================== Render ========================
  return /*#__PURE__*/React.createElement("div", _extends({}, resetProps, {
    ref: ref,
    className: collapsePanelClassNames
  }), /*#__PURE__*/React.createElement("div", headerProps, showArrow && iconNode, /*#__PURE__*/React.createElement("span", {
    className: "".concat(prefixCls, "-header-text"),
    onClick: collapsible === 'header' ? handleItemClick : undefined
  }, header), ifExtraExist && /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-extra")
  }, extra)), /*#__PURE__*/React.createElement(CSSMotion, _extends({
    visible: isActive,
    leavedClassName: "".concat(prefixCls, "-content-hidden")
  }, openMotion, {
    forceRender: forceRender,
    removeOnLeave: destroyInactivePanel
  }), function (_ref, motionRef) {
    var motionClassName = _ref.className,
      motionStyle = _ref.style;
    return /*#__PURE__*/React.createElement(PanelContent, {
      ref: motionRef,
      prefixCls: prefixCls,
      className: motionClassName,
      style: motionStyle,
      isActive: isActive,
      forceRender: forceRender,
      role: accordion ? 'tabpanel' : void 0
    }, children);
  }));
});
export default CollapsePanel;