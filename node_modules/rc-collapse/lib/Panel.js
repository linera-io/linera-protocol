"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _react = _interopRequireDefault(require("react"));
var _PanelContent = _interopRequireDefault(require("./PanelContent"));
var _excluded = ["showArrow", "headerClass", "isActive", "onItemClick", "forceRender", "className", "prefixCls", "collapsible", "accordion", "panelKey", "extra", "header", "expandIcon", "openMotion", "destroyInactivePanel", "children"];
var CollapsePanel = /*#__PURE__*/_react.default.forwardRef(function (props, ref) {
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
    resetProps = (0, _objectWithoutProperties2.default)(props, _excluded);
  var disabled = collapsible === 'disabled';
  var collapsibleHeader = collapsible === 'header';
  var collapsibleIcon = collapsible === 'icon';
  var ifExtraExist = extra !== null && extra !== undefined && typeof extra !== 'boolean';
  var handleItemClick = function handleItemClick() {
    onItemClick === null || onItemClick === void 0 || onItemClick(panelKey);
  };
  var handleKeyDown = function handleKeyDown(e) {
    if (e.key === 'Enter' || e.keyCode === _KeyCode.default.ENTER || e.which === _KeyCode.default.ENTER) {
      handleItemClick();
    }
  };

  // ======================== Icon ========================
  var iconNode = typeof expandIcon === 'function' ? expandIcon(props) : /*#__PURE__*/_react.default.createElement("i", {
    className: "arrow"
  });
  if (iconNode) {
    iconNode = /*#__PURE__*/_react.default.createElement("div", {
      className: "".concat(prefixCls, "-expand-icon"),
      onClick: ['header', 'icon'].includes(collapsible) ? handleItemClick : undefined
    }, iconNode);
  }
  var collapsePanelClassNames = (0, _classnames.default)((_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-item"), true), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-item-active"), isActive), (0, _defineProperty2.default)(_classNames, "".concat(prefixCls, "-item-disabled"), disabled), _classNames), className);
  var headerClassName = (0, _classnames.default)(headerClass, (_classNames2 = {}, (0, _defineProperty2.default)(_classNames2, "".concat(prefixCls, "-header"), true), (0, _defineProperty2.default)(_classNames2, "".concat(prefixCls, "-header-collapsible-only"), collapsibleHeader), (0, _defineProperty2.default)(_classNames2, "".concat(prefixCls, "-icon-collapsible-only"), collapsibleIcon), _classNames2));

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
  return /*#__PURE__*/_react.default.createElement("div", (0, _extends2.default)({}, resetProps, {
    ref: ref,
    className: collapsePanelClassNames
  }), /*#__PURE__*/_react.default.createElement("div", headerProps, showArrow && iconNode, /*#__PURE__*/_react.default.createElement("span", {
    className: "".concat(prefixCls, "-header-text"),
    onClick: collapsible === 'header' ? handleItemClick : undefined
  }, header), ifExtraExist && /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-extra")
  }, extra)), /*#__PURE__*/_react.default.createElement(_rcMotion.default, (0, _extends2.default)({
    visible: isActive,
    leavedClassName: "".concat(prefixCls, "-content-hidden")
  }, openMotion, {
    forceRender: forceRender,
    removeOnLeave: destroyInactivePanel
  }), function (_ref, motionRef) {
    var motionClassName = _ref.className,
      motionStyle = _ref.style;
    return /*#__PURE__*/_react.default.createElement(_PanelContent.default, {
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
var _default = exports.default = CollapsePanel;