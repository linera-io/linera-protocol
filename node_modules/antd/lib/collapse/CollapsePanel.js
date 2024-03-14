"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcCollapse = _interopRequireDefault(require("rc-collapse"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
const CollapsePanel = /*#__PURE__*/React.forwardRef((props, ref) => {
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Collapse.Panel');
    warning.deprecated(!('disabled' in props), 'disabled', 'collapsible="disabled"');
  }
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const {
    prefixCls: customizePrefixCls,
    className,
    showArrow = true
  } = props;
  const prefixCls = getPrefixCls('collapse', customizePrefixCls);
  const collapsePanelClassName = (0, _classnames.default)({
    [`${prefixCls}-no-arrow`]: !showArrow
  }, className);
  return /*#__PURE__*/React.createElement(_rcCollapse.default.Panel, Object.assign({
    ref: ref
  }, props, {
    prefixCls: prefixCls,
    className: collapsePanelClassName
  }));
});
var _default = exports.default = CollapsePanel;