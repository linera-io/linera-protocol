"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.IconMap = exports.ExceptionMap = void 0;
var React = _interopRequireWildcard(require("react"));
var _CheckCircleFilled = _interopRequireDefault(require("@ant-design/icons/CheckCircleFilled"));
var _CloseCircleFilled = _interopRequireDefault(require("@ant-design/icons/CloseCircleFilled"));
var _ExclamationCircleFilled = _interopRequireDefault(require("@ant-design/icons/ExclamationCircleFilled"));
var _WarningFilled = _interopRequireDefault(require("@ant-design/icons/WarningFilled"));
var _classnames = _interopRequireDefault(require("classnames"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _noFound = _interopRequireDefault(require("./noFound"));
var _serverError = _interopRequireDefault(require("./serverError"));
var _style = _interopRequireDefault(require("./style"));
var _unauthorized = _interopRequireDefault(require("./unauthorized"));
const IconMap = exports.IconMap = {
  success: _CheckCircleFilled.default,
  error: _CloseCircleFilled.default,
  info: _ExclamationCircleFilled.default,
  warning: _WarningFilled.default
};
const ExceptionMap = exports.ExceptionMap = {
  '404': _noFound.default,
  '500': _serverError.default,
  '403': _unauthorized.default
};
// ExceptionImageMap keys
const ExceptionStatus = Object.keys(ExceptionMap);
const Icon = _ref => {
  let {
    prefixCls,
    icon,
    status
  } = _ref;
  const className = (0, _classnames.default)(`${prefixCls}-icon`);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Result');
    process.env.NODE_ENV !== "production" ? warning(!(typeof icon === 'string' && icon.length > 2), 'breaking', `\`icon\` is using ReactNode instead of string naming in v4. Please check \`${icon}\` at https://ant.design/components/icon`) : void 0;
  }
  if (ExceptionStatus.includes(`${status}`)) {
    const SVGComponent = ExceptionMap[status];
    return /*#__PURE__*/React.createElement("div", {
      className: `${className} ${prefixCls}-image`
    }, /*#__PURE__*/React.createElement(SVGComponent, null));
  }
  const iconNode = /*#__PURE__*/React.createElement(IconMap[status]);
  if (icon === null || icon === false) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: className
  }, icon || iconNode);
};
const Extra = _ref2 => {
  let {
    prefixCls,
    extra
  } = _ref2;
  if (!extra) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-extra`
  }, extra);
};
const Result = _ref3 => {
  let {
    prefixCls: customizePrefixCls,
    className: customizeClassName,
    rootClassName,
    subTitle,
    title,
    style,
    children,
    status = 'info',
    icon,
    extra
  } = _ref3;
  const {
    getPrefixCls,
    direction,
    result
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('result', customizePrefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const className = (0, _classnames.default)(prefixCls, `${prefixCls}-${status}`, customizeClassName, result === null || result === void 0 ? void 0 : result.className, rootClassName, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, hashId, cssVarCls);
  const mergedStyle = Object.assign(Object.assign({}, result === null || result === void 0 ? void 0 : result.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: className,
    style: mergedStyle
  }, /*#__PURE__*/React.createElement(Icon, {
    prefixCls: prefixCls,
    status: status,
    icon: icon
  }), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-title`
  }, title), subTitle && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-subtitle`
  }, subTitle), /*#__PURE__*/React.createElement(Extra, {
    prefixCls: prefixCls,
    extra: extra
  }), children && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-content`
  }, children)));
};
Result.PRESENTED_IMAGE_403 = ExceptionMap['403'];
Result.PRESENTED_IMAGE_404 = ExceptionMap['404'];
Result.PRESENTED_IMAGE_500 = ExceptionMap['500'];
if (process.env.NODE_ENV !== 'production') {
  Result.displayName = 'Result';
}
var _default = exports.default = Result;