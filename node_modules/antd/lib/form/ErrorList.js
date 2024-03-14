"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireWildcard(require("rc-motion"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _motion = _interopRequireDefault(require("../_util/motion"));
var _context = require("./context");
var _useDebounce = _interopRequireDefault(require("./hooks/useDebounce"));
var _style = _interopRequireDefault(require("./style"));
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
const EMPTY_LIST = [];
function toErrorEntity(error, prefix, errorStatus) {
  let index = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 0;
  return {
    key: typeof error === 'string' ? error : `${prefix}-${index}`,
    error,
    errorStatus
  };
}
const ErrorList = _ref => {
  let {
    help,
    helpStatus,
    errors = EMPTY_LIST,
    warnings = EMPTY_LIST,
    className: rootClassName,
    fieldId,
    onVisibleChanged
  } = _ref;
  const {
    prefixCls
  } = React.useContext(_context.FormItemPrefixContext);
  const baseClassName = `${prefixCls}-item-explain`;
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const collapseMotion = (0, _react.useMemo)(() => (0, _motion.default)(prefixCls), [prefixCls]);
  // We have to debounce here again since somewhere use ErrorList directly still need no shaking
  // ref: https://github.com/ant-design/ant-design/issues/36336
  const debounceErrors = (0, _useDebounce.default)(errors);
  const debounceWarnings = (0, _useDebounce.default)(warnings);
  const fullKeyList = React.useMemo(() => {
    if (help !== undefined && help !== null) {
      return [toErrorEntity(help, 'help', helpStatus)];
    }
    return [].concat((0, _toConsumableArray2.default)(debounceErrors.map((error, index) => toErrorEntity(error, 'error', 'error', index))), (0, _toConsumableArray2.default)(debounceWarnings.map((warning, index) => toErrorEntity(warning, 'warning', 'warning', index))));
  }, [help, helpStatus, debounceErrors, debounceWarnings]);
  const helpProps = {};
  if (fieldId) {
    helpProps.id = `${fieldId}_help`;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcMotion.default, {
    motionDeadline: collapseMotion.motionDeadline,
    motionName: `${prefixCls}-show-help`,
    visible: !!fullKeyList.length,
    onVisibleChanged: onVisibleChanged
  }, holderProps => {
    const {
      className: holderClassName,
      style: holderStyle
    } = holderProps;
    return /*#__PURE__*/React.createElement("div", Object.assign({}, helpProps, {
      className: (0, _classnames.default)(baseClassName, holderClassName, cssVarCls, rootCls, rootClassName, hashId),
      style: holderStyle,
      role: "alert"
    }), /*#__PURE__*/React.createElement(_rcMotion.CSSMotionList, Object.assign({
      keys: fullKeyList
    }, (0, _motion.default)(prefixCls), {
      motionName: `${prefixCls}-show-help-item`,
      component: false
    }), itemProps => {
      const {
        key,
        error,
        errorStatus,
        className: itemClassName,
        style: itemStyle
      } = itemProps;
      return /*#__PURE__*/React.createElement("div", {
        key: key,
        className: (0, _classnames.default)(itemClassName, {
          [`${baseClassName}-${errorStatus}`]: errorStatus
        }),
        style: itemStyle
      }, error);
    }));
  }));
};
var _default = exports.default = ErrorList;