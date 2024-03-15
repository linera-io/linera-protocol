"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _DoubleLeftOutlined = _interopRequireDefault(require("@ant-design/icons/DoubleLeftOutlined"));
var _DoubleRightOutlined = _interopRequireDefault(require("@ant-design/icons/DoubleRightOutlined"));
var _LeftOutlined = _interopRequireDefault(require("@ant-design/icons/LeftOutlined"));
var _RightOutlined = _interopRequireDefault(require("@ant-design/icons/RightOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcPagination = _interopRequireDefault(require("rc-pagination"));
var _en_US = _interopRequireDefault(require("rc-pagination/lib/locale/en_US"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _useSize = _interopRequireDefault(require("../config-provider/hooks/useSize"));
var _useBreakpoint = _interopRequireDefault(require("../grid/hooks/useBreakpoint"));
var _locale = require("../locale");
var _Select = require("./Select");
var _style = _interopRequireDefault(require("./style"));
var _internal = require("../theme/internal");
var _bordered = _interopRequireDefault(require("./style/bordered"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Pagination = props => {
  const {
      prefixCls: customizePrefixCls,
      selectPrefixCls: customizeSelectPrefixCls,
      className,
      rootClassName,
      style,
      size: customizeSize,
      locale: customLocale,
      selectComponentClass,
      responsive,
      showSizeChanger
    } = props,
    restProps = __rest(props, ["prefixCls", "selectPrefixCls", "className", "rootClassName", "style", "size", "locale", "selectComponentClass", "responsive", "showSizeChanger"]);
  const {
    xs
  } = (0, _useBreakpoint.default)(responsive);
  const [, token] = (0, _internal.useToken)();
  const {
    getPrefixCls,
    direction,
    pagination = {}
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('pagination', customizePrefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const mergedShowSizeChanger = showSizeChanger !== null && showSizeChanger !== void 0 ? showSizeChanger : pagination.showSizeChanger;
  const iconsProps = React.useMemo(() => {
    const ellipsis = /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-item-ellipsis`
    }, "\u2022\u2022\u2022");
    const prevIcon = /*#__PURE__*/React.createElement("button", {
      className: `${prefixCls}-item-link`,
      type: "button",
      tabIndex: -1
    }, direction === 'rtl' ? /*#__PURE__*/React.createElement(_RightOutlined.default, null) : /*#__PURE__*/React.createElement(_LeftOutlined.default, null));
    const nextIcon = /*#__PURE__*/React.createElement("button", {
      className: `${prefixCls}-item-link`,
      type: "button",
      tabIndex: -1
    }, direction === 'rtl' ? /*#__PURE__*/React.createElement(_LeftOutlined.default, null) : /*#__PURE__*/React.createElement(_RightOutlined.default, null));
    const jumpPrevIcon = /*#__PURE__*/React.createElement("a", {
      className: `${prefixCls}-item-link`
    }, /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-item-container`
    }, direction === 'rtl' ? ( /*#__PURE__*/React.createElement(_DoubleRightOutlined.default, {
      className: `${prefixCls}-item-link-icon`
    })) : ( /*#__PURE__*/React.createElement(_DoubleLeftOutlined.default, {
      className: `${prefixCls}-item-link-icon`
    })), ellipsis));
    const jumpNextIcon = /*#__PURE__*/React.createElement("a", {
      className: `${prefixCls}-item-link`
    }, /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-item-container`
    }, direction === 'rtl' ? ( /*#__PURE__*/React.createElement(_DoubleLeftOutlined.default, {
      className: `${prefixCls}-item-link-icon`
    })) : ( /*#__PURE__*/React.createElement(_DoubleRightOutlined.default, {
      className: `${prefixCls}-item-link-icon`
    })), ellipsis));
    return {
      prevIcon,
      nextIcon,
      jumpPrevIcon,
      jumpNextIcon
    };
  }, [direction, prefixCls]);
  const [contextLocale] = (0, _locale.useLocale)('Pagination', _en_US.default);
  const locale = Object.assign(Object.assign({}, contextLocale), customLocale);
  const mergedSize = (0, _useSize.default)(customizeSize);
  const isSmall = mergedSize === 'small' || !!(xs && !mergedSize && responsive);
  const selectPrefixCls = getPrefixCls('select', customizeSelectPrefixCls);
  const extendedClassName = (0, _classnames.default)({
    [`${prefixCls}-mini`]: isSmall,
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-bordered`]: token.wireframe
  }, pagination === null || pagination === void 0 ? void 0 : pagination.className, className, rootClassName, hashId, cssVarCls);
  const mergedStyle = Object.assign(Object.assign({}, pagination === null || pagination === void 0 ? void 0 : pagination.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement(React.Fragment, null, token.wireframe && /*#__PURE__*/React.createElement(_bordered.default, {
    prefixCls: prefixCls
  }), /*#__PURE__*/React.createElement(_rcPagination.default, Object.assign({}, iconsProps, restProps, {
    style: mergedStyle,
    prefixCls: prefixCls,
    selectPrefixCls: selectPrefixCls,
    className: extendedClassName,
    selectComponentClass: selectComponentClass || (isSmall ? _Select.MiniSelect : _Select.MiddleSelect),
    locale: locale,
    showSizeChanger: mergedShowSizeChanger
  }))));
};
if (process.env.NODE_ENV !== 'production') {
  Pagination.displayName = 'Pagination';
}
var _default = exports.default = Pagination;