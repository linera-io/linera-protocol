"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.GroupSizeContext = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _internal = require("../theme/internal");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const GroupSizeContext = exports.GroupSizeContext = /*#__PURE__*/React.createContext(undefined);
const ButtonGroup = props => {
  const {
    getPrefixCls,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const {
      prefixCls: customizePrefixCls,
      size,
      className
    } = props,
    others = __rest(props, ["prefixCls", "size", "className"]);
  const prefixCls = getPrefixCls('btn-group', customizePrefixCls);
  const [,, hashId] = (0, _internal.useToken)();
  let sizeCls = '';
  switch (size) {
    case 'large':
      sizeCls = 'lg';
      break;
    case 'small':
      sizeCls = 'sm';
      break;
    case 'middle':
    default:
    // Do nothing
  }
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Button.Group');
    process.env.NODE_ENV !== "production" ? warning(!size || ['large', 'small', 'middle'].includes(size), 'usage', 'Invalid prop `size`.') : void 0;
  }
  const classes = (0, _classnames.default)(prefixCls, {
    [`${prefixCls}-${sizeCls}`]: sizeCls,
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, hashId);
  return /*#__PURE__*/React.createElement(GroupSizeContext.Provider, {
    value: size
  }, /*#__PURE__*/React.createElement("div", Object.assign({}, others, {
    className: classes
  })));
};
var _default = exports.default = ButtonGroup;