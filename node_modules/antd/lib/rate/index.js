"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _StarFilled = _interopRequireDefault(require("@ant-design/icons/StarFilled"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcRate = _interopRequireDefault(require("rc-rate"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _tooltip = _interopRequireDefault(require("../tooltip"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Rate = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls,
      className,
      rootClassName,
      style,
      tooltips,
      character = /*#__PURE__*/React.createElement(_StarFilled.default, null)
    } = props,
    rest = __rest(props, ["prefixCls", "className", "rootClassName", "style", "tooltips", "character"]);
  const characterRender = (node, _ref) => {
    let {
      index
    } = _ref;
    if (!tooltips) {
      return node;
    }
    return /*#__PURE__*/React.createElement(_tooltip.default, {
      title: tooltips[index]
    }, node);
  };
  const {
    getPrefixCls,
    direction,
    rate
  } = React.useContext(_configProvider.ConfigContext);
  const ratePrefixCls = getPrefixCls('rate', prefixCls);
  // Style
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(ratePrefixCls);
  const mergedStyle = Object.assign(Object.assign({}, rate === null || rate === void 0 ? void 0 : rate.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcRate.default, Object.assign({
    ref: ref,
    character: character,
    characterRender: characterRender
  }, rest, {
    className: (0, _classnames.default)(className, rootClassName, hashId, cssVarCls, rate === null || rate === void 0 ? void 0 : rate.className),
    style: mergedStyle,
    prefixCls: ratePrefixCls,
    direction: direction
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Rate.displayName = 'Rate';
}
var _default = exports.default = Rate;