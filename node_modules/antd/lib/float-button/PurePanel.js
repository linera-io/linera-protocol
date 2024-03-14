"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _BackTop = _interopRequireDefault(require("./BackTop"));
var _FloatButton = _interopRequireWildcard(require("./FloatButton"));
var _FloatButtonGroup = _interopRequireDefault(require("./FloatButtonGroup"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
/* eslint-disable react/no-array-index-key */

const PureFloatButton = _a => {
  var {
      backTop
    } = _a,
    props = __rest(_a, ["backTop"]);
  return backTop ? /*#__PURE__*/React.createElement(_BackTop.default, Object.assign({}, props, {
    visibilityHeight: 0
  })) : /*#__PURE__*/React.createElement(_FloatButton.default, Object.assign({}, props));
};
/** @private Internal Component. Do not use in your production. */
const PurePanel = _a => {
  var {
      className,
      items
    } = _a,
    props = __rest(_a, ["className", "items"]);
  const {
    prefixCls: customizePrefixCls
  } = props;
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls(_FloatButton.floatButtonPrefixCls, customizePrefixCls);
  const pureCls = `${prefixCls}-pure`;
  if (items) {
    return /*#__PURE__*/React.createElement(_FloatButtonGroup.default, Object.assign({
      className: (0, _classnames.default)(className, pureCls)
    }, props), items.map((item, index) => ( /*#__PURE__*/React.createElement(PureFloatButton, Object.assign({
      key: index
    }, item)))));
  }
  return /*#__PURE__*/React.createElement(PureFloatButton, Object.assign({
    className: (0, _classnames.default)(className, pureCls)
  }, props));
};
var _default = exports.default = PurePanel;