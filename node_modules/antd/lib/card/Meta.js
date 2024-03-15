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
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Meta = props => {
  const {
      prefixCls: customizePrefixCls,
      className,
      avatar,
      title,
      description
    } = props,
    others = __rest(props, ["prefixCls", "className", "avatar", "title", "description"]);
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('card', customizePrefixCls);
  const classString = (0, _classnames.default)(`${prefixCls}-meta`, className);
  const avatarDom = avatar ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-avatar`
  }, avatar)) : null;
  const titleDom = title ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-title`
  }, title)) : null;
  const descriptionDom = description ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-description`
  }, description)) : null;
  const MetaDetail = titleDom || descriptionDom ? ( /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-meta-detail`
  }, titleDom, descriptionDom)) : null;
  return /*#__PURE__*/React.createElement("div", Object.assign({}, others, {
    className: classString
  }), avatarDom, MetaDetail);
};
var _default = exports.default = Meta;