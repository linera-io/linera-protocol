"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useItemRender;
exports.renderItem = renderItem;
var _classnames = _interopRequireDefault(require("classnames"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var React = _interopRequireWildcard(require("react"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
function getBreadcrumbName(route, params) {
  if (route.title === undefined || route.title === null) {
    return null;
  }
  const paramsKeys = Object.keys(params).join('|');
  return typeof route.title === 'object' ? route.title : String(route.title).replace(new RegExp(`:(${paramsKeys})`, 'g'), (replacement, key) => params[key] || replacement);
}
function renderItem(prefixCls, item, children, href) {
  if (children === null || children === undefined) {
    return null;
  }
  const {
      className,
      onClick
    } = item,
    restItem = __rest(item, ["className", "onClick"]);
  const passedProps = Object.assign(Object.assign({}, (0, _pickAttrs.default)(restItem, {
    data: true,
    aria: true
  })), {
    onClick
  });
  if (href !== undefined) {
    return /*#__PURE__*/React.createElement("a", Object.assign({}, passedProps, {
      className: (0, _classnames.default)(`${prefixCls}-link`, className),
      href: href
    }), children);
  }
  return /*#__PURE__*/React.createElement("span", Object.assign({}, passedProps, {
    className: (0, _classnames.default)(`${prefixCls}-link`, className)
  }), children);
}
function useItemRender(prefixCls, itemRender) {
  const mergedItemRender = (item, params, routes, path, href) => {
    if (itemRender) {
      return itemRender(item, params, routes, path);
    }
    const name = getBreadcrumbName(item, params);
    return renderItem(prefixCls, item, name, href);
  };
  return mergedItemRender;
}