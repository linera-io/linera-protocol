"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _warning = require("../_util/warning");
var _Base = _interopRequireDefault(require("./Base"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Link = /*#__PURE__*/React.forwardRef((_a, ref) => {
  var {
      ellipsis,
      rel
    } = _a,
    restProps = __rest(_a, ["ellipsis", "rel"]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Typography.Link');
    process.env.NODE_ENV !== "production" ? warning(typeof ellipsis !== 'object', 'usage', '`ellipsis` only supports boolean value.') : void 0;
  }
  const mergedProps = Object.assign(Object.assign({}, restProps), {
    rel: rel === undefined && restProps.target === '_blank' ? 'noopener noreferrer' : rel
  });
  // @ts-expect-error: https://github.com/ant-design/ant-design/issues/26622
  delete mergedProps.navigate;
  return /*#__PURE__*/React.createElement(_Base.default, Object.assign({}, mergedProps, {
    ref: ref,
    ellipsis: !!ellipsis,
    component: "a"
  }));
});
var _default = exports.default = Link;