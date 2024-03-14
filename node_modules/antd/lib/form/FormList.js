"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _rcFieldForm = require("rc-field-form");
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _context = require("./context");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const FormList = _a => {
  var {
      prefixCls: customizePrefixCls,
      children
    } = _a,
    props = __rest(_a, ["prefixCls", "children"]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Form.List');
    process.env.NODE_ENV !== "production" ? warning(typeof props.name === 'number' || (Array.isArray(props.name) ? !!props.name.length : !!props.name), 'usage', 'Miss `name` prop.') : void 0;
  }
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('form', customizePrefixCls);
  const contextValue = React.useMemo(() => ({
    prefixCls,
    status: 'error'
  }), [prefixCls]);
  return /*#__PURE__*/React.createElement(_rcFieldForm.List, Object.assign({}, props), (fields, operation, meta) => ( /*#__PURE__*/React.createElement(_context.FormItemPrefixContext.Provider, {
    value: contextValue
  }, children(fields.map(field => Object.assign(Object.assign({}, field), {
    fieldKey: field.key
  })), operation, {
    errors: meta.errors,
    warnings: meta.warnings
  }))));
};
var _default = exports.default = FormList;