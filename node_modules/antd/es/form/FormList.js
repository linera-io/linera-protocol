"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import { List } from 'rc-field-form';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import { FormItemPrefixContext } from './context';
const FormList = _a => {
  var {
      prefixCls: customizePrefixCls,
      children
    } = _a,
    props = __rest(_a, ["prefixCls", "children"]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Form.List');
    process.env.NODE_ENV !== "production" ? warning(typeof props.name === 'number' || (Array.isArray(props.name) ? !!props.name.length : !!props.name), 'usage', 'Miss `name` prop.') : void 0;
  }
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('form', customizePrefixCls);
  const contextValue = React.useMemo(() => ({
    prefixCls,
    status: 'error'
  }), [prefixCls]);
  return /*#__PURE__*/React.createElement(List, Object.assign({}, props), (fields, operation, meta) => ( /*#__PURE__*/React.createElement(FormItemPrefixContext.Provider, {
    value: contextValue
  }, children(fields.map(field => Object.assign(Object.assign({}, field), {
    fieldKey: field.key
  })), operation, {
    errors: meta.errors,
    warnings: meta.warnings
  }))));
};
export default FormList;