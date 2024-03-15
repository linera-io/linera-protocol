"use client";

import * as React from 'react';
import { createContext, useContext, useMemo } from 'react';
import { FormProvider as RcFormProvider } from 'rc-field-form';
import omit from "rc-util/es/omit";
export const FormContext = /*#__PURE__*/React.createContext({
  labelAlign: 'right',
  vertical: false,
  itemRef: () => {}
});
export const NoStyleItemContext = /*#__PURE__*/React.createContext(null);
export const FormProvider = props => {
  const providerProps = omit(props, ['prefixCls']);
  return /*#__PURE__*/React.createElement(RcFormProvider, Object.assign({}, providerProps));
};
export const FormItemPrefixContext = /*#__PURE__*/React.createContext({
  prefixCls: ''
});
export const FormItemInputContext = /*#__PURE__*/React.createContext({});
if (process.env.NODE_ENV !== 'production') {
  FormItemInputContext.displayName = 'FormItemInputContext';
}
export const NoFormStyle = _ref => {
  let {
    children,
    status,
    override
  } = _ref;
  const formItemInputContext = useContext(FormItemInputContext);
  const newFormItemInputContext = useMemo(() => {
    const newContext = Object.assign({}, formItemInputContext);
    if (override) {
      delete newContext.isFormItemInput;
    }
    if (status) {
      delete newContext.status;
      delete newContext.hasFeedback;
      delete newContext.feedbackIcon;
    }
    return newContext;
  }, [status, override, formItemInputContext]);
  return /*#__PURE__*/React.createElement(FormItemInputContext.Provider, {
    value: newFormItemInputContext
  }, children);
};
export const VariantContext = /*#__PURE__*/createContext(undefined);