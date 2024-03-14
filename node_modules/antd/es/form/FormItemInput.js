"use client";

import * as React from 'react';
import classNames from 'classnames';
import Col from '../grid/col';
import { FormContext, FormItemPrefixContext } from './context';
import ErrorList from './ErrorList';
import FallbackCmp from './style/fallbackCmp';
const FormItemInput = props => {
  const {
    prefixCls,
    status,
    wrapperCol,
    children,
    errors,
    warnings,
    _internalItemRender: formItemRender,
    extra,
    help,
    fieldId,
    marginBottom,
    onErrorVisibleChanged
  } = props;
  const baseClassName = `${prefixCls}-item`;
  const formContext = React.useContext(FormContext);
  const mergedWrapperCol = wrapperCol || formContext.wrapperCol || {};
  const className = classNames(`${baseClassName}-control`, mergedWrapperCol.className);
  // Pass to sub FormItem should not with col info
  const subFormContext = React.useMemo(() => Object.assign({}, formContext), [formContext]);
  delete subFormContext.labelCol;
  delete subFormContext.wrapperCol;
  const inputDom = /*#__PURE__*/React.createElement("div", {
    className: `${baseClassName}-control-input`
  }, /*#__PURE__*/React.createElement("div", {
    className: `${baseClassName}-control-input-content`
  }, children));
  const formItemContext = React.useMemo(() => ({
    prefixCls,
    status
  }), [prefixCls, status]);
  const errorListDom = marginBottom !== null || errors.length || warnings.length ? ( /*#__PURE__*/React.createElement("div", {
    style: {
      display: 'flex',
      flexWrap: 'nowrap'
    }
  }, /*#__PURE__*/React.createElement(FormItemPrefixContext.Provider, {
    value: formItemContext
  }, /*#__PURE__*/React.createElement(ErrorList, {
    fieldId: fieldId,
    errors: errors,
    warnings: warnings,
    help: help,
    helpStatus: status,
    className: `${baseClassName}-explain-connected`,
    onVisibleChanged: onErrorVisibleChanged
  })), !!marginBottom && /*#__PURE__*/React.createElement("div", {
    style: {
      width: 0,
      height: marginBottom
    }
  }))) : null;
  const extraProps = {};
  if (fieldId) {
    extraProps.id = `${fieldId}_extra`;
  }
  // If extra = 0, && will goes wrong
  // 0&&error -> 0
  const extraDom = extra ? ( /*#__PURE__*/React.createElement("div", Object.assign({}, extraProps, {
    className: `${baseClassName}-extra`
  }), extra)) : null;
  const dom = formItemRender && formItemRender.mark === 'pro_table_render' && formItemRender.render ? formItemRender.render(props, {
    input: inputDom,
    errorList: errorListDom,
    extra: extraDom
  }) : ( /*#__PURE__*/React.createElement(React.Fragment, null, inputDom, errorListDom, extraDom));
  return /*#__PURE__*/React.createElement(FormContext.Provider, {
    value: subFormContext
  }, /*#__PURE__*/React.createElement(Col, Object.assign({}, mergedWrapperCol, {
    className: className
  }), dom), /*#__PURE__*/React.createElement(FallbackCmp, {
    prefixCls: prefixCls
  }));
};
export default FormItemInput;