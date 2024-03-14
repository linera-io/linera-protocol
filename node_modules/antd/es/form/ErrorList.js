"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import classNames from 'classnames';
import CSSMotion, { CSSMotionList } from 'rc-motion';
import * as React from 'react';
import { useMemo } from 'react';
import initCollapseMotion from '../_util/motion';
import { FormItemPrefixContext } from './context';
import useDebounce from './hooks/useDebounce';
import useStyle from './style';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
const EMPTY_LIST = [];
function toErrorEntity(error, prefix, errorStatus) {
  let index = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 0;
  return {
    key: typeof error === 'string' ? error : `${prefix}-${index}`,
    error,
    errorStatus
  };
}
const ErrorList = _ref => {
  let {
    help,
    helpStatus,
    errors = EMPTY_LIST,
    warnings = EMPTY_LIST,
    className: rootClassName,
    fieldId,
    onVisibleChanged
  } = _ref;
  const {
    prefixCls
  } = React.useContext(FormItemPrefixContext);
  const baseClassName = `${prefixCls}-item-explain`;
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
  const collapseMotion = useMemo(() => initCollapseMotion(prefixCls), [prefixCls]);
  // We have to debounce here again since somewhere use ErrorList directly still need no shaking
  // ref: https://github.com/ant-design/ant-design/issues/36336
  const debounceErrors = useDebounce(errors);
  const debounceWarnings = useDebounce(warnings);
  const fullKeyList = React.useMemo(() => {
    if (help !== undefined && help !== null) {
      return [toErrorEntity(help, 'help', helpStatus)];
    }
    return [].concat(_toConsumableArray(debounceErrors.map((error, index) => toErrorEntity(error, 'error', 'error', index))), _toConsumableArray(debounceWarnings.map((warning, index) => toErrorEntity(warning, 'warning', 'warning', index))));
  }, [help, helpStatus, debounceErrors, debounceWarnings]);
  const helpProps = {};
  if (fieldId) {
    helpProps.id = `${fieldId}_help`;
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement(CSSMotion, {
    motionDeadline: collapseMotion.motionDeadline,
    motionName: `${prefixCls}-show-help`,
    visible: !!fullKeyList.length,
    onVisibleChanged: onVisibleChanged
  }, holderProps => {
    const {
      className: holderClassName,
      style: holderStyle
    } = holderProps;
    return /*#__PURE__*/React.createElement("div", Object.assign({}, helpProps, {
      className: classNames(baseClassName, holderClassName, cssVarCls, rootCls, rootClassName, hashId),
      style: holderStyle,
      role: "alert"
    }), /*#__PURE__*/React.createElement(CSSMotionList, Object.assign({
      keys: fullKeyList
    }, initCollapseMotion(prefixCls), {
      motionName: `${prefixCls}-show-help-item`,
      component: false
    }), itemProps => {
      const {
        key,
        error,
        errorStatus,
        className: itemClassName,
        style: itemStyle
      } = itemProps;
      return /*#__PURE__*/React.createElement("div", {
        key: key,
        className: classNames(itemClassName, {
          [`${baseClassName}-${errorStatus}`]: errorStatus
        }),
        style: itemStyle
      }, error);
    }));
  }));
};
export default ErrorList;