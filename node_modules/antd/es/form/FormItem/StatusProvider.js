"use client";

import * as React from 'react';
import CheckCircleFilled from "@ant-design/icons/es/icons/CheckCircleFilled";
import CloseCircleFilled from "@ant-design/icons/es/icons/CloseCircleFilled";
import ExclamationCircleFilled from "@ant-design/icons/es/icons/ExclamationCircleFilled";
import LoadingOutlined from "@ant-design/icons/es/icons/LoadingOutlined";
import classNames from 'classnames';
import { FormContext, FormItemInputContext } from '../context';
import { getStatus } from '../util';
const iconMap = {
  success: CheckCircleFilled,
  warning: ExclamationCircleFilled,
  error: CloseCircleFilled,
  validating: LoadingOutlined
};
export default function StatusProvider(_ref) {
  let {
    children,
    errors,
    warnings,
    hasFeedback,
    validateStatus,
    prefixCls,
    meta,
    noStyle
  } = _ref;
  const itemPrefixCls = `${prefixCls}-item`;
  const {
    feedbackIcons
  } = React.useContext(FormContext);
  const mergedValidateStatus = getStatus(errors, warnings, meta, null, !!hasFeedback, validateStatus);
  const {
    isFormItemInput: parentIsFormItemInput,
    status: parentStatus,
    hasFeedback: parentHasFeedback,
    feedbackIcon: parentFeedbackIcon
  } = React.useContext(FormItemInputContext);
  // ====================== Context =======================
  const formItemStatusContext = React.useMemo(() => {
    var _a;
    let feedbackIcon;
    if (hasFeedback) {
      const customIcons = hasFeedback !== true && hasFeedback.icons || feedbackIcons;
      const customIconNode = mergedValidateStatus && ((_a = customIcons === null || customIcons === void 0 ? void 0 : customIcons({
        status: mergedValidateStatus,
        errors,
        warnings
      })) === null || _a === void 0 ? void 0 : _a[mergedValidateStatus]);
      const IconNode = mergedValidateStatus && iconMap[mergedValidateStatus];
      feedbackIcon = customIconNode !== false && IconNode ? ( /*#__PURE__*/React.createElement("span", {
        className: classNames(`${itemPrefixCls}-feedback-icon`, `${itemPrefixCls}-feedback-icon-${mergedValidateStatus}`)
      }, customIconNode || /*#__PURE__*/React.createElement(IconNode, null))) : null;
    }
    const context = {
      status: mergedValidateStatus || '',
      errors,
      warnings,
      hasFeedback: !!hasFeedback,
      feedbackIcon,
      isFormItemInput: true
    };
    // No style will follow parent context
    if (noStyle) {
      context.status = (mergedValidateStatus !== null && mergedValidateStatus !== void 0 ? mergedValidateStatus : parentStatus) || '';
      context.isFormItemInput = parentIsFormItemInput;
      context.hasFeedback = !!(hasFeedback !== null && hasFeedback !== void 0 ? hasFeedback : parentHasFeedback);
      context.feedbackIcon = hasFeedback !== undefined ? context.feedbackIcon : parentFeedbackIcon;
    }
    return context;
  }, [mergedValidateStatus, hasFeedback, noStyle, parentIsFormItemInput, parentStatus]);
  // ======================= Render =======================
  return /*#__PURE__*/React.createElement(FormItemInputContext.Provider, {
    value: formItemStatusContext
  }, children);
}