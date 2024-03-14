"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import QuestionCircleOutlined from "@ant-design/icons/es/icons/QuestionCircleOutlined";
import classNames from 'classnames';
import * as React from 'react';
import Col from '../grid/col';
import defaultLocale from '../locale/en_US';
import { useLocale } from '../locale';
import Tooltip from '../tooltip';
import { FormContext } from './context';
function toTooltipProps(tooltip) {
  if (!tooltip) {
    return null;
  }
  if (typeof tooltip === 'object' && ! /*#__PURE__*/React.isValidElement(tooltip)) {
    return tooltip;
  }
  return {
    title: tooltip
  };
}
const FormItemLabel = _ref => {
  let {
    prefixCls,
    label,
    htmlFor,
    labelCol,
    labelAlign,
    colon,
    required,
    requiredMark,
    tooltip
  } = _ref;
  var _a;
  const [formLocale] = useLocale('Form');
  const {
    vertical,
    labelAlign: contextLabelAlign,
    labelCol: contextLabelCol,
    labelWrap,
    colon: contextColon
  } = React.useContext(FormContext);
  if (!label) {
    return null;
  }
  const mergedLabelCol = labelCol || contextLabelCol || {};
  const mergedLabelAlign = labelAlign || contextLabelAlign;
  const labelClsBasic = `${prefixCls}-item-label`;
  const labelColClassName = classNames(labelClsBasic, mergedLabelAlign === 'left' && `${labelClsBasic}-left`, mergedLabelCol.className, {
    [`${labelClsBasic}-wrap`]: !!labelWrap
  });
  let labelChildren = label;
  // Keep label is original where there should have no colon
  const computedColon = colon === true || contextColon !== false && colon !== false;
  const haveColon = computedColon && !vertical;
  // Remove duplicated user input colon
  if (haveColon && typeof label === 'string' && label.trim() !== '') {
    labelChildren = label.replace(/[:|：]\s*$/, '');
  }
  // Tooltip
  const tooltipProps = toTooltipProps(tooltip);
  if (tooltipProps) {
    const {
        icon = /*#__PURE__*/React.createElement(QuestionCircleOutlined, null)
      } = tooltipProps,
      restTooltipProps = __rest(tooltipProps, ["icon"]);
    const tooltipNode = /*#__PURE__*/React.createElement(Tooltip, Object.assign({}, restTooltipProps), /*#__PURE__*/React.cloneElement(icon, {
      className: `${prefixCls}-item-tooltip`,
      title: '',
      onClick: e => {
        // Prevent label behavior in tooltip icon
        // https://github.com/ant-design/ant-design/issues/46154
        e.preventDefault();
      },
      tabIndex: null
    }));
    labelChildren = /*#__PURE__*/React.createElement(React.Fragment, null, labelChildren, tooltipNode);
  }
  // Required Mark
  const isOptionalMark = requiredMark === 'optional';
  const isRenderMark = typeof requiredMark === 'function';
  if (isRenderMark) {
    labelChildren = requiredMark(labelChildren, {
      required: !!required
    });
  } else if (isOptionalMark && !required) {
    labelChildren = /*#__PURE__*/React.createElement(React.Fragment, null, labelChildren, /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-item-optional`,
      title: ""
    }, (formLocale === null || formLocale === void 0 ? void 0 : formLocale.optional) || ((_a = defaultLocale.Form) === null || _a === void 0 ? void 0 : _a.optional)));
  }
  const labelClassName = classNames({
    [`${prefixCls}-item-required`]: required,
    [`${prefixCls}-item-required-mark-optional`]: isOptionalMark || isRenderMark,
    [`${prefixCls}-item-no-colon`]: !computedColon
  });
  return /*#__PURE__*/React.createElement(Col, Object.assign({}, mergedLabelCol, {
    className: labelColClassName
  }), /*#__PURE__*/React.createElement("label", {
    htmlFor: htmlFor,
    className: labelClassName,
    title: typeof label === 'string' ? label : ''
  }, labelChildren));
};
export default FormItemLabel;