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
import CheckCircleFilled from "@ant-design/icons/es/icons/CheckCircleFilled";
import CheckOutlined from "@ant-design/icons/es/icons/CheckOutlined";
import CloseCircleFilled from "@ant-design/icons/es/icons/CloseCircleFilled";
import CloseOutlined from "@ant-design/icons/es/icons/CloseOutlined";
import classNames from 'classnames';
import omit from "rc-util/es/omit";
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import Circle from './Circle';
import Line from './Line';
import Steps from './Steps';
import useStyle from './style';
import { getSize, getSuccessPercent, validProgress } from './utils';
export const ProgressTypes = ['line', 'circle', 'dashboard'];
const ProgressStatuses = ['normal', 'exception', 'active', 'success'];
const Progress = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      className,
      rootClassName,
      steps,
      strokeColor,
      percent = 0,
      size = 'default',
      showInfo = true,
      type = 'line',
      status,
      format,
      style
    } = props,
    restProps = __rest(props, ["prefixCls", "className", "rootClassName", "steps", "strokeColor", "percent", "size", "showInfo", "type", "status", "format", "style"]);
  const percentNumber = React.useMemo(() => {
    var _a, _b;
    const successPercent = getSuccessPercent(props);
    return parseInt(successPercent !== undefined ? (_a = successPercent !== null && successPercent !== void 0 ? successPercent : 0) === null || _a === void 0 ? void 0 : _a.toString() : (_b = percent !== null && percent !== void 0 ? percent : 0) === null || _b === void 0 ? void 0 : _b.toString(), 10);
  }, [percent, props.success, props.successPercent]);
  const progressStatus = React.useMemo(() => {
    if (!ProgressStatuses.includes(status) && percentNumber >= 100) {
      return 'success';
    }
    return status || 'normal';
  }, [status, percentNumber]);
  const {
    getPrefixCls,
    direction,
    progress: progressStyle
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('progress', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const progressInfo = React.useMemo(() => {
    if (!showInfo) {
      return null;
    }
    const successPercent = getSuccessPercent(props);
    let text;
    const textFormatter = format || (number => `${number}%`);
    const isLineType = type === 'line';
    if (format || progressStatus !== 'exception' && progressStatus !== 'success') {
      text = textFormatter(validProgress(percent), validProgress(successPercent));
    } else if (progressStatus === 'exception') {
      text = isLineType ? /*#__PURE__*/React.createElement(CloseCircleFilled, null) : /*#__PURE__*/React.createElement(CloseOutlined, null);
    } else if (progressStatus === 'success') {
      text = isLineType ? /*#__PURE__*/React.createElement(CheckCircleFilled, null) : /*#__PURE__*/React.createElement(CheckOutlined, null);
    }
    return /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-text`,
      title: typeof text === 'string' ? text : undefined
    }, text);
  }, [showInfo, percent, percentNumber, progressStatus, type, prefixCls, format]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Progress');
    warning.deprecated(!('successPercent' in props), 'successPercent', 'success.percent');
    warning.deprecated(!('width' in props), 'width', 'size');
    if ((type === 'circle' || type === 'dashboard') && Array.isArray(size)) {
      process.env.NODE_ENV !== "production" ? warning(false, 'usage', 'Type "circle" and "dashboard" do not accept array as `size`, please use number or preset size instead.') : void 0;
    }
    if (props.success && 'progress' in props.success) {
      warning.deprecated(false, 'success.progress', 'success.percent');
    }
  }
  const strokeColorNotArray = Array.isArray(strokeColor) ? strokeColor[0] : strokeColor;
  const strokeColorNotGradient = typeof strokeColor === 'string' || Array.isArray(strokeColor) ? strokeColor : undefined;
  let progress;
  // Render progress shape
  if (type === 'line') {
    progress = steps ? ( /*#__PURE__*/React.createElement(Steps, Object.assign({}, props, {
      strokeColor: strokeColorNotGradient,
      prefixCls: prefixCls,
      steps: steps
    }), progressInfo)) : ( /*#__PURE__*/React.createElement(Line, Object.assign({}, props, {
      strokeColor: strokeColorNotArray,
      prefixCls: prefixCls,
      direction: direction
    }), progressInfo));
  } else if (type === 'circle' || type === 'dashboard') {
    progress = /*#__PURE__*/React.createElement(Circle, Object.assign({}, props, {
      strokeColor: strokeColorNotArray,
      prefixCls: prefixCls,
      progressStatus: progressStatus
    }), progressInfo);
  }
  const classString = classNames(prefixCls, `${prefixCls}-status-${progressStatus}`, `${prefixCls}-${type === 'dashboard' && 'circle' || steps && 'steps' || type}`, {
    [`${prefixCls}-inline-circle`]: type === 'circle' && getSize(size, 'circle')[0] <= 20,
    [`${prefixCls}-show-info`]: showInfo,
    [`${prefixCls}-${size}`]: typeof size === 'string',
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, progressStyle === null || progressStyle === void 0 ? void 0 : progressStyle.className, className, rootClassName, hashId, cssVarCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    ref: ref,
    style: Object.assign(Object.assign({}, progressStyle === null || progressStyle === void 0 ? void 0 : progressStyle.style), style),
    className: classString,
    role: "progressbar",
    "aria-valuenow": percentNumber
  }, omit(restProps, ['trailColor', 'strokeWidth', 'width', 'gapDegree', 'gapPosition', 'strokeLinecap', 'success', 'successPercent'])), progress));
});
if (process.env.NODE_ENV !== 'production') {
  Progress.displayName = 'Progress';
}
export default Progress;