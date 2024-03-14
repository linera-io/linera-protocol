"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import CheckOutlined from "@ant-design/icons/es/icons/CheckOutlined";
import CloseOutlined from "@ant-design/icons/es/icons/CloseOutlined";
import classNames from 'classnames';
import RcSteps from 'rc-steps';
import * as React from 'react';
import { ConfigContext } from '../config-provider';
import useSize from '../config-provider/hooks/useSize';
import useBreakpoint from '../grid/hooks/useBreakpoint';
import Progress from '../progress';
import Tooltip from '../tooltip';
import useStyle from './style';
import useLegacyItems from './useLegacyItems';
const Steps = props => {
  const {
      percent,
      size: customizeSize,
      className,
      rootClassName,
      direction,
      items,
      responsive = true,
      current = 0,
      children,
      style
    } = props,
    restProps = __rest(props, ["percent", "size", "className", "rootClassName", "direction", "items", "responsive", "current", "children", "style"]);
  const {
    xs
  } = useBreakpoint(responsive);
  const {
    getPrefixCls,
    direction: rtlDirection,
    steps
  } = React.useContext(ConfigContext);
  const realDirectionValue = React.useMemo(() => responsive && xs ? 'vertical' : direction, [xs, direction]);
  const size = useSize(customizeSize);
  const prefixCls = getPrefixCls('steps', props.prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const isInline = props.type === 'inline';
  const iconPrefix = getPrefixCls('', props.iconPrefix);
  const mergedItems = useLegacyItems(items, children);
  const mergedPercent = isInline ? undefined : percent;
  const mergedStyle = Object.assign(Object.assign({}, steps === null || steps === void 0 ? void 0 : steps.style), style);
  const stepsClassName = classNames(steps === null || steps === void 0 ? void 0 : steps.className, {
    [`${prefixCls}-rtl`]: rtlDirection === 'rtl',
    [`${prefixCls}-with-progress`]: mergedPercent !== undefined
  }, className, rootClassName, hashId, cssVarCls);
  const icons = {
    finish: /*#__PURE__*/React.createElement(CheckOutlined, {
      className: `${prefixCls}-finish-icon`
    }),
    error: /*#__PURE__*/React.createElement(CloseOutlined, {
      className: `${prefixCls}-error-icon`
    })
  };
  const stepIconRender = _ref => {
    let {
      node,
      status
    } = _ref;
    if (status === 'process' && mergedPercent !== undefined) {
      // currently it's hard-coded, since we can't easily read the actually width of icon
      const progressWidth = size === 'small' ? 32 : 40;
      // iconWithProgress
      return /*#__PURE__*/React.createElement("div", {
        className: `${prefixCls}-progress-icon`
      }, /*#__PURE__*/React.createElement(Progress, {
        type: "circle",
        percent: mergedPercent,
        size: progressWidth,
        strokeWidth: 4,
        format: () => null
      }), node);
    }
    return node;
  };
  const itemRender = (item, stepItem) => item.description ? /*#__PURE__*/React.createElement(Tooltip, {
    title: item.description
  }, stepItem) : stepItem;
  return wrapCSSVar( /*#__PURE__*/React.createElement(RcSteps, Object.assign({
    icons: icons
  }, restProps, {
    style: mergedStyle,
    current: current,
    size: size,
    items: mergedItems,
    itemRender: isInline ? itemRender : undefined,
    stepIcon: stepIconRender,
    direction: realDirectionValue,
    prefixCls: prefixCls,
    iconPrefix: iconPrefix,
    className: stepsClassName
  })));
};
Steps.Step = RcSteps.Step;
if (process.env.NODE_ENV !== 'production') {
  Steps.displayName = 'Steps';
}
export default Steps;