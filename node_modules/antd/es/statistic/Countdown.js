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
import useForceUpdate from '../_util/hooks/useForceUpdate';
import { cloneElement } from '../_util/reactNode';
import Statistic from './Statistic';
import { formatCountdown } from './utils';
const REFRESH_INTERVAL = 1000 / 30;
function getTime(value) {
  return new Date(value).getTime();
}
const Countdown = props => {
  const {
      value,
      format = 'HH:mm:ss',
      onChange,
      onFinish
    } = props,
    rest = __rest(props, ["value", "format", "onChange", "onFinish"]);
  const forceUpdate = useForceUpdate();
  const countdown = React.useRef(null);
  const stopTimer = () => {
    onFinish === null || onFinish === void 0 ? void 0 : onFinish();
    if (countdown.current) {
      clearInterval(countdown.current);
      countdown.current = null;
    }
  };
  const syncTimer = () => {
    const timestamp = getTime(value);
    if (timestamp >= Date.now()) {
      countdown.current = setInterval(() => {
        forceUpdate();
        onChange === null || onChange === void 0 ? void 0 : onChange(timestamp - Date.now());
        if (timestamp < Date.now()) {
          stopTimer();
        }
      }, REFRESH_INTERVAL);
    }
  };
  React.useEffect(() => {
    syncTimer();
    return () => {
      if (countdown.current) {
        clearInterval(countdown.current);
        countdown.current = null;
      }
    };
  }, [value]);
  const formatter = (formatValue, config) => formatCountdown(formatValue, Object.assign(Object.assign({}, config), {
    format
  }));
  const valueRender = node => cloneElement(node, {
    title: undefined
  });
  return /*#__PURE__*/React.createElement(Statistic, Object.assign({}, rest, {
    value: value,
    valueRender: valueRender,
    formatter: formatter
  }));
};
export default /*#__PURE__*/React.memo(Countdown);