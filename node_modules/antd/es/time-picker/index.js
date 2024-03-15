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
import genPurePanel from '../_util/PurePanel';
import { devUseWarning } from '../_util/warning';
import DatePicker from '../date-picker';
const {
  TimePicker: InternalTimePicker,
  RangePicker: InternalRangePicker
} = DatePicker;
const RangePicker = /*#__PURE__*/React.forwardRef((props, ref) => ( /*#__PURE__*/React.createElement(InternalRangePicker, Object.assign({}, props, {
  picker: "time",
  mode: undefined,
  ref: ref
}))));
const TimePicker = /*#__PURE__*/React.forwardRef((_a, ref) => {
  var {
      addon,
      renderExtraFooter
    } = _a,
    restProps = __rest(_a, ["addon", "renderExtraFooter"]);
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('TimePicker');
    warning.deprecated(!addon, 'addon', 'renderExtraFooter');
  }
  const internalRenderExtraFooter = React.useMemo(() => {
    if (renderExtraFooter) {
      return renderExtraFooter;
    }
    if (addon) {
      return addon;
    }
    return undefined;
  }, [addon, renderExtraFooter]);
  return /*#__PURE__*/React.createElement(InternalTimePicker, Object.assign({}, restProps, {
    mode: undefined,
    ref: ref,
    renderExtraFooter: internalRenderExtraFooter
  }));
});
if (process.env.NODE_ENV !== 'production') {
  TimePicker.displayName = 'TimePicker';
}
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = genPurePanel(TimePicker, 'picker');
TimePicker._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
TimePicker.RangePicker = RangePicker;
TimePicker._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
export default TimePicker;