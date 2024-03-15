import classNames from 'classnames';
import * as React from 'react';
import { isSameOrAfter } from "../utils/dateUtil";
import { PickerHackContext, usePanelContext } from "./context";
var HIDDEN_STYLE = {
  visibility: 'hidden'
};
function PanelHeader(props) {
  var offset = props.offset,
    superOffset = props.superOffset,
    onChange = props.onChange,
    getStart = props.getStart,
    getEnd = props.getEnd,
    children = props.children;
  var _usePanelContext = usePanelContext(),
    prefixCls = _usePanelContext.prefixCls,
    _usePanelContext$prev = _usePanelContext.prevIcon,
    prevIcon = _usePanelContext$prev === void 0 ? "\u2039" : _usePanelContext$prev,
    _usePanelContext$next = _usePanelContext.nextIcon,
    nextIcon = _usePanelContext$next === void 0 ? "\u203A" : _usePanelContext$next,
    _usePanelContext$supe = _usePanelContext.superPrevIcon,
    superPrevIcon = _usePanelContext$supe === void 0 ? "\xAB" : _usePanelContext$supe,
    _usePanelContext$supe2 = _usePanelContext.superNextIcon,
    superNextIcon = _usePanelContext$supe2 === void 0 ? "\xBB" : _usePanelContext$supe2,
    minDate = _usePanelContext.minDate,
    maxDate = _usePanelContext.maxDate,
    generateConfig = _usePanelContext.generateConfig,
    locale = _usePanelContext.locale,
    pickerValue = _usePanelContext.pickerValue,
    type = _usePanelContext.panelType;
  var headerPrefixCls = "".concat(prefixCls, "-header");
  var _React$useContext = React.useContext(PickerHackContext),
    hidePrev = _React$useContext.hidePrev,
    hideNext = _React$useContext.hideNext,
    hideHeader = _React$useContext.hideHeader;

  // ======================= Limitation =======================
  var disabledOffsetPrev = React.useMemo(function () {
    if (!minDate || !offset || !getEnd) {
      return false;
    }
    var prevPanelLimitDate = getEnd(offset(-1, pickerValue));
    return !isSameOrAfter(generateConfig, locale, prevPanelLimitDate, minDate, type);
  }, [minDate, offset, pickerValue, getEnd, generateConfig, locale, type]);
  var disabledSuperOffsetPrev = React.useMemo(function () {
    if (!minDate || !superOffset || !getEnd) {
      return false;
    }
    var prevPanelLimitDate = getEnd(superOffset(-1, pickerValue));
    return !isSameOrAfter(generateConfig, locale, prevPanelLimitDate, minDate, type);
  }, [minDate, superOffset, pickerValue, getEnd, generateConfig, locale, type]);
  var disabledOffsetNext = React.useMemo(function () {
    if (!maxDate || !offset || !getStart) {
      return false;
    }
    var nextPanelLimitDate = getStart(offset(1, pickerValue));
    return !isSameOrAfter(generateConfig, locale, maxDate, nextPanelLimitDate, type);
  }, [maxDate, offset, pickerValue, getStart, generateConfig, locale, type]);
  var disabledSuperOffsetNext = React.useMemo(function () {
    if (!maxDate || !superOffset || !getStart) {
      return false;
    }
    var nextPanelLimitDate = getStart(superOffset(1, pickerValue));
    return !isSameOrAfter(generateConfig, locale, maxDate, nextPanelLimitDate, type);
  }, [maxDate, superOffset, pickerValue, getStart, generateConfig, locale, type]);

  // ========================= Offset =========================
  var onOffset = function onOffset(distance) {
    if (offset) {
      onChange(offset(distance, pickerValue));
    }
  };
  var onSuperOffset = function onSuperOffset(distance) {
    if (superOffset) {
      onChange(superOffset(distance, pickerValue));
    }
  };

  // ========================= Render =========================
  if (hideHeader) {
    return null;
  }
  var prevBtnCls = "".concat(headerPrefixCls, "-prev-btn");
  var nextBtnCls = "".concat(headerPrefixCls, "-next-btn");
  var superPrevBtnCls = "".concat(headerPrefixCls, "-super-prev-btn");
  var superNextBtnCls = "".concat(headerPrefixCls, "-super-next-btn");
  return /*#__PURE__*/React.createElement("div", {
    className: headerPrefixCls
  }, superOffset && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: function onClick() {
      return onSuperOffset(-1);
    },
    tabIndex: -1,
    className: classNames(superPrevBtnCls, disabledSuperOffsetPrev && "".concat(superPrevBtnCls, "-disabled")),
    disabled: disabledSuperOffsetPrev,
    style: hidePrev ? HIDDEN_STYLE : {}
  }, superPrevIcon), offset && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: function onClick() {
      return onOffset(-1);
    },
    tabIndex: -1,
    className: classNames(prevBtnCls, disabledOffsetPrev && "".concat(prevBtnCls, "-disabled")),
    disabled: disabledOffsetPrev,
    style: hidePrev ? HIDDEN_STYLE : {}
  }, prevIcon), /*#__PURE__*/React.createElement("div", {
    className: "".concat(headerPrefixCls, "-view")
  }, children), offset && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: function onClick() {
      return onOffset(1);
    },
    tabIndex: -1,
    className: classNames(nextBtnCls, disabledOffsetNext && "".concat(nextBtnCls, "-disabled")),
    disabled: disabledOffsetNext,
    style: hideNext ? HIDDEN_STYLE : {}
  }, nextIcon), superOffset && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: function onClick() {
      return onSuperOffset(1);
    },
    tabIndex: -1,
    className: classNames(superNextBtnCls, disabledSuperOffsetNext && "".concat(superNextBtnCls, "-disabled")),
    disabled: disabledSuperOffsetNext,
    style: hideNext ? HIDDEN_STYLE : {}
  }, superNextIcon));
}
export default PanelHeader;