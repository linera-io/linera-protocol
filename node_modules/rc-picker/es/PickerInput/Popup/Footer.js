import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import * as React from 'react';
import useTimeInfo from "../../hooks/useTimeInfo";
import PickerContext from "../context";
export default function Footer(props) {
  var mode = props.mode,
    internalMode = props.internalMode,
    renderExtraFooter = props.renderExtraFooter,
    showNow = props.showNow,
    showTime = props.showTime,
    onSubmit = props.onSubmit,
    onNow = props.onNow,
    invalid = props.invalid,
    needConfirm = props.needConfirm,
    generateConfig = props.generateConfig,
    disabledDate = props.disabledDate;
  var _React$useContext = React.useContext(PickerContext),
    prefixCls = _React$useContext.prefixCls,
    locale = _React$useContext.locale,
    _React$useContext$but = _React$useContext.button,
    Button = _React$useContext$but === void 0 ? 'button' : _React$useContext$but;

  // >>> Now
  var now = generateConfig.getNow();
  var _useTimeInfo = useTimeInfo(generateConfig, showTime, now),
    _useTimeInfo2 = _slicedToArray(_useTimeInfo, 1),
    getValidTime = _useTimeInfo2[0];

  // ======================== Extra =========================
  var extraNode = renderExtraFooter === null || renderExtraFooter === void 0 ? void 0 : renderExtraFooter(mode);

  // ======================== Ranges ========================
  var nowDisabled = disabledDate(now, {
    type: mode
  });
  var onInternalNow = function onInternalNow() {
    if (!nowDisabled) {
      var validateNow = getValidTime(now);
      onNow(validateNow);
    }
  };
  var nowPrefixCls = "".concat(prefixCls, "-now");
  var nowBtnPrefixCls = "".concat(nowPrefixCls, "-btn");
  var presetNode = showNow && /*#__PURE__*/React.createElement("li", {
    className: nowPrefixCls
  }, /*#__PURE__*/React.createElement("a", {
    className: classNames(nowBtnPrefixCls, nowDisabled && "".concat(nowBtnPrefixCls, "-disabled")),
    "aria-disabled": nowDisabled,
    onClick: onInternalNow
  }, internalMode === 'date' ? locale.today : locale.now));

  // >>> OK
  var okNode = needConfirm && /*#__PURE__*/React.createElement("li", {
    className: "".concat(prefixCls, "-ok")
  }, /*#__PURE__*/React.createElement(Button, {
    disabled: invalid,
    onClick: onSubmit
  }, locale.ok));
  var rangeNode = (presetNode || okNode) && /*#__PURE__*/React.createElement("ul", {
    className: "".concat(prefixCls, "-ranges")
  }, presetNode, okNode);

  // ======================== Render ========================
  if (!extraNode && !rangeNode) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-footer")
  }, extraNode && /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-footer-extra")
  }, extraNode), rangeNode);
}