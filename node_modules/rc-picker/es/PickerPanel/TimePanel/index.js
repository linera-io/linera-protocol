import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import * as React from 'react';
import { formatValue } from "../../utils/dateUtil";
import { PanelContext, useInfo } from "../context";
import PanelHeader from "../PanelHeader";
import TimePanelBody from "./TimePanelBody";
export default function TimePanel(props) {
  var prefixCls = props.prefixCls,
    value = props.value,
    locale = props.locale,
    generateConfig = props.generateConfig,
    showTime = props.showTime;
  var _ref = showTime || {},
    format = _ref.format;
  var panelPrefixCls = "".concat(prefixCls, "-time-panel");

  // ========================== Base ==========================
  var _useInfo = useInfo(props, 'time'),
    _useInfo2 = _slicedToArray(_useInfo, 1),
    info = _useInfo2[0];

  // ========================= Render =========================
  return /*#__PURE__*/React.createElement(PanelContext.Provider, {
    value: info
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames(panelPrefixCls)
  }, /*#__PURE__*/React.createElement(PanelHeader, null, value ? formatValue(value, {
    locale: locale,
    format: format,
    generateConfig: generateConfig
  }) : "\xA0"), /*#__PURE__*/React.createElement(TimePanelBody, showTime)));
}