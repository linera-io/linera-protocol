import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _extends from "@babel/runtime/helpers/esm/extends";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import ResizeObserver from 'rc-resize-observer';
import * as React from 'react';
import { toArray } from "../../utils/miscUtil";
import PickerContext from "../context";
import Footer from "./Footer";
import PopupPanel from "./PopupPanel";
import PresetPanel from "./PresetPanel";
export default function Popup(props) {
  var panelRender = props.panelRender,
    internalMode = props.internalMode,
    picker = props.picker,
    showNow = props.showNow,
    range = props.range,
    multiple = props.multiple,
    _props$activeOffset = props.activeOffset,
    activeOffset = _props$activeOffset === void 0 ? 0 : _props$activeOffset,
    presets = props.presets,
    onPresetHover = props.onPresetHover,
    onPresetSubmit = props.onPresetSubmit,
    onFocus = props.onFocus,
    onBlur = props.onBlur,
    direction = props.direction,
    value = props.value,
    onSelect = props.onSelect,
    isInvalid = props.isInvalid,
    defaultOpenValue = props.defaultOpenValue,
    onOk = props.onOk,
    onSubmit = props.onSubmit;
  var _React$useContext = React.useContext(PickerContext),
    prefixCls = _React$useContext.prefixCls;
  var panelPrefixCls = "".concat(prefixCls, "-panel");
  var rtl = direction === 'rtl';

  // ========================= Refs =========================
  var arrowRef = React.useRef(null);
  var wrapperRef = React.useRef(null);

  // ======================== Offset ========================
  var _React$useState = React.useState(0),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    containerWidth = _React$useState2[0],
    setContainerWidth = _React$useState2[1];
  var _React$useState3 = React.useState(0),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    containerOffset = _React$useState4[0],
    setContainerOffset = _React$useState4[1];
  var onResize = function onResize(info) {
    if (info.offsetWidth) {
      setContainerWidth(info.offsetWidth);
    }
  };
  React.useEffect(function () {
    // `activeOffset` is always align with the active input element
    // So we need only check container contains the `activeOffset`
    if (range) {
      var _arrowRef$current;
      // Offset in case container has border radius
      var arrowWidth = ((_arrowRef$current = arrowRef.current) === null || _arrowRef$current === void 0 ? void 0 : _arrowRef$current.offsetWidth) || 0;
      var maxOffset = containerWidth - arrowWidth;
      if (activeOffset <= maxOffset) {
        setContainerOffset(0);
      } else {
        setContainerOffset(activeOffset + arrowWidth - containerWidth);
      }
    }
  }, [containerWidth, activeOffset, range]);

  // ======================== Custom ========================
  function filterEmpty(list) {
    return list.filter(function (item) {
      return item;
    });
  }
  var valueList = React.useMemo(function () {
    return filterEmpty(toArray(value));
  }, [value]);
  var isTimePickerEmptyValue = picker === 'time' && !valueList.length;
  var footerSubmitValue = React.useMemo(function () {
    if (isTimePickerEmptyValue) {
      return filterEmpty([defaultOpenValue]);
    }
    return valueList;
  }, [isTimePickerEmptyValue, valueList, defaultOpenValue]);
  var popupPanelValue = isTimePickerEmptyValue ? defaultOpenValue : valueList;
  var disableSubmit = React.useMemo(function () {
    // Empty is invalid
    if (!footerSubmitValue.length) {
      return true;
    }
    return footerSubmitValue.some(function (val) {
      return isInvalid(val);
    });
  }, [footerSubmitValue, isInvalid]);
  var onFooterSubmit = function onFooterSubmit() {
    // For TimePicker, we will additional trigger the value update
    if (isTimePickerEmptyValue) {
      onSelect(defaultOpenValue);
    }
    onOk();
    onSubmit();
  };
  var mergedNodes = /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-panel-layout")
  }, /*#__PURE__*/React.createElement(PresetPanel, {
    prefixCls: prefixCls,
    presets: presets,
    onClick: onPresetSubmit,
    onHover: onPresetHover
  }), /*#__PURE__*/React.createElement("div", null, /*#__PURE__*/React.createElement(PopupPanel, _extends({}, props, {
    value: popupPanelValue
  })), /*#__PURE__*/React.createElement(Footer, _extends({}, props, {
    showNow: multiple ? false : showNow,
    invalid: disableSubmit,
    onSubmit: onFooterSubmit
  }))));
  if (panelRender) {
    mergedNodes = panelRender(mergedNodes);
  }

  // ======================== Render ========================
  var containerPrefixCls = "".concat(panelPrefixCls, "-container");
  var marginLeft = 'marginLeft';
  var marginRight = 'marginRight';

  // Container
  var renderNode = /*#__PURE__*/React.createElement("div", {
    tabIndex: -1,
    className: classNames(containerPrefixCls, // Used for Today Button style, safe to remove if no need
    "".concat(prefixCls, "-").concat(internalMode, "-panel-container")),
    style: _defineProperty(_defineProperty({}, rtl ? marginRight : marginLeft, containerOffset), rtl ? marginLeft : marginRight, 'auto')
    // Still wish not to lose focus on mouse down
    // onMouseDown={(e) => {
    //   // e.preventDefault();
    // }}
    ,
    onFocus: onFocus,
    onBlur: onBlur
  }, mergedNodes);
  if (range) {
    renderNode = /*#__PURE__*/React.createElement("div", {
      ref: wrapperRef,
      className: classNames("".concat(prefixCls, "-range-wrapper"), "".concat(prefixCls, "-").concat(picker, "-range-wrapper"))
    }, /*#__PURE__*/React.createElement("div", {
      ref: arrowRef,
      className: "".concat(prefixCls, "-range-arrow"),
      style: _defineProperty({}, rtl ? 'right' : 'left', activeOffset)
    }), /*#__PURE__*/React.createElement(ResizeObserver, {
      onResize: onResize
    }, renderNode));
  }
  return renderNode;
}