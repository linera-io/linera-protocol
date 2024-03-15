import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _extends from "@babel/runtime/helpers/esm/extends";
import * as React from 'react';
import classNames from 'classnames';
import pickAttrs from "rc-util/es/pickAttrs";
export default function DefaultPanel(props) {
  var _closable$closeIcon;
  var prefixCls = props.prefixCls,
    current = props.current,
    total = props.total,
    title = props.title,
    description = props.description,
    onClose = props.onClose,
    onPrev = props.onPrev,
    onNext = props.onNext,
    onFinish = props.onFinish,
    className = props.className,
    closable = props.closable;
  var ariaProps = pickAttrs(closable || {}, true);
  var closeIcon = (_closable$closeIcon = closable === null || closable === void 0 ? void 0 : closable.closeIcon) !== null && _closable$closeIcon !== void 0 ? _closable$closeIcon : /*#__PURE__*/React.createElement("span", {
    className: "".concat(prefixCls, "-close-x")
  }, "\xD7");
  var mergedClosable = !!closable;
  return /*#__PURE__*/React.createElement("div", {
    className: classNames("".concat(prefixCls, "-content"), className)
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-inner")
  }, mergedClosable && /*#__PURE__*/React.createElement("button", _extends({
    type: "button",
    onClick: onClose,
    "aria-label": "Close"
  }, ariaProps, {
    className: "".concat(prefixCls, "-close")
  }), closeIcon), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-header")
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-title")
  }, title)), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-description")
  }, description), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-footer")
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-sliders")
  }, total > 1 ? _toConsumableArray(Array.from({
    length: total
  }).keys()).map(function (item, index) {
    return /*#__PURE__*/React.createElement("span", {
      key: item,
      className: index === current ? 'active' : ''
    });
  }) : null), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-buttons")
  }, current !== 0 ? /*#__PURE__*/React.createElement("button", {
    className: "".concat(prefixCls, "-prev-btn"),
    onClick: onPrev
  }, "Prev") : null, current === total - 1 ? /*#__PURE__*/React.createElement("button", {
    className: "".concat(prefixCls, "-finish-btn"),
    onClick: onFinish
  }, "Finish") : /*#__PURE__*/React.createElement("button", {
    className: "".concat(prefixCls, "-next-btn"),
    onClick: onNext
  }, "Next")))));
}