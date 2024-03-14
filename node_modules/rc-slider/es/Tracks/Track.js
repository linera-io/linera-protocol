import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import cls from 'classnames';
import * as React from 'react';
import SliderContext from "../context";
import { getOffset } from "../util";
export default function Track(props) {
  var _cls;
  var prefixCls = props.prefixCls,
    style = props.style,
    start = props.start,
    end = props.end,
    index = props.index,
    onStartMove = props.onStartMove,
    replaceCls = props.replaceCls;
  var _React$useContext = React.useContext(SliderContext),
    direction = _React$useContext.direction,
    min = _React$useContext.min,
    max = _React$useContext.max,
    disabled = _React$useContext.disabled,
    range = _React$useContext.range,
    classNames = _React$useContext.classNames;
  var trackPrefixCls = "".concat(prefixCls, "-track");
  var offsetStart = getOffset(start, min, max);
  var offsetEnd = getOffset(end, min, max);

  // ============================ Events ============================
  var onInternalStartMove = function onInternalStartMove(e) {
    if (!disabled && onStartMove) {
      onStartMove(e, -1);
    }
  };

  // ============================ Render ============================
  var positionStyle = {};
  switch (direction) {
    case 'rtl':
      positionStyle.right = "".concat(offsetStart * 100, "%");
      positionStyle.width = "".concat(offsetEnd * 100 - offsetStart * 100, "%");
      break;
    case 'btt':
      positionStyle.bottom = "".concat(offsetStart * 100, "%");
      positionStyle.height = "".concat(offsetEnd * 100 - offsetStart * 100, "%");
      break;
    case 'ttb':
      positionStyle.top = "".concat(offsetStart * 100, "%");
      positionStyle.height = "".concat(offsetEnd * 100 - offsetStart * 100, "%");
      break;
    default:
      positionStyle.left = "".concat(offsetStart * 100, "%");
      positionStyle.width = "".concat(offsetEnd * 100 - offsetStart * 100, "%");
  }
  var className = replaceCls || cls(trackPrefixCls, (_cls = {}, _defineProperty(_cls, "".concat(trackPrefixCls, "-").concat(index + 1), index !== null && range), _defineProperty(_cls, "".concat(prefixCls, "-track-draggable"), onStartMove), _cls), classNames.track);
  return /*#__PURE__*/React.createElement("div", {
    className: className,
    style: _objectSpread(_objectSpread({}, positionStyle), style),
    onMouseDown: onInternalStartMove,
    onTouchStart: onInternalStartMove
  });
}