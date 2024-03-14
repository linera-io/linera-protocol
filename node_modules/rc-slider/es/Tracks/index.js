import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import cls from 'classnames';
import * as React from 'react';
import SliderContext from "../context";
import { getIndex } from "../util";
import Track from "./Track";
export default function Tracks(props) {
  var prefixCls = props.prefixCls,
    style = props.style,
    values = props.values,
    startPoint = props.startPoint,
    onStartMove = props.onStartMove;
  var _React$useContext = React.useContext(SliderContext),
    included = _React$useContext.included,
    range = _React$useContext.range,
    min = _React$useContext.min,
    styles = _React$useContext.styles,
    classNames = _React$useContext.classNames;

  // =========================== List ===========================
  var trackList = React.useMemo(function () {
    if (!range) {
      // null value do not have track
      if (values.length === 0) {
        return [];
      }
      var startValue = startPoint !== null && startPoint !== void 0 ? startPoint : min;
      var endValue = values[0];
      return [{
        start: Math.min(startValue, endValue),
        end: Math.max(startValue, endValue)
      }];
    }

    // Multiple
    var list = [];
    for (var i = 0; i < values.length - 1; i += 1) {
      list.push({
        start: values[i],
        end: values[i + 1]
      });
    }
    return list;
  }, [values, range, startPoint, min]);

  // ========================== Render ==========================
  var tracksNode = null;
  if (classNames.tracks || styles.tracks) {
    tracksNode = /*#__PURE__*/React.createElement(Track, {
      index: null,
      prefixCls: prefixCls,
      start: trackList[0].start,
      end: trackList[trackList.length - 1].end,
      replaceCls: cls(classNames.tracks, "".concat(prefixCls, "-tracks")),
      style: styles.tracks
    });
  }
  return included ? /*#__PURE__*/React.createElement(React.Fragment, null, tracksNode, trackList.map(function (_ref, index) {
    var start = _ref.start,
      end = _ref.end;
    return /*#__PURE__*/React.createElement(Track, {
      index: index,
      prefixCls: prefixCls,
      style: _objectSpread(_objectSpread({}, getIndex(style, index)), styles.track),
      start: start,
      end: end,
      key: index,
      onStartMove: onStartMove
    });
  })) : null;
}