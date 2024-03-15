"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Tracks;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _util = require("../util");
var _Track = _interopRequireDefault(require("./Track"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Tracks(props) {
  var prefixCls = props.prefixCls,
    style = props.style,
    values = props.values,
    startPoint = props.startPoint,
    onStartMove = props.onStartMove;
  var _React$useContext = React.useContext(_context.default),
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
    tracksNode = /*#__PURE__*/React.createElement(_Track.default, {
      index: null,
      prefixCls: prefixCls,
      start: trackList[0].start,
      end: trackList[trackList.length - 1].end,
      replaceCls: (0, _classnames.default)(classNames.tracks, "".concat(prefixCls, "-tracks")),
      style: styles.tracks
    });
  }
  return included ? /*#__PURE__*/React.createElement(React.Fragment, null, tracksNode, trackList.map(function (_ref, index) {
    var start = _ref.start,
      end = _ref.end;
    return /*#__PURE__*/React.createElement(_Track.default, {
      index: index,
      prefixCls: prefixCls,
      style: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, (0, _util.getIndex)(style, index)), styles.track),
      start: start,
      end: end,
      key: index,
      onStartMove: onStartMove
    });
  })) : null;
}