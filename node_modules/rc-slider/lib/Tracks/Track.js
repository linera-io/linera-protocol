"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = Track;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _util = require("../util");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function Track(props) {
  var _cls;
  var prefixCls = props.prefixCls,
    style = props.style,
    start = props.start,
    end = props.end,
    index = props.index,
    onStartMove = props.onStartMove,
    replaceCls = props.replaceCls;
  var _React$useContext = React.useContext(_context.default),
    direction = _React$useContext.direction,
    min = _React$useContext.min,
    max = _React$useContext.max,
    disabled = _React$useContext.disabled,
    range = _React$useContext.range,
    classNames = _React$useContext.classNames;
  var trackPrefixCls = "".concat(prefixCls, "-track");
  var offsetStart = (0, _util.getOffset)(start, min, max);
  var offsetEnd = (0, _util.getOffset)(end, min, max);

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
  var className = replaceCls || (0, _classnames.default)(trackPrefixCls, (_cls = {}, (0, _defineProperty2.default)(_cls, "".concat(trackPrefixCls, "-").concat(index + 1), index !== null && range), (0, _defineProperty2.default)(_cls, "".concat(prefixCls, "-track-draggable"), onStartMove), _cls), classNames.track);
  return /*#__PURE__*/React.createElement("div", {
    className: className,
    style: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, positionStyle), style),
    onMouseDown: onInternalStartMove,
    onTouchStart: onInternalStartMove
  });
}