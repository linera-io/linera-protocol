"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var _react = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var useIndicator = function useIndicator(options) {
  var activeTabOffset = options.activeTabOffset,
    horizontal = options.horizontal,
    rtl = options.rtl,
    _options$indicator = options.indicator,
    indicator = _options$indicator === void 0 ? {} : _options$indicator;
  var size = indicator.size,
    _indicator$align = indicator.align,
    align = _indicator$align === void 0 ? 'center' : _indicator$align;
  var _useState = (0, _react.useState)(),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    inkStyle = _useState2[0],
    setInkStyle = _useState2[1];
  var inkBarRafRef = (0, _react.useRef)();
  var getLength = _react.default.useCallback(function (origin) {
    if (typeof size === 'function') {
      return size(origin);
    }
    if (typeof size === 'number') {
      return size;
    }
    return origin;
  }, [size]);

  // Delay set ink style to avoid remove tab blink
  function cleanInkBarRaf() {
    _raf.default.cancel(inkBarRafRef.current);
  }
  (0, _react.useEffect)(function () {
    var newInkStyle = {};
    if (activeTabOffset) {
      if (horizontal) {
        newInkStyle.width = getLength(activeTabOffset.width);
        var key = rtl ? 'right' : 'left';
        if (align === 'start') {
          newInkStyle[key] = activeTabOffset[key];
        }
        if (align === 'center') {
          newInkStyle[key] = activeTabOffset[key] + activeTabOffset.width / 2;
          newInkStyle.transform = rtl ? 'translateX(50%)' : 'translateX(-50%)';
        }
        if (align === 'end') {
          newInkStyle[key] = activeTabOffset[key] + activeTabOffset.width;
          newInkStyle.transform = 'translateX(-100%)';
        }
      } else {
        newInkStyle.height = getLength(activeTabOffset.height);
        if (align === 'start') {
          newInkStyle.top = activeTabOffset.top;
        }
        if (align === 'center') {
          newInkStyle.top = activeTabOffset.top + activeTabOffset.height / 2;
          newInkStyle.transform = 'translateY(-50%)';
        }
        if (align === 'end') {
          newInkStyle.top = activeTabOffset.top + activeTabOffset.height;
          newInkStyle.transform = 'translateY(-100%)';
        }
      }
    }
    cleanInkBarRaf();
    inkBarRafRef.current = (0, _raf.default)(function () {
      setInkStyle(newInkStyle);
    });
    return cleanInkBarRaf;
  }, [activeTabOffset, horizontal, rtl, align, getLength]);
  return {
    style: inkStyle
  };
};
var _default = exports.default = useIndicator;