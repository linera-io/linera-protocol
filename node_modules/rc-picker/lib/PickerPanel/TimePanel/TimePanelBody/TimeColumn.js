"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = TimeColumn;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var React = _interopRequireWildcard(require("react"));
var _context = require("../../context");
var _useScrollTo3 = _interopRequireDefault(require("./useScrollTo"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var SCROLL_DELAY = 300;
function TimeColumn(props) {
  var units = props.units,
    value = props.value,
    optionalValue = props.optionalValue,
    type = props.type,
    onChange = props.onChange,
    onDblClick = props.onDblClick,
    changeOnScroll = props.changeOnScroll;
  var _usePanelContext = (0, _context.usePanelContext)(),
    prefixCls = _usePanelContext.prefixCls,
    cellRender = _usePanelContext.cellRender,
    now = _usePanelContext.now,
    locale = _usePanelContext.locale;
  var panelPrefixCls = "".concat(prefixCls, "-time-panel");
  var cellPrefixCls = "".concat(prefixCls, "-time-panel-cell");

  // ========================== Refs ==========================
  var ulRef = React.useRef(null);

  // ========================= Scroll =========================
  var checkDelayRef = React.useRef();
  var clearDelayCheck = function clearDelayCheck() {
    clearTimeout(checkDelayRef.current);
  };

  // ========================== Sync ==========================
  var _useScrollTo = (0, _useScrollTo3.default)(ulRef, value !== null && value !== void 0 ? value : optionalValue),
    _useScrollTo2 = (0, _slicedToArray2.default)(_useScrollTo, 3),
    syncScroll = _useScrollTo2[0],
    stopScroll = _useScrollTo2[1],
    isScrolling = _useScrollTo2[2];

  // Effect sync value scroll
  (0, _useLayoutEffect.default)(function () {
    syncScroll();
    clearDelayCheck();
    return function () {
      stopScroll();
      clearDelayCheck();
    };
  }, [value, optionalValue, units]);

  // ========================= Change =========================
  // Scroll event if sync onScroll
  var onInternalScroll = function onInternalScroll(event) {
    clearDelayCheck();
    var target = event.target;
    if (!isScrolling() && changeOnScroll) {
      checkDelayRef.current = setTimeout(function () {
        var ul = ulRef.current;
        var firstLiTop = ul.querySelector("li").offsetTop;
        var liList = Array.from(ul.querySelectorAll("li"));
        var liTopList = liList.map(function (li) {
          return li.offsetTop - firstLiTop;
        });
        var liDistList = liTopList.map(function (top, index) {
          if (units[index].disabled) {
            return Number.MAX_SAFE_INTEGER;
          }
          return Math.abs(top - target.scrollTop);
        });

        // Find min distance index
        var minDist = Math.min.apply(Math, (0, _toConsumableArray2.default)(liDistList));
        var minDistIndex = liDistList.findIndex(function (dist) {
          return dist === minDist;
        });
        var targetUnit = units[minDistIndex];
        if (targetUnit && !targetUnit.disabled) {
          onChange(targetUnit.value);
        }
      }, SCROLL_DELAY);
    }
  };

  // ========================= Render =========================
  var columnPrefixCls = "".concat(panelPrefixCls, "-column");
  return /*#__PURE__*/React.createElement("ul", {
    className: columnPrefixCls,
    ref: ulRef,
    "data-type": type,
    onScroll: onInternalScroll
  }, units.map(function (_ref) {
    var label = _ref.label,
      unitValue = _ref.value,
      disabled = _ref.disabled;
    var inner = /*#__PURE__*/React.createElement("div", {
      className: "".concat(cellPrefixCls, "-inner")
    }, label);
    return /*#__PURE__*/React.createElement("li", {
      key: unitValue,
      className: (0, _classnames.default)(cellPrefixCls, (0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(cellPrefixCls, "-selected"), value === unitValue), "".concat(cellPrefixCls, "-disabled"), disabled)),
      onClick: function onClick() {
        if (!disabled) {
          onChange(unitValue);
        }
      },
      onDoubleClick: function onDoubleClick() {
        if (!disabled && onDblClick) {
          onDblClick();
        }
      },
      "data-value": unitValue
    }, cellRender ? cellRender(unitValue, {
      prefixCls: prefixCls,
      originNode: inner,
      today: now,
      type: 'time',
      subType: type,
      locale: locale
    }) : inner);
  }));
}