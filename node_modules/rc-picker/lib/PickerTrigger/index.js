"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _trigger = _interopRequireDefault(require("@rc-component/trigger"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../PickerInput/context"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var BUILT_IN_PLACEMENTS = {
  bottomLeft: {
    points: ['tl', 'bl'],
    offset: [0, 4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  },
  bottomRight: {
    points: ['tr', 'br'],
    offset: [0, 4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  },
  topLeft: {
    points: ['bl', 'tl'],
    offset: [0, -4],
    overflow: {
      adjustX: 0,
      adjustY: 1
    }
  },
  topRight: {
    points: ['br', 'tr'],
    offset: [0, -4],
    overflow: {
      adjustX: 0,
      adjustY: 1
    }
  }
};
function PickerTrigger(_ref) {
  var popupElement = _ref.popupElement,
    popupStyle = _ref.popupStyle,
    popupClassName = _ref.popupClassName,
    popupAlign = _ref.popupAlign,
    transitionName = _ref.transitionName,
    getPopupContainer = _ref.getPopupContainer,
    children = _ref.children,
    range = _ref.range,
    placement = _ref.placement,
    _ref$builtinPlacement = _ref.builtinPlacements,
    builtinPlacements = _ref$builtinPlacement === void 0 ? BUILT_IN_PLACEMENTS : _ref$builtinPlacement,
    direction = _ref.direction,
    visible = _ref.visible,
    onClose = _ref.onClose;
  var _React$useContext = React.useContext(_context.default),
    prefixCls = _React$useContext.prefixCls;
  var dropdownPrefixCls = "".concat(prefixCls, "-dropdown");
  var mergedPlacement = React.useMemo(function () {
    if (placement !== undefined) {
      return placement;
    }
    return direction === 'rtl' ? 'bottomRight' : 'bottomLeft';
  }, [placement, direction]);
  return /*#__PURE__*/React.createElement(_trigger.default, {
    showAction: [],
    hideAction: ['click'],
    popupPlacement: mergedPlacement,
    builtinPlacements: builtinPlacements,
    prefixCls: dropdownPrefixCls,
    popupTransitionName: transitionName,
    popup: popupElement,
    popupAlign: popupAlign,
    popupVisible: visible,
    popupClassName: (0, _classnames.default)(popupClassName, (0, _defineProperty2.default)((0, _defineProperty2.default)({}, "".concat(dropdownPrefixCls, "-range"), range), "".concat(dropdownPrefixCls, "-rtl"), direction === 'rtl')),
    popupStyle: popupStyle,
    stretch: "minWidth",
    getPopupContainer: getPopupContainer,
    onPopupVisibleChange: function onPopupVisibleChange(nextVisible) {
      if (!nextVisible) {
        onClose();
      }
    }
  }, children);
}
var _default = exports.default = PickerTrigger;