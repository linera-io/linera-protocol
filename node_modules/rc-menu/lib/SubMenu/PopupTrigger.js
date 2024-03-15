"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = PopupTrigger;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _trigger = _interopRequireDefault(require("@rc-component/trigger"));
var _classnames = _interopRequireDefault(require("classnames"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var _MenuContext = require("../context/MenuContext");
var _placements = require("../placements");
var _motionUtil = require("../utils/motionUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var popupPlacementMap = {
  horizontal: 'bottomLeft',
  vertical: 'rightTop',
  'vertical-left': 'rightTop',
  'vertical-right': 'leftTop'
};
function PopupTrigger(_ref) {
  var prefixCls = _ref.prefixCls,
    visible = _ref.visible,
    children = _ref.children,
    popup = _ref.popup,
    popupStyle = _ref.popupStyle,
    popupClassName = _ref.popupClassName,
    popupOffset = _ref.popupOffset,
    disabled = _ref.disabled,
    mode = _ref.mode,
    onVisibleChange = _ref.onVisibleChange;
  var _React$useContext = React.useContext(_MenuContext.MenuContext),
    getPopupContainer = _React$useContext.getPopupContainer,
    rtl = _React$useContext.rtl,
    subMenuOpenDelay = _React$useContext.subMenuOpenDelay,
    subMenuCloseDelay = _React$useContext.subMenuCloseDelay,
    builtinPlacements = _React$useContext.builtinPlacements,
    triggerSubMenuAction = _React$useContext.triggerSubMenuAction,
    forceSubMenuRender = _React$useContext.forceSubMenuRender,
    rootClassName = _React$useContext.rootClassName,
    motion = _React$useContext.motion,
    defaultMotions = _React$useContext.defaultMotions;
  var _React$useState = React.useState(false),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    innerVisible = _React$useState2[0],
    setInnerVisible = _React$useState2[1];
  var placement = rtl ? (0, _objectSpread2.default)((0, _objectSpread2.default)({}, _placements.placementsRtl), builtinPlacements) : (0, _objectSpread2.default)((0, _objectSpread2.default)({}, _placements.placements), builtinPlacements);
  var popupPlacement = popupPlacementMap[mode];
  var targetMotion = (0, _motionUtil.getMotion)(mode, motion, defaultMotions);
  var targetMotionRef = React.useRef(targetMotion);
  if (mode !== 'inline') {
    /**
     * PopupTrigger is only used for vertical and horizontal types.
     * When collapsed is unfolded, the inline animation will destroy the vertical animation.
     */
    targetMotionRef.current = targetMotion;
  }
  var mergedMotion = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, targetMotionRef.current), {}, {
    leavedClassName: "".concat(prefixCls, "-hidden"),
    removeOnLeave: false,
    motionAppear: true
  });

  // Delay to change visible
  var visibleRef = React.useRef();
  React.useEffect(function () {
    visibleRef.current = (0, _raf.default)(function () {
      setInnerVisible(visible);
    });
    return function () {
      _raf.default.cancel(visibleRef.current);
    };
  }, [visible]);
  return /*#__PURE__*/React.createElement(_trigger.default, {
    prefixCls: prefixCls,
    popupClassName: (0, _classnames.default)("".concat(prefixCls, "-popup"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-rtl"), rtl), popupClassName, rootClassName),
    stretch: mode === 'horizontal' ? 'minWidth' : null,
    getPopupContainer: getPopupContainer,
    builtinPlacements: placement,
    popupPlacement: popupPlacement,
    popupVisible: innerVisible,
    popup: popup,
    popupStyle: popupStyle,
    popupAlign: popupOffset && {
      offset: popupOffset
    },
    action: disabled ? [] : [triggerSubMenuAction],
    mouseEnterDelay: subMenuOpenDelay,
    mouseLeaveDelay: subMenuCloseDelay,
    onPopupVisibleChange: onVisibleChange,
    forceRender: forceSubMenuRender,
    popupMotion: mergedMotion,
    fresh: true
  }, children);
}