"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = InlineSubMenuList;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var _motionUtil = require("../utils/motionUtil");
var _MenuContext = _interopRequireWildcard(require("../context/MenuContext"));
var _SubMenuList = _interopRequireDefault(require("./SubMenuList"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function InlineSubMenuList(_ref) {
  var id = _ref.id,
    open = _ref.open,
    keyPath = _ref.keyPath,
    children = _ref.children;
  var fixedMode = 'inline';
  var _React$useContext = React.useContext(_MenuContext.MenuContext),
    prefixCls = _React$useContext.prefixCls,
    forceSubMenuRender = _React$useContext.forceSubMenuRender,
    motion = _React$useContext.motion,
    defaultMotions = _React$useContext.defaultMotions,
    mode = _React$useContext.mode;

  // Always use latest mode check
  var sameModeRef = React.useRef(false);
  sameModeRef.current = mode === fixedMode;

  // We record `destroy` mark here since when mode change from `inline` to others.
  // The inline list should remove when motion end.
  var _React$useState = React.useState(!sameModeRef.current),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    destroy = _React$useState2[0],
    setDestroy = _React$useState2[1];
  var mergedOpen = sameModeRef.current ? open : false;

  // ================================= Effect =================================
  // Reset destroy state when mode change back
  React.useEffect(function () {
    if (sameModeRef.current) {
      setDestroy(false);
    }
  }, [mode]);

  // ================================= Render =================================
  var mergedMotion = (0, _objectSpread2.default)({}, (0, _motionUtil.getMotion)(fixedMode, motion, defaultMotions));

  // No need appear since nest inlineCollapse changed
  if (keyPath.length > 1) {
    mergedMotion.motionAppear = false;
  }

  // Hide inline list when mode changed and motion end
  var originOnVisibleChanged = mergedMotion.onVisibleChanged;
  mergedMotion.onVisibleChanged = function (newVisible) {
    if (!sameModeRef.current && !newVisible) {
      setDestroy(true);
    }
    return originOnVisibleChanged === null || originOnVisibleChanged === void 0 ? void 0 : originOnVisibleChanged(newVisible);
  };
  if (destroy) {
    return null;
  }
  return /*#__PURE__*/React.createElement(_MenuContext.default, {
    mode: fixedMode,
    locked: !sameModeRef.current
  }, /*#__PURE__*/React.createElement(_rcMotion.default, (0, _extends2.default)({
    visible: mergedOpen
  }, mergedMotion, {
    forceRender: forceSubMenuRender,
    removeOnLeave: false,
    leavedClassName: "".concat(prefixCls, "-hidden")
  }), function (_ref2) {
    var motionClassName = _ref2.className,
      motionStyle = _ref2.style;
    return /*#__PURE__*/React.createElement(_SubMenuList.default, {
      id: id,
      className: motionClassName,
      style: motionStyle
    }, children);
  }));
}