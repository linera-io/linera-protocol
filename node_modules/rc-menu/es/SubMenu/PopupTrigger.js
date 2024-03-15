import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import Trigger from '@rc-component/trigger';
import classNames from 'classnames';
import raf from "rc-util/es/raf";
import { MenuContext } from "../context/MenuContext";
import { placements, placementsRtl } from "../placements";
import { getMotion } from "../utils/motionUtil";
var popupPlacementMap = {
  horizontal: 'bottomLeft',
  vertical: 'rightTop',
  'vertical-left': 'rightTop',
  'vertical-right': 'leftTop'
};
export default function PopupTrigger(_ref) {
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
  var _React$useContext = React.useContext(MenuContext),
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
    _React$useState2 = _slicedToArray(_React$useState, 2),
    innerVisible = _React$useState2[0],
    setInnerVisible = _React$useState2[1];
  var placement = rtl ? _objectSpread(_objectSpread({}, placementsRtl), builtinPlacements) : _objectSpread(_objectSpread({}, placements), builtinPlacements);
  var popupPlacement = popupPlacementMap[mode];
  var targetMotion = getMotion(mode, motion, defaultMotions);
  var targetMotionRef = React.useRef(targetMotion);
  if (mode !== 'inline') {
    /**
     * PopupTrigger is only used for vertical and horizontal types.
     * When collapsed is unfolded, the inline animation will destroy the vertical animation.
     */
    targetMotionRef.current = targetMotion;
  }
  var mergedMotion = _objectSpread(_objectSpread({}, targetMotionRef.current), {}, {
    leavedClassName: "".concat(prefixCls, "-hidden"),
    removeOnLeave: false,
    motionAppear: true
  });

  // Delay to change visible
  var visibleRef = React.useRef();
  React.useEffect(function () {
    visibleRef.current = raf(function () {
      setInnerVisible(visible);
    });
    return function () {
      raf.cancel(visibleRef.current);
    };
  }, [visible]);
  return /*#__PURE__*/React.createElement(Trigger, {
    prefixCls: prefixCls,
    popupClassName: classNames("".concat(prefixCls, "-popup"), _defineProperty({}, "".concat(prefixCls, "-rtl"), rtl), popupClassName, rootClassName),
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