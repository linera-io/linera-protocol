import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import Trigger from '@rc-component/trigger';
import classNames from 'classnames';
import * as React from 'react';
import PickerContext from "../PickerInput/context";
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
  var _React$useContext = React.useContext(PickerContext),
    prefixCls = _React$useContext.prefixCls;
  var dropdownPrefixCls = "".concat(prefixCls, "-dropdown");
  var mergedPlacement = React.useMemo(function () {
    if (placement !== undefined) {
      return placement;
    }
    return direction === 'rtl' ? 'bottomRight' : 'bottomLeft';
  }, [placement, direction]);
  return /*#__PURE__*/React.createElement(Trigger, {
    showAction: [],
    hideAction: ['click'],
    popupPlacement: mergedPlacement,
    builtinPlacements: builtinPlacements,
    prefixCls: dropdownPrefixCls,
    popupTransitionName: transitionName,
    popup: popupElement,
    popupAlign: popupAlign,
    popupVisible: visible,
    popupClassName: classNames(popupClassName, _defineProperty(_defineProperty({}, "".concat(dropdownPrefixCls, "-range"), range), "".concat(dropdownPrefixCls, "-rtl"), direction === 'rtl')),
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
export default PickerTrigger;