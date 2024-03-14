import Trigger from '@rc-component/trigger';
import * as React from 'react';
import { useMemo } from 'react';
import DropdownMenu from "./DropdownMenu";
var BUILT_IN_PLACEMENTS = {
  bottomRight: {
    points: ['tl', 'br'],
    offset: [0, 4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  },
  bottomLeft: {
    points: ['tr', 'bl'],
    offset: [0, 4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  },
  topRight: {
    points: ['bl', 'tr'],
    offset: [0, -4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  },
  topLeft: {
    points: ['br', 'tl'],
    offset: [0, -4],
    overflow: {
      adjustX: 1,
      adjustY: 1
    }
  }
};
var KeywordTrigger = function KeywordTrigger(props) {
  var prefixCls = props.prefixCls,
    options = props.options,
    children = props.children,
    visible = props.visible,
    transitionName = props.transitionName,
    getPopupContainer = props.getPopupContainer,
    dropdownClassName = props.dropdownClassName,
    direction = props.direction,
    placement = props.placement;
  var dropdownPrefix = "".concat(prefixCls, "-dropdown");
  var dropdownElement = /*#__PURE__*/React.createElement(DropdownMenu, {
    prefixCls: dropdownPrefix,
    options: options
  });
  var dropdownPlacement = useMemo(function () {
    var popupPlacement;
    if (direction === 'rtl') {
      popupPlacement = placement === 'top' ? 'topLeft' : 'bottomLeft';
    } else {
      popupPlacement = placement === 'top' ? 'topRight' : 'bottomRight';
    }
    return popupPlacement;
  }, [direction, placement]);
  return /*#__PURE__*/React.createElement(Trigger, {
    prefixCls: dropdownPrefix,
    popupVisible: visible,
    popup: dropdownElement,
    popupPlacement: dropdownPlacement,
    popupTransitionName: transitionName,
    builtinPlacements: BUILT_IN_PLACEMENTS,
    getPopupContainer: getPopupContainer,
    popupClassName: dropdownClassName
  }, children);
};
export default KeywordTrigger;