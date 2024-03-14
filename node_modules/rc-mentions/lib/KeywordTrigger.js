"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _trigger = _interopRequireDefault(require("@rc-component/trigger"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _DropdownMenu = _interopRequireDefault(require("./DropdownMenu"));
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
  var dropdownElement = /*#__PURE__*/React.createElement(_DropdownMenu.default, {
    prefixCls: dropdownPrefix,
    options: options
  });
  var dropdownPlacement = (0, _react.useMemo)(function () {
    var popupPlacement;
    if (direction === 'rtl') {
      popupPlacement = placement === 'top' ? 'topLeft' : 'bottomLeft';
    } else {
      popupPlacement = placement === 'top' ? 'topRight' : 'bottomRight';
    }
    return popupPlacement;
  }, [direction, placement]);
  return /*#__PURE__*/React.createElement(_trigger.default, {
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
var _default = exports.default = KeywordTrigger;