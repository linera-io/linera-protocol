import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import Portal from '@rc-component/portal';
import classnames from 'classnames';
import CSSMotion from 'rc-motion';
import KeyCode from "rc-util/es/KeyCode";
import * as React from 'react';
import { useContext } from 'react';
import { PreviewGroupContext } from "./context";
var Operations = function Operations(props) {
  var visible = props.visible,
    maskTransitionName = props.maskTransitionName,
    getContainer = props.getContainer,
    prefixCls = props.prefixCls,
    rootClassName = props.rootClassName,
    icons = props.icons,
    countRender = props.countRender,
    showSwitch = props.showSwitch,
    showProgress = props.showProgress,
    current = props.current,
    transform = props.transform,
    count = props.count,
    scale = props.scale,
    minScale = props.minScale,
    maxScale = props.maxScale,
    closeIcon = props.closeIcon,
    onSwitchLeft = props.onSwitchLeft,
    onSwitchRight = props.onSwitchRight,
    onClose = props.onClose,
    onZoomIn = props.onZoomIn,
    onZoomOut = props.onZoomOut,
    onRotateRight = props.onRotateRight,
    onRotateLeft = props.onRotateLeft,
    onFlipX = props.onFlipX,
    onFlipY = props.onFlipY,
    toolbarRender = props.toolbarRender,
    zIndex = props.zIndex;
  var groupContext = useContext(PreviewGroupContext);
  var rotateLeft = icons.rotateLeft,
    rotateRight = icons.rotateRight,
    zoomIn = icons.zoomIn,
    zoomOut = icons.zoomOut,
    close = icons.close,
    left = icons.left,
    right = icons.right,
    flipX = icons.flipX,
    flipY = icons.flipY;
  var toolClassName = "".concat(prefixCls, "-operations-operation");
  React.useEffect(function () {
    var onKeyDown = function onKeyDown(e) {
      if (e.keyCode === KeyCode.ESC) {
        onClose();
      }
    };
    if (visible) {
      window.addEventListener('keydown', onKeyDown);
    }
    return function () {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [visible]);
  var tools = [{
    icon: flipY,
    onClick: onFlipY,
    type: 'flipY'
  }, {
    icon: flipX,
    onClick: onFlipX,
    type: 'flipX'
  }, {
    icon: rotateLeft,
    onClick: onRotateLeft,
    type: 'rotateLeft'
  }, {
    icon: rotateRight,
    onClick: onRotateRight,
    type: 'rotateRight'
  }, {
    icon: zoomOut,
    onClick: onZoomOut,
    type: 'zoomOut',
    disabled: scale <= minScale
  }, {
    icon: zoomIn,
    onClick: onZoomIn,
    type: 'zoomIn',
    disabled: scale === maxScale
  }];
  var toolsNode = tools.map(function (_ref) {
    var _classnames;
    var icon = _ref.icon,
      onClick = _ref.onClick,
      type = _ref.type,
      disabled = _ref.disabled;
    return /*#__PURE__*/React.createElement("div", {
      className: classnames(toolClassName, (_classnames = {}, _defineProperty(_classnames, "".concat(prefixCls, "-operations-operation-").concat(type), true), _defineProperty(_classnames, "".concat(prefixCls, "-operations-operation-disabled"), !!disabled), _classnames)),
      onClick: onClick,
      key: type
    }, icon);
  });
  var toolbarNode = /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-operations")
  }, toolsNode);
  return /*#__PURE__*/React.createElement(CSSMotion, {
    visible: visible,
    motionName: maskTransitionName
  }, function (_ref2) {
    var className = _ref2.className,
      style = _ref2.style;
    return /*#__PURE__*/React.createElement(Portal, {
      open: true,
      getContainer: getContainer !== null && getContainer !== void 0 ? getContainer : document.body
    }, /*#__PURE__*/React.createElement("div", {
      className: classnames("".concat(prefixCls, "-operations-wrapper"), className, rootClassName),
      style: _objectSpread(_objectSpread({}, style), {}, {
        zIndex: zIndex
      })
    }, closeIcon === null ? null : /*#__PURE__*/React.createElement("button", {
      className: "".concat(prefixCls, "-close"),
      onClick: onClose
    }, closeIcon || close), showSwitch && /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("div", {
      className: classnames("".concat(prefixCls, "-switch-left"), _defineProperty({}, "".concat(prefixCls, "-switch-left-disabled"), current === 0)),
      onClick: onSwitchLeft
    }, left), /*#__PURE__*/React.createElement("div", {
      className: classnames("".concat(prefixCls, "-switch-right"), _defineProperty({}, "".concat(prefixCls, "-switch-right-disabled"), current === count - 1)),
      onClick: onSwitchRight
    }, right)), /*#__PURE__*/React.createElement("div", {
      className: "".concat(prefixCls, "-footer")
    }, showProgress && /*#__PURE__*/React.createElement("div", {
      className: "".concat(prefixCls, "-progress")
    }, countRender ? countRender(current + 1, count) : "".concat(current + 1, " / ").concat(count)), toolbarRender ? toolbarRender(toolbarNode, _objectSpread({
      icons: {
        flipYIcon: toolsNode[0],
        flipXIcon: toolsNode[1],
        rotateLeftIcon: toolsNode[2],
        rotateRightIcon: toolsNode[3],
        zoomOutIcon: toolsNode[4],
        zoomInIcon: toolsNode[5]
      },
      actions: {
        onFlipY: onFlipY,
        onFlipX: onFlipX,
        onRotateLeft: onRotateLeft,
        onRotateRight: onRotateRight,
        onZoomOut: onZoomOut,
        onZoomIn: onZoomIn
      },
      transform: transform
    }, groupContext ? {
      current: current,
      total: count
    } : {})) : toolbarNode)));
  });
};
export default Operations;