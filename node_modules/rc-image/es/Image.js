import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["src", "alt", "onPreviewClose", "prefixCls", "previewPrefixCls", "placeholder", "fallback", "width", "height", "style", "preview", "className", "onClick", "onError", "wrapperClassName", "wrapperStyle", "rootClassName"],
  _excluded2 = ["src", "visible", "onVisibleChange", "getContainer", "mask", "maskClassName", "movable", "icons", "scaleStep", "minScale", "maxScale", "imageRender", "toolbarRender"];
import cn from 'classnames';
import { getOffset } from "rc-util/es/Dom/css";
import useMergedState from "rc-util/es/hooks/useMergedState";
import * as React from 'react';
import { useContext, useMemo, useState } from 'react';
import { COMMON_PROPS } from "./common";
import { PreviewGroupContext } from "./context";
import useRegisterImage from "./hooks/useRegisterImage";
import useStatus from "./hooks/useStatus";
import Preview from "./Preview";
import PreviewGroup from "./PreviewGroup";
var ImageInternal = function ImageInternal(props) {
  var imgSrc = props.src,
    alt = props.alt,
    onInitialPreviewClose = props.onPreviewClose,
    _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-image' : _props$prefixCls,
    _props$previewPrefixC = props.previewPrefixCls,
    previewPrefixCls = _props$previewPrefixC === void 0 ? "".concat(prefixCls, "-preview") : _props$previewPrefixC,
    placeholder = props.placeholder,
    fallback = props.fallback,
    width = props.width,
    height = props.height,
    style = props.style,
    _props$preview = props.preview,
    preview = _props$preview === void 0 ? true : _props$preview,
    className = props.className,
    onClick = props.onClick,
    onError = props.onError,
    wrapperClassName = props.wrapperClassName,
    wrapperStyle = props.wrapperStyle,
    rootClassName = props.rootClassName,
    otherProps = _objectWithoutProperties(props, _excluded);
  var isCustomPlaceholder = placeholder && placeholder !== true;
  var _ref = _typeof(preview) === 'object' ? preview : {},
    previewSrc = _ref.src,
    _ref$visible = _ref.visible,
    previewVisible = _ref$visible === void 0 ? undefined : _ref$visible,
    _ref$onVisibleChange = _ref.onVisibleChange,
    onPreviewVisibleChange = _ref$onVisibleChange === void 0 ? onInitialPreviewClose : _ref$onVisibleChange,
    _ref$getContainer = _ref.getContainer,
    getPreviewContainer = _ref$getContainer === void 0 ? undefined : _ref$getContainer,
    previewMask = _ref.mask,
    maskClassName = _ref.maskClassName,
    movable = _ref.movable,
    icons = _ref.icons,
    scaleStep = _ref.scaleStep,
    minScale = _ref.minScale,
    maxScale = _ref.maxScale,
    imageRender = _ref.imageRender,
    toolbarRender = _ref.toolbarRender,
    dialogProps = _objectWithoutProperties(_ref, _excluded2);
  var src = previewSrc !== null && previewSrc !== void 0 ? previewSrc : imgSrc;
  var _useMergedState = useMergedState(!!previewVisible, {
      value: previewVisible,
      onChange: onPreviewVisibleChange
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    isShowPreview = _useMergedState2[0],
    setShowPreview = _useMergedState2[1];
  var _useStatus = useStatus({
      src: imgSrc,
      isCustomPlaceholder: isCustomPlaceholder,
      fallback: fallback
    }),
    _useStatus2 = _slicedToArray(_useStatus, 3),
    getImgRef = _useStatus2[0],
    srcAndOnload = _useStatus2[1],
    status = _useStatus2[2];
  var _useState = useState(null),
    _useState2 = _slicedToArray(_useState, 2),
    mousePosition = _useState2[0],
    setMousePosition = _useState2[1];
  var groupContext = useContext(PreviewGroupContext);
  var canPreview = !!preview;
  var onPreviewClose = function onPreviewClose() {
    setShowPreview(false);
    setMousePosition(null);
  };
  var wrapperClass = cn(prefixCls, wrapperClassName, rootClassName, _defineProperty({}, "".concat(prefixCls, "-error"), status === 'error'));

  // ========================= ImageProps =========================
  var imgCommonProps = useMemo(function () {
    var obj = {};
    COMMON_PROPS.forEach(function (prop) {
      if (props[prop] !== undefined) {
        obj[prop] = props[prop];
      }
    });
    return obj;
  }, COMMON_PROPS.map(function (prop) {
    return props[prop];
  }));

  // ========================== Register ==========================
  var registerData = useMemo(function () {
    return _objectSpread(_objectSpread({}, imgCommonProps), {}, {
      src: src
    });
  }, [src, imgCommonProps]);
  var imageId = useRegisterImage(canPreview, registerData);

  // ========================== Preview ===========================
  var onPreview = function onPreview(e) {
    var _getOffset = getOffset(e.target),
      left = _getOffset.left,
      top = _getOffset.top;
    if (groupContext) {
      groupContext.onPreview(imageId, left, top);
    } else {
      setMousePosition({
        x: left,
        y: top
      });
      setShowPreview(true);
    }
    onClick === null || onClick === void 0 || onClick(e);
  };

  // =========================== Render ===========================
  return /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("div", _extends({}, otherProps, {
    className: wrapperClass,
    onClick: canPreview ? onPreview : onClick,
    style: _objectSpread({
      width: width,
      height: height
    }, wrapperStyle)
  }), /*#__PURE__*/React.createElement("img", _extends({}, imgCommonProps, {
    className: cn("".concat(prefixCls, "-img"), _defineProperty({}, "".concat(prefixCls, "-img-placeholder"), placeholder === true), className),
    style: _objectSpread({
      height: height
    }, style),
    ref: getImgRef
  }, srcAndOnload, {
    width: width,
    height: height,
    onError: onError
  })), status === 'loading' && /*#__PURE__*/React.createElement("div", {
    "aria-hidden": "true",
    className: "".concat(prefixCls, "-placeholder")
  }, placeholder), previewMask && canPreview && /*#__PURE__*/React.createElement("div", {
    className: cn("".concat(prefixCls, "-mask"), maskClassName),
    style: {
      display: (style === null || style === void 0 ? void 0 : style.display) === 'none' ? 'none' : undefined
    }
  }, previewMask)), !groupContext && canPreview && /*#__PURE__*/React.createElement(Preview, _extends({
    "aria-hidden": !isShowPreview,
    visible: isShowPreview,
    prefixCls: previewPrefixCls,
    onClose: onPreviewClose,
    mousePosition: mousePosition,
    src: src,
    alt: alt,
    fallback: fallback,
    getContainer: getPreviewContainer,
    icons: icons,
    movable: movable,
    scaleStep: scaleStep,
    minScale: minScale,
    maxScale: maxScale,
    rootClassName: rootClassName,
    imageRender: imageRender,
    imgCommonProps: imgCommonProps,
    toolbarRender: toolbarRender
  }, dialogProps)));
};
ImageInternal.PreviewGroup = PreviewGroup;
ImageInternal.displayName = 'Image';
export default ImageInternal;