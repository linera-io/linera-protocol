import _extends from "@babel/runtime/helpers/esm/extends";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["visible", "onVisibleChange", "getContainer", "current", "movable", "minScale", "maxScale", "countRender", "closeIcon", "onChange", "onTransform", "toolbarRender", "imageRender"],
  _excluded2 = ["src"];
import useMergedState from "rc-util/es/hooks/useMergedState";
import * as React from 'react';
import { useState } from 'react';
import { PreviewGroupContext } from "./context";
import usePreviewItems from "./hooks/usePreviewItems";
import Preview from "./Preview";
var Group = function Group(_ref) {
  var _mergedItems$current;
  var _ref$previewPrefixCls = _ref.previewPrefixCls,
    previewPrefixCls = _ref$previewPrefixCls === void 0 ? 'rc-image-preview' : _ref$previewPrefixCls,
    children = _ref.children,
    _ref$icons = _ref.icons,
    icons = _ref$icons === void 0 ? {} : _ref$icons,
    items = _ref.items,
    preview = _ref.preview,
    fallback = _ref.fallback;
  var _ref2 = _typeof(preview) === 'object' ? preview : {},
    previewVisible = _ref2.visible,
    onVisibleChange = _ref2.onVisibleChange,
    getContainer = _ref2.getContainer,
    currentIndex = _ref2.current,
    movable = _ref2.movable,
    minScale = _ref2.minScale,
    maxScale = _ref2.maxScale,
    countRender = _ref2.countRender,
    closeIcon = _ref2.closeIcon,
    onChange = _ref2.onChange,
    onTransform = _ref2.onTransform,
    toolbarRender = _ref2.toolbarRender,
    imageRender = _ref2.imageRender,
    dialogProps = _objectWithoutProperties(_ref2, _excluded);

  // ========================== Items ===========================
  var _usePreviewItems = usePreviewItems(items),
    _usePreviewItems2 = _slicedToArray(_usePreviewItems, 2),
    mergedItems = _usePreviewItems2[0],
    register = _usePreviewItems2[1];

  // ========================= Preview ==========================
  // >>> Index
  var _useMergedState = useMergedState(0, {
      value: currentIndex
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    current = _useMergedState2[0],
    setCurrent = _useMergedState2[1];
  var _useState = useState(false),
    _useState2 = _slicedToArray(_useState, 2),
    keepOpenIndex = _useState2[0],
    setKeepOpenIndex = _useState2[1];

  // >>> Image
  var _ref3 = ((_mergedItems$current = mergedItems[current]) === null || _mergedItems$current === void 0 ? void 0 : _mergedItems$current.data) || {},
    src = _ref3.src,
    imgCommonProps = _objectWithoutProperties(_ref3, _excluded2);
  // >>> Visible
  var _useMergedState3 = useMergedState(!!previewVisible, {
      value: previewVisible,
      onChange: function onChange(val, prevVal) {
        onVisibleChange === null || onVisibleChange === void 0 || onVisibleChange(val, prevVal, current);
      }
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    isShowPreview = _useMergedState4[0],
    setShowPreview = _useMergedState4[1];

  // >>> Position
  var _useState3 = useState(null),
    _useState4 = _slicedToArray(_useState3, 2),
    mousePosition = _useState4[0],
    setMousePosition = _useState4[1];
  var onPreviewFromImage = React.useCallback(function (id, mouseX, mouseY) {
    var index = mergedItems.findIndex(function (item) {
      return item.id === id;
    });
    setShowPreview(true);
    setMousePosition({
      x: mouseX,
      y: mouseY
    });
    setCurrent(index < 0 ? 0 : index);
    setKeepOpenIndex(true);
  }, [mergedItems]);

  // Reset current when reopen
  React.useEffect(function () {
    if (isShowPreview) {
      if (!keepOpenIndex) {
        setCurrent(0);
      }
    } else {
      setKeepOpenIndex(false);
    }
  }, [isShowPreview]);

  // ========================== Events ==========================
  var onInternalChange = function onInternalChange(next, prev) {
    setCurrent(next);
    onChange === null || onChange === void 0 || onChange(next, prev);
  };
  var onPreviewClose = function onPreviewClose() {
    setShowPreview(false);
    setMousePosition(null);
  };

  // ========================= Context ==========================
  var previewGroupContext = React.useMemo(function () {
    return {
      register: register,
      onPreview: onPreviewFromImage
    };
  }, [register, onPreviewFromImage]);

  // ========================== Render ==========================
  return /*#__PURE__*/React.createElement(PreviewGroupContext.Provider, {
    value: previewGroupContext
  }, children, /*#__PURE__*/React.createElement(Preview, _extends({
    "aria-hidden": !isShowPreview,
    movable: movable,
    visible: isShowPreview,
    prefixCls: previewPrefixCls,
    closeIcon: closeIcon,
    onClose: onPreviewClose,
    mousePosition: mousePosition,
    imgCommonProps: imgCommonProps,
    src: src,
    fallback: fallback,
    icons: icons,
    minScale: minScale,
    maxScale: maxScale,
    getContainer: getContainer,
    current: current,
    count: mergedItems.length,
    countRender: countRender,
    onTransform: onTransform,
    toolbarRender: toolbarRender,
    imageRender: imageRender,
    onChange: onInternalChange
  }, dialogProps)));
};
export default Group;