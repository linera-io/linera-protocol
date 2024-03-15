"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _useMergedState5 = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _context = require("./context");
var _usePreviewItems3 = _interopRequireDefault(require("./hooks/usePreviewItems"));
var _Preview = _interopRequireDefault(require("./Preview"));
var _excluded = ["visible", "onVisibleChange", "getContainer", "current", "movable", "minScale", "maxScale", "countRender", "closeIcon", "onChange", "onTransform", "toolbarRender", "imageRender"],
  _excluded2 = ["src"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
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
  var _ref2 = (0, _typeof2.default)(preview) === 'object' ? preview : {},
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
    dialogProps = (0, _objectWithoutProperties2.default)(_ref2, _excluded);

  // ========================== Items ===========================
  var _usePreviewItems = (0, _usePreviewItems3.default)(items),
    _usePreviewItems2 = (0, _slicedToArray2.default)(_usePreviewItems, 2),
    mergedItems = _usePreviewItems2[0],
    register = _usePreviewItems2[1];

  // ========================= Preview ==========================
  // >>> Index
  var _useMergedState = (0, _useMergedState5.default)(0, {
      value: currentIndex
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    current = _useMergedState2[0],
    setCurrent = _useMergedState2[1];
  var _useState = (0, _react.useState)(false),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    keepOpenIndex = _useState2[0],
    setKeepOpenIndex = _useState2[1];

  // >>> Image
  var _ref3 = ((_mergedItems$current = mergedItems[current]) === null || _mergedItems$current === void 0 ? void 0 : _mergedItems$current.data) || {},
    src = _ref3.src,
    imgCommonProps = (0, _objectWithoutProperties2.default)(_ref3, _excluded2);
  // >>> Visible
  var _useMergedState3 = (0, _useMergedState5.default)(!!previewVisible, {
      value: previewVisible,
      onChange: function onChange(val, prevVal) {
        onVisibleChange === null || onVisibleChange === void 0 || onVisibleChange(val, prevVal, current);
      }
    }),
    _useMergedState4 = (0, _slicedToArray2.default)(_useMergedState3, 2),
    isShowPreview = _useMergedState4[0],
    setShowPreview = _useMergedState4[1];

  // >>> Position
  var _useState3 = (0, _react.useState)(null),
    _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
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
  return /*#__PURE__*/React.createElement(_context.PreviewGroupContext.Provider, {
    value: previewGroupContext
  }, children, /*#__PURE__*/React.createElement(_Preview.default, (0, _extends2.default)({
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
var _default = exports.default = Group;