"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.icons = exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _CloseOutlined = _interopRequireDefault(require("@ant-design/icons/CloseOutlined"));
var _LeftOutlined = _interopRequireDefault(require("@ant-design/icons/LeftOutlined"));
var _RightOutlined = _interopRequireDefault(require("@ant-design/icons/RightOutlined"));
var _RotateLeftOutlined = _interopRequireDefault(require("@ant-design/icons/RotateLeftOutlined"));
var _RotateRightOutlined = _interopRequireDefault(require("@ant-design/icons/RotateRightOutlined"));
var _SwapOutlined = _interopRequireDefault(require("@ant-design/icons/SwapOutlined"));
var _ZoomInOutlined = _interopRequireDefault(require("@ant-design/icons/ZoomInOutlined"));
var _ZoomOutOutlined = _interopRequireDefault(require("@ant-design/icons/ZoomOutOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcImage = _interopRequireDefault(require("rc-image"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _motion = require("../_util/motion");
var _configProvider = require("../config-provider");
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const icons = exports.icons = {
  rotateLeft: /*#__PURE__*/React.createElement(_RotateLeftOutlined.default, null),
  rotateRight: /*#__PURE__*/React.createElement(_RotateRightOutlined.default, null),
  zoomIn: /*#__PURE__*/React.createElement(_ZoomInOutlined.default, null),
  zoomOut: /*#__PURE__*/React.createElement(_ZoomOutOutlined.default, null),
  close: /*#__PURE__*/React.createElement(_CloseOutlined.default, null),
  left: /*#__PURE__*/React.createElement(_LeftOutlined.default, null),
  right: /*#__PURE__*/React.createElement(_RightOutlined.default, null),
  flipX: /*#__PURE__*/React.createElement(_SwapOutlined.default, null),
  flipY: /*#__PURE__*/React.createElement(_SwapOutlined.default, {
    rotate: 90
  })
};
const InternalPreviewGroup = _a => {
  var {
      previewPrefixCls: customizePrefixCls,
      preview
    } = _a,
    otherProps = __rest(_a, ["previewPrefixCls", "preview"]);
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('image', customizePrefixCls);
  const previewPrefixCls = `${prefixCls}-preview`;
  const rootPrefixCls = getPrefixCls();
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const [zIndex] = (0, _useZIndex.useZIndex)('ImagePreview', typeof preview === 'object' ? preview.zIndex : undefined);
  const mergedPreview = React.useMemo(() => {
    var _a;
    if (preview === false) {
      return preview;
    }
    const _preview = typeof preview === 'object' ? preview : {};
    const mergedRootClassName = (0, _classnames.default)(hashId, cssVarCls, rootCls, (_a = _preview.rootClassName) !== null && _a !== void 0 ? _a : '');
    return Object.assign(Object.assign({}, _preview), {
      transitionName: (0, _motion.getTransitionName)(rootPrefixCls, 'zoom', _preview.transitionName),
      maskTransitionName: (0, _motion.getTransitionName)(rootPrefixCls, 'fade', _preview.maskTransitionName),
      rootClassName: mergedRootClassName,
      zIndex
    });
  }, [preview]);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcImage.default.PreviewGroup, Object.assign({
    preview: mergedPreview,
    previewPrefixCls: previewPrefixCls,
    icons: icons
  }, otherProps)));
};
var _default = exports.default = InternalPreviewGroup;