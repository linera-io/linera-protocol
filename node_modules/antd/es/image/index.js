"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import EyeOutlined from "@ant-design/icons/es/icons/EyeOutlined";
import classNames from 'classnames';
import RcImage from 'rc-image';
import { useZIndex } from '../_util/hooks/useZIndex';
import { getTransitionName } from '../_util/motion';
import { ConfigContext } from '../config-provider';
import useCSSVarCls from '../config-provider/hooks/useCSSVarCls';
import defaultLocale from '../locale/en_US';
import PreviewGroup, { icons } from './PreviewGroup';
import useStyle from './style';
const Image = props => {
  var _a;
  const {
      prefixCls: customizePrefixCls,
      preview,
      className,
      rootClassName,
      style
    } = props,
    otherProps = __rest(props, ["prefixCls", "preview", "className", "rootClassName", "style"]);
  const {
    getPrefixCls,
    locale: contextLocale = defaultLocale,
    getPopupContainer: getContextPopupContainer,
    image
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('image', customizePrefixCls);
  const rootPrefixCls = getPrefixCls();
  const imageLocale = contextLocale.Image || defaultLocale.Image;
  // Style
  const rootCls = useCSSVarCls(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls, rootCls);
  const mergedRootClassName = classNames(rootClassName, hashId, cssVarCls, rootCls);
  const mergedClassName = classNames(className, hashId, image === null || image === void 0 ? void 0 : image.className);
  const [zIndex] = useZIndex('ImagePreview', typeof preview === 'object' ? preview.zIndex : undefined);
  const mergedPreview = React.useMemo(() => {
    var _a;
    if (preview === false) {
      return preview;
    }
    const _preview = typeof preview === 'object' ? preview : {};
    const {
        getContainer,
        closeIcon
      } = _preview,
      restPreviewProps = __rest(_preview, ["getContainer", "closeIcon"]);
    return Object.assign(Object.assign({
      mask: ( /*#__PURE__*/React.createElement("div", {
        className: `${prefixCls}-mask-info`
      }, /*#__PURE__*/React.createElement(EyeOutlined, null), imageLocale === null || imageLocale === void 0 ? void 0 : imageLocale.preview)),
      icons
    }, restPreviewProps), {
      getContainer: getContainer !== null && getContainer !== void 0 ? getContainer : getContextPopupContainer,
      transitionName: getTransitionName(rootPrefixCls, 'zoom', _preview.transitionName),
      maskTransitionName: getTransitionName(rootPrefixCls, 'fade', _preview.maskTransitionName),
      zIndex,
      closeIcon: closeIcon !== null && closeIcon !== void 0 ? closeIcon : (_a = image === null || image === void 0 ? void 0 : image.preview) === null || _a === void 0 ? void 0 : _a.closeIcon
    });
  }, [preview, imageLocale, (_a = image === null || image === void 0 ? void 0 : image.preview) === null || _a === void 0 ? void 0 : _a.closeIcon]);
  const mergedStyle = Object.assign(Object.assign({}, image === null || image === void 0 ? void 0 : image.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement(RcImage, Object.assign({
    prefixCls: prefixCls,
    preview: mergedPreview,
    rootClassName: mergedRootClassName,
    className: mergedClassName,
    style: mergedStyle
  }, otherProps)));
};
Image.PreviewGroup = PreviewGroup;
if (process.env.NODE_ENV !== 'production') {
  Image.displayName = 'Image';
}
export default Image;