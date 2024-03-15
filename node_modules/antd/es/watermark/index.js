"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import React, { useEffect } from 'react';
import { useMutateObserver } from '@rc-component/mutate-observer';
import classNames from 'classnames';
import { useToken } from '../theme/internal';
import WatermarkContext from './context';
import useClips, { FontGap } from './useClips';
import useRafDebounce from './useRafDebounce';
import useWatermark from './useWatermark';
import { getPixelRatio, reRendering } from './utils';
/**
 * Only return `next` when size changed.
 * This is only used for elements compare, not a shallow equal!
 */
function getSizeDiff(prev, next) {
  return prev.size === next.size ? prev : next;
}
const DEFAULT_GAP_X = 100;
const DEFAULT_GAP_Y = 100;
const Watermark = props => {
  var _a, _b;
  const {
    /**
     * The antd content layer zIndex is basically below 10
     * https://github.com/ant-design/ant-design/blob/6192403b2ce517c017f9e58a32d58774921c10cd/components/style/themes/default.less#L335
     */
    zIndex = 9,
    rotate = -22,
    width,
    height,
    image,
    content,
    font = {},
    style,
    className,
    rootClassName,
    gap = [DEFAULT_GAP_X, DEFAULT_GAP_Y],
    offset,
    children,
    inherit = true
  } = props;
  const [, token] = useToken();
  const {
    color = token.colorFill,
    fontSize = token.fontSizeLG,
    fontWeight = 'normal',
    fontStyle = 'normal',
    fontFamily = 'sans-serif',
    textAlign = 'center'
  } = font;
  const [gapX = DEFAULT_GAP_X, gapY = DEFAULT_GAP_Y] = gap;
  const gapXCenter = gapX / 2;
  const gapYCenter = gapY / 2;
  const offsetLeft = (_a = offset === null || offset === void 0 ? void 0 : offset[0]) !== null && _a !== void 0 ? _a : gapXCenter;
  const offsetTop = (_b = offset === null || offset === void 0 ? void 0 : offset[1]) !== null && _b !== void 0 ? _b : gapYCenter;
  const markStyle = React.useMemo(() => {
    const mergedStyle = {
      zIndex,
      position: 'absolute',
      left: 0,
      top: 0,
      width: '100%',
      height: '100%',
      pointerEvents: 'none',
      backgroundRepeat: 'repeat'
    };
    /** Calculate the style of the offset */
    let positionLeft = offsetLeft - gapXCenter;
    let positionTop = offsetTop - gapYCenter;
    if (positionLeft > 0) {
      mergedStyle.left = `${positionLeft}px`;
      mergedStyle.width = `calc(100% - ${positionLeft}px)`;
      positionLeft = 0;
    }
    if (positionTop > 0) {
      mergedStyle.top = `${positionTop}px`;
      mergedStyle.height = `calc(100% - ${positionTop}px)`;
      positionTop = 0;
    }
    mergedStyle.backgroundPosition = `${positionLeft}px ${positionTop}px`;
    return mergedStyle;
  }, [zIndex, offsetLeft, gapXCenter, offsetTop, gapYCenter]);
  const [container, setContainer] = React.useState();
  // Used for nest case like Modal, Drawer
  const [subElements, setSubElements] = React.useState(new Set());
  // Nest elements should also support watermark
  const targetElements = React.useMemo(() => {
    const list = container ? [container] : [];
    return [].concat(list, _toConsumableArray(Array.from(subElements)));
  }, [container, subElements]);
  // ============================ Content =============================
  /**
   * Get the width and height of the watermark. The default values are as follows
   * Image: [120, 64]; Content: It's calculated by content;
   */
  const getMarkSize = ctx => {
    let defaultWidth = 120;
    let defaultHeight = 64;
    if (!image && ctx.measureText) {
      ctx.font = `${Number(fontSize)}px ${fontFamily}`;
      const contents = Array.isArray(content) ? content : [content];
      const sizes = contents.map(item => {
        const metrics = ctx.measureText(item);
        return [metrics.width, metrics.fontBoundingBoxAscent + metrics.fontBoundingBoxDescent];
      });
      defaultWidth = Math.ceil(Math.max.apply(Math, _toConsumableArray(sizes.map(size => size[0]))));
      defaultHeight = Math.ceil(Math.max.apply(Math, _toConsumableArray(sizes.map(size => size[1])))) * contents.length + (contents.length - 1) * FontGap;
    }
    return [width !== null && width !== void 0 ? width : defaultWidth, height !== null && height !== void 0 ? height : defaultHeight];
  };
  const getClips = useClips();
  const [watermarkInfo, setWatermarkInfo] = React.useState(null);
  // Generate new Watermark content
  const renderWatermark = () => {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (ctx) {
      const ratio = getPixelRatio();
      const [markWidth, markHeight] = getMarkSize(ctx);
      const drawCanvas = drawContent => {
        const [nextClips, clipWidth] = getClips(drawContent || '', rotate, ratio, markWidth, markHeight, {
          color,
          fontSize,
          fontStyle,
          fontWeight,
          fontFamily,
          textAlign
        }, gapX, gapY);
        setWatermarkInfo([nextClips, clipWidth]);
      };
      if (image) {
        const img = new Image();
        img.onload = () => {
          drawCanvas(img);
        };
        img.onerror = () => {
          drawCanvas(content);
        };
        img.crossOrigin = 'anonymous';
        img.referrerPolicy = 'no-referrer';
        img.src = image;
      } else {
        drawCanvas(content);
      }
    }
  };
  const syncWatermark = useRafDebounce(renderWatermark);
  // ============================= Effect =============================
  // Append watermark to the container
  const [appendWatermark, removeWatermark, isWatermarkEle] = useWatermark(markStyle);
  useEffect(() => {
    if (watermarkInfo) {
      targetElements.forEach(holder => {
        appendWatermark(watermarkInfo[0], watermarkInfo[1], holder);
      });
    }
  }, [watermarkInfo, targetElements]);
  // ============================ Observe =============================
  const onMutate = mutations => {
    mutations.forEach(mutation => {
      if (reRendering(mutation, isWatermarkEle)) {
        syncWatermark();
      }
    });
  };
  useMutateObserver(targetElements, onMutate);
  useEffect(syncWatermark, [rotate, zIndex, width, height, image, content, color, fontSize, fontWeight, fontStyle, fontFamily, textAlign, gapX, gapY, offsetLeft, offsetTop]);
  // ============================ Context =============================
  const watermarkContext = React.useMemo(() => ({
    add: ele => {
      setSubElements(prev => {
        const clone = new Set(prev);
        clone.add(ele);
        return getSizeDiff(prev, clone);
      });
    },
    remove: ele => {
      removeWatermark(ele);
      setSubElements(prev => {
        const clone = new Set(prev);
        clone.delete(ele);
        return getSizeDiff(prev, clone);
      });
    }
  }), []);
  // ============================= Render =============================
  const childNode = inherit ? ( /*#__PURE__*/React.createElement(WatermarkContext.Provider, {
    value: watermarkContext
  }, children)) : children;
  return /*#__PURE__*/React.createElement("div", {
    ref: setContainer,
    className: classNames(className, rootClassName),
    style: Object.assign({
      position: 'relative'
    }, style)
  }, childNode);
};
if (process.env.NODE_ENV !== 'production') {
  Watermark.displayName = 'Watermark';
}
export default Watermark;