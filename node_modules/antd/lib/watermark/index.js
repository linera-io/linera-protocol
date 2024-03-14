"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _react = _interopRequireWildcard(require("react"));
var _mutateObserver = require("@rc-component/mutate-observer");
var _classnames = _interopRequireDefault(require("classnames"));
var _internal = require("../theme/internal");
var _context = _interopRequireDefault(require("./context"));
var _useClips = _interopRequireWildcard(require("./useClips"));
var _useRafDebounce = _interopRequireDefault(require("./useRafDebounce"));
var _useWatermark = _interopRequireDefault(require("./useWatermark"));
var _utils = require("./utils");
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
  const [, token] = (0, _internal.useToken)();
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
  const markStyle = _react.default.useMemo(() => {
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
  const [container, setContainer] = _react.default.useState();
  // Used for nest case like Modal, Drawer
  const [subElements, setSubElements] = _react.default.useState(new Set());
  // Nest elements should also support watermark
  const targetElements = _react.default.useMemo(() => {
    const list = container ? [container] : [];
    return [].concat(list, (0, _toConsumableArray2.default)(Array.from(subElements)));
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
      defaultWidth = Math.ceil(Math.max.apply(Math, (0, _toConsumableArray2.default)(sizes.map(size => size[0]))));
      defaultHeight = Math.ceil(Math.max.apply(Math, (0, _toConsumableArray2.default)(sizes.map(size => size[1])))) * contents.length + (contents.length - 1) * _useClips.FontGap;
    }
    return [width !== null && width !== void 0 ? width : defaultWidth, height !== null && height !== void 0 ? height : defaultHeight];
  };
  const getClips = (0, _useClips.default)();
  const [watermarkInfo, setWatermarkInfo] = _react.default.useState(null);
  // Generate new Watermark content
  const renderWatermark = () => {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (ctx) {
      const ratio = (0, _utils.getPixelRatio)();
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
  const syncWatermark = (0, _useRafDebounce.default)(renderWatermark);
  // ============================= Effect =============================
  // Append watermark to the container
  const [appendWatermark, removeWatermark, isWatermarkEle] = (0, _useWatermark.default)(markStyle);
  (0, _react.useEffect)(() => {
    if (watermarkInfo) {
      targetElements.forEach(holder => {
        appendWatermark(watermarkInfo[0], watermarkInfo[1], holder);
      });
    }
  }, [watermarkInfo, targetElements]);
  // ============================ Observe =============================
  const onMutate = mutations => {
    mutations.forEach(mutation => {
      if ((0, _utils.reRendering)(mutation, isWatermarkEle)) {
        syncWatermark();
      }
    });
  };
  (0, _mutateObserver.useMutateObserver)(targetElements, onMutate);
  (0, _react.useEffect)(syncWatermark, [rotate, zIndex, width, height, image, content, color, fontSize, fontWeight, fontStyle, fontFamily, textAlign, gapX, gapY, offsetLeft, offsetTop]);
  // ============================ Context =============================
  const watermarkContext = _react.default.useMemo(() => ({
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
  const childNode = inherit ? ( /*#__PURE__*/_react.default.createElement(_context.default.Provider, {
    value: watermarkContext
  }, children)) : children;
  return /*#__PURE__*/_react.default.createElement("div", {
    ref: setContainer,
    className: (0, _classnames.default)(className, rootClassName),
    style: Object.assign({
      position: 'relative'
    }, style)
  }, childNode);
};
if (process.env.NODE_ENV !== 'production') {
  Watermark.displayName = 'Watermark';
}
var _default = exports.default = Watermark;