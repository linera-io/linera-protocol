"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _reactSlick = _interopRequireDefault(require("@ant-design/react-slick"));
var _classnames = _interopRequireDefault(require("classnames"));
var _configProvider = require("../config-provider");
var _style = _interopRequireDefault(require("./style"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const Carousel = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      dots = true,
      arrows = false,
      draggable = false,
      waitForAnimate = false,
      dotPosition = 'bottom',
      vertical = dotPosition === 'left' || dotPosition === 'right',
      rootClassName,
      className: customClassName,
      style,
      id
    } = props,
    otherProps = __rest(props, ["dots", "arrows", "draggable", "waitForAnimate", "dotPosition", "vertical", "rootClassName", "className", "style", "id"]);
  const {
    getPrefixCls,
    direction,
    carousel
  } = React.useContext(_configProvider.ConfigContext);
  const slickRef = React.useRef();
  const goTo = function (slide) {
    let dontAnimate = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
    slickRef.current.slickGoTo(slide, dontAnimate);
  };
  React.useImperativeHandle(ref, () => ({
    goTo,
    autoPlay: slickRef.current.innerSlider.autoPlay,
    innerSlider: slickRef.current.innerSlider,
    prev: slickRef.current.slickPrev,
    next: slickRef.current.slickNext
  }), [slickRef.current]);
  const prevCount = React.useRef(React.Children.count(props.children));
  React.useEffect(() => {
    if (prevCount.current !== React.Children.count(props.children)) {
      goTo(props.initialSlide || 0, false);
      prevCount.current = React.Children.count(props.children);
    }
  }, [props.children]);
  const newProps = Object.assign({
    vertical,
    className: (0, _classnames.default)(customClassName, carousel === null || carousel === void 0 ? void 0 : carousel.className),
    style: Object.assign(Object.assign({}, carousel === null || carousel === void 0 ? void 0 : carousel.style), style)
  }, otherProps);
  if (newProps.effect === 'fade') {
    newProps.fade = true;
  }
  const prefixCls = getPrefixCls('carousel', newProps.prefixCls);
  const dotsClass = 'slick-dots';
  const enableDots = !!dots;
  const dsClass = (0, _classnames.default)(dotsClass, `${dotsClass}-${dotPosition}`, typeof dots === 'boolean' ? false : dots === null || dots === void 0 ? void 0 : dots.className);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const className = (0, _classnames.default)(prefixCls, {
    [`${prefixCls}-rtl`]: direction === 'rtl',
    [`${prefixCls}-vertical`]: newProps.vertical
  }, hashId, cssVarCls, rootClassName);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
    className: className,
    id: id
  }, /*#__PURE__*/React.createElement(_reactSlick.default, Object.assign({
    ref: slickRef
  }, newProps, {
    dots: enableDots,
    dotsClass: dsClass,
    arrows: arrows,
    draggable: draggable,
    verticalSwiping: vertical,
    waitForAnimate: waitForAnimate
  }))));
});
if (process.env.NODE_ENV !== 'production') {
  Carousel.displayName = 'Carousel';
}
var _default = exports.default = Carousel;