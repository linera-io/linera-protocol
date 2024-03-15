import _typeof from "@babel/runtime/helpers/esm/typeof";
import * as React from 'react';
var Block = function Block(_ref) {
  var bg = _ref.bg,
    children = _ref.children;
  return /*#__PURE__*/React.createElement("div", {
    style: {
      width: '100%',
      height: '100%',
      background: bg
    }
  }, children);
};
function getPtgColors(color, scale) {
  return Object.keys(color).map(function (key) {
    var parsedKey = parseFloat(key);
    var ptgKey = "".concat(Math.floor(parsedKey * scale), "%");
    return "".concat(color[key], " ").concat(ptgKey);
  });
}
var PtgCircle = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var prefixCls = props.prefixCls,
    color = props.color,
    gradientId = props.gradientId,
    radius = props.radius,
    circleStyleForStack = props.style,
    ptg = props.ptg,
    strokeLinecap = props.strokeLinecap,
    strokeWidth = props.strokeWidth,
    size = props.size,
    gapDegree = props.gapDegree;
  var isGradient = color && _typeof(color) === 'object';
  var stroke = isGradient ? "#FFF" : undefined;
  // ========================== Circle ==========================
  var halfSize = size / 2;
  var circleNode = /*#__PURE__*/React.createElement("circle", {
    className: "".concat(prefixCls, "-circle-path"),
    r: radius,
    cx: halfSize,
    cy: halfSize,
    stroke: stroke,
    strokeLinecap: strokeLinecap,
    strokeWidth: strokeWidth,
    opacity: ptg === 0 ? 0 : 1,
    style: circleStyleForStack,
    ref: ref
  });
  // ========================== Render ==========================
  if (!isGradient) {
    return circleNode;
  }
  var maskId = "".concat(gradientId, "-conic");
  var fromDeg = gapDegree ? "".concat(180 + gapDegree / 2, "deg") : '0deg';
  var conicColors = getPtgColors(color, (360 - gapDegree) / 360);
  var linearColors = getPtgColors(color, 1);
  var conicColorBg = "conic-gradient(from ".concat(fromDeg, ", ").concat(conicColors.join(', '), ")");
  var linearColorBg = "linear-gradient(to ".concat(gapDegree ? 'bottom' : 'top', ", ").concat(linearColors.join(', '), ")");
  return /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("mask", {
    id: maskId
  }, circleNode), /*#__PURE__*/React.createElement("foreignObject", {
    x: 0,
    y: 0,
    width: size,
    height: size,
    mask: "url(#".concat(maskId, ")")
  }, /*#__PURE__*/React.createElement(Block, {
    bg: linearColorBg
  }, /*#__PURE__*/React.createElement(Block, {
    bg: conicColorBg
  }))));
});
if (process.env.NODE_ENV !== 'production') {
  PtgCircle.displayName = 'PtgCircle';
}
export default PtgCircle;