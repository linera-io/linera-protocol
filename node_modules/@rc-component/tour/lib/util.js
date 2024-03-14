"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getPlacement = getPlacement;
exports.isInViewPort = isInViewPort;
function isInViewPort(element) {
  var viewWidth = window.innerWidth || document.documentElement.clientWidth;
  var viewHeight = window.innerHeight || document.documentElement.clientHeight;
  var _element$getBoundingC = element.getBoundingClientRect(),
    top = _element$getBoundingC.top,
    right = _element$getBoundingC.right,
    bottom = _element$getBoundingC.bottom,
    left = _element$getBoundingC.left;
  return top >= 0 && left >= 0 && right <= viewWidth && bottom <= viewHeight;
}
function getPlacement(targetElement, placement, stepPlacement) {
  var _ref;
  return (_ref = stepPlacement !== null && stepPlacement !== void 0 ? stepPlacement : placement) !== null && _ref !== void 0 ? _ref : targetElement === null ? 'center' : 'bottom';
}