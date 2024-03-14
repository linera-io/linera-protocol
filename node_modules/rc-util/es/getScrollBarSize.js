/* eslint-disable no-param-reassign */
import { removeCSS, updateCSS } from "./Dom/dynamicCSS";
var cached;
function measureScrollbarSize(ele) {
  var randomId = "rc-scrollbar-measure-".concat(Math.random().toString(36).substring(7));
  var measureEle = document.createElement('div');
  measureEle.id = randomId;

  // Create Style
  var measureStyle = measureEle.style;
  measureStyle.position = 'absolute';
  measureStyle.left = '0';
  measureStyle.top = '0';
  measureStyle.width = '100px';
  measureStyle.height = '100px';
  measureStyle.overflow = 'scroll';

  // Clone Style if needed
  var fallbackWidth;
  var fallbackHeight;
  if (ele) {
    var targetStyle = getComputedStyle(ele);
    measureStyle.scrollbarColor = targetStyle.scrollbarColor;
    measureStyle.scrollbarWidth = targetStyle.scrollbarWidth;

    // Set Webkit style
    var webkitScrollbarStyle = getComputedStyle(ele, '::-webkit-scrollbar');
    var width = parseInt(webkitScrollbarStyle.width, 10);
    var height = parseInt(webkitScrollbarStyle.height, 10);

    // Try wrap to handle CSP case
    try {
      var widthStyle = width ? "width: ".concat(webkitScrollbarStyle.width, ";") : '';
      var heightStyle = height ? "height: ".concat(webkitScrollbarStyle.height, ";") : '';
      updateCSS("\n#".concat(randomId, "::-webkit-scrollbar {\n").concat(widthStyle, "\n").concat(heightStyle, "\n}"), randomId);
    } catch (e) {
      // Can't wrap, just log error
      console.error(e);

      // Get from style directly
      fallbackWidth = width;
      fallbackHeight = height;
    }
  }
  document.body.appendChild(measureEle);

  // Measure. Get fallback style if provided
  var scrollWidth = ele && fallbackWidth && !isNaN(fallbackWidth) ? fallbackWidth : measureEle.offsetWidth - measureEle.clientWidth;
  var scrollHeight = ele && fallbackHeight && !isNaN(fallbackHeight) ? fallbackHeight : measureEle.offsetHeight - measureEle.clientHeight;

  // Clean up
  document.body.removeChild(measureEle);
  removeCSS(randomId);
  return {
    width: scrollWidth,
    height: scrollHeight
  };
}
export default function getScrollBarSize(fresh) {
  if (typeof document === 'undefined') {
    return 0;
  }
  if (fresh || cached === undefined) {
    cached = measureScrollbarSize();
  }
  return cached.width;
}
export function getTargetScrollBarSize(target) {
  if (typeof document === 'undefined' || !target || !(target instanceof Element)) {
    return {
      width: 0,
      height: 0
    };
  }
  return measureScrollbarSize(target);
}