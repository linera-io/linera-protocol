import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import { getClientSize } from "rc-util/es/Dom/css";
function fixPoint(key, start, width, clientWidth) {
  var startAddWidth = start + width;
  var offsetStart = (width - clientWidth) / 2;
  if (width > clientWidth) {
    if (start > 0) {
      return _defineProperty({}, key, offsetStart);
    }
    if (start < 0 && startAddWidth < clientWidth) {
      return _defineProperty({}, key, -offsetStart);
    }
  } else if (start < 0 || startAddWidth > clientWidth) {
    return _defineProperty({}, key, start < 0 ? offsetStart : -offsetStart);
  }
  return {};
}

/**
 * Fix positon x,y point when
 *
 * Ele width && height < client
 * - Back origin
 *
 * - Ele width | height > clientWidth | clientHeight
 * - left | top > 0 -> Back 0
 * - left | top + width | height < clientWidth | clientHeight -> Back left | top + width | height === clientWidth | clientHeight
 *
 * Regardless of other
 */
export default function getFixScaleEleTransPosition(width, height, left, top) {
  var _getClientSize = getClientSize(),
    clientWidth = _getClientSize.width,
    clientHeight = _getClientSize.height;
  var fixPos = null;
  if (width <= clientWidth && height <= clientHeight) {
    fixPos = {
      x: 0,
      y: 0
    };
  } else if (width > clientWidth || height > clientHeight) {
    fixPos = _objectSpread(_objectSpread({}, fixPoint('x', left, width, clientWidth)), fixPoint('y', top, height, clientHeight));
  }
  return fixPos;
}