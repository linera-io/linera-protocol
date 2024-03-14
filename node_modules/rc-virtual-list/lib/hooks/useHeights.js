"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useHeights;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _findDOMNode = _interopRequireDefault(require("rc-util/lib/Dom/findDOMNode"));
var _raf = _interopRequireDefault(require("rc-util/lib/raf"));
var _CacheMap = _interopRequireDefault(require("../utils/CacheMap"));
function useHeights(getKey, onItemAdd, onItemRemove) {
  var _React$useState = React.useState(0),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    updatedMark = _React$useState2[0],
    setUpdatedMark = _React$useState2[1];
  var instanceRef = (0, _react.useRef)(new Map());
  var heightsRef = (0, _react.useRef)(new _CacheMap.default());
  var collectRafRef = (0, _react.useRef)();
  function cancelRaf() {
    _raf.default.cancel(collectRafRef.current);
  }
  function collectHeight() {
    var sync = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
    cancelRaf();
    var doCollect = function doCollect() {
      instanceRef.current.forEach(function (element, key) {
        if (element && element.offsetParent) {
          var htmlElement = (0, _findDOMNode.default)(element);
          var offsetHeight = htmlElement.offsetHeight;
          if (heightsRef.current.get(key) !== offsetHeight) {
            heightsRef.current.set(key, htmlElement.offsetHeight);
          }
        }
      });

      // Always trigger update mark to tell parent that should re-calculate heights when resized
      setUpdatedMark(function (c) {
        return c + 1;
      });
    };
    if (sync) {
      doCollect();
    } else {
      collectRafRef.current = (0, _raf.default)(doCollect);
    }
  }
  function setInstanceRef(item, instance) {
    var key = getKey(item);
    var origin = instanceRef.current.get(key);
    if (instance) {
      instanceRef.current.set(key, instance);
      collectHeight();
    } else {
      instanceRef.current.delete(key);
    }

    // Instance changed
    if (!origin !== !instance) {
      if (instance) {
        onItemAdd === null || onItemAdd === void 0 || onItemAdd(item);
      } else {
        onItemRemove === null || onItemRemove === void 0 || onItemRemove(item);
      }
    }
  }
  (0, _react.useEffect)(function () {
    return cancelRaf;
  }, []);
  return [setInstanceRef, collectHeight, heightsRef.current, updatedMark];
}