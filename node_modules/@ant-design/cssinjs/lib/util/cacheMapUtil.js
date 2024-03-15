"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.CSS_FILE_STYLE = exports.ATTR_CACHE_MAP = void 0;
exports.existPath = existPath;
exports.getStyleAndHash = getStyleAndHash;
exports.prepare = prepare;
exports.reset = reset;
exports.serialize = serialize;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _canUseDom = _interopRequireDefault(require("rc-util/lib/Dom/canUseDom"));
var _StyleContext = require("../StyleContext");
var ATTR_CACHE_MAP = exports.ATTR_CACHE_MAP = 'data-ant-cssinjs-cache-path';

/**
 * This marks style from the css file.
 * Which means not exist in `<style />` tag.
 */
var CSS_FILE_STYLE = exports.CSS_FILE_STYLE = '_FILE_STYLE__';
function serialize(cachePathMap) {
  return Object.keys(cachePathMap).map(function (path) {
    var hash = cachePathMap[path];
    return "".concat(path, ":").concat(hash);
  }).join(';');
}
var cachePathMap;
var fromCSSFile = true;

/**
 * @private Test usage only. Can save remove if no need.
 */
function reset(mockCache) {
  var fromFile = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;
  cachePathMap = mockCache;
  fromCSSFile = fromFile;
}
function prepare() {
  if (!cachePathMap) {
    cachePathMap = {};
    if ((0, _canUseDom.default)()) {
      var div = document.createElement('div');
      div.className = ATTR_CACHE_MAP;
      div.style.position = 'fixed';
      div.style.visibility = 'hidden';
      div.style.top = '-9999px';
      document.body.appendChild(div);
      var content = getComputedStyle(div).content || '';
      content = content.replace(/^"/, '').replace(/"$/, '');

      // Fill data
      content.split(';').forEach(function (item) {
        var _item$split = item.split(':'),
          _item$split2 = (0, _slicedToArray2.default)(_item$split, 2),
          path = _item$split2[0],
          hash = _item$split2[1];
        cachePathMap[path] = hash;
      });

      // Remove inline record style
      var inlineMapStyle = document.querySelector("style[".concat(ATTR_CACHE_MAP, "]"));
      if (inlineMapStyle) {
        var _inlineMapStyle$paren;
        fromCSSFile = false;
        (_inlineMapStyle$paren = inlineMapStyle.parentNode) === null || _inlineMapStyle$paren === void 0 || _inlineMapStyle$paren.removeChild(inlineMapStyle);
      }
      document.body.removeChild(div);
    }
  }
}
function existPath(path) {
  prepare();
  return !!cachePathMap[path];
}
function getStyleAndHash(path) {
  var hash = cachePathMap[path];
  var styleStr = null;
  if (hash && (0, _canUseDom.default)()) {
    if (fromCSSFile) {
      styleStr = CSS_FILE_STYLE;
    } else {
      var _style = document.querySelector("style[".concat(_StyleContext.ATTR_MARK, "=\"").concat(cachePathMap[path], "\"]"));
      if (_style) {
        styleStr = _style.innerHTML;
      } else {
        // Clean up since not exist anymore
        delete cachePathMap[path];
      }
    }
  }
  return [styleStr, hash];
}