import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
var _ExtractStyleFns;
import { extract as tokenExtractStyle, TOKEN_PREFIX } from "./hooks/useCacheToken";
import { CSS_VAR_PREFIX, extract as cssVarExtractStyle } from "./hooks/useCSSVarRegister";
import { extract as styleExtractStyle, STYLE_PREFIX } from "./hooks/useStyleRegister";
import { toStyleStr } from "./util";
import { ATTR_CACHE_MAP, serialize as serializeCacheMap } from "./util/cacheMapUtil";
var ExtractStyleFns = (_ExtractStyleFns = {}, _defineProperty(_ExtractStyleFns, STYLE_PREFIX, styleExtractStyle), _defineProperty(_ExtractStyleFns, TOKEN_PREFIX, tokenExtractStyle), _defineProperty(_ExtractStyleFns, CSS_VAR_PREFIX, cssVarExtractStyle), _ExtractStyleFns);
function isNotNull(value) {
  return value !== null;
}
export default function extractStyle(cache, options) {
  var _ref = typeof options === 'boolean' ? {
      plain: options
    } : options || {},
    _ref$plain = _ref.plain,
    plain = _ref$plain === void 0 ? false : _ref$plain,
    _ref$types = _ref.types,
    types = _ref$types === void 0 ? ['style', 'token', 'cssVar'] : _ref$types;
  var matchPrefixRegexp = new RegExp("^(".concat((typeof types === 'string' ? [types] : types).join('|'), ")%"));

  // prefix with `style` is used for `useStyleRegister` to cache style context
  var styleKeys = Array.from(cache.cache.keys()).filter(function (key) {
    return matchPrefixRegexp.test(key);
  });

  // Common effect styles like animation
  var effectStyles = {};

  // Mapping of cachePath to style hash
  var cachePathMap = {};
  var styleText = '';
  styleKeys.map(function (key) {
    var cachePath = key.replace(matchPrefixRegexp, '').replace(/%/g, '|');
    var _key$split = key.split('%'),
      _key$split2 = _slicedToArray(_key$split, 1),
      prefix = _key$split2[0];
    var extractFn = ExtractStyleFns[prefix];
    var extractedStyle = extractFn(cache.cache.get(key)[1], effectStyles, {
      plain: plain
    });
    if (!extractedStyle) {
      return null;
    }
    var _extractedStyle = _slicedToArray(extractedStyle, 3),
      order = _extractedStyle[0],
      styleId = _extractedStyle[1],
      styleStr = _extractedStyle[2];
    if (key.startsWith('style')) {
      cachePathMap[cachePath] = styleId;
    }
    return [order, styleStr];
  }).filter(isNotNull).sort(function (_ref2, _ref3) {
    var _ref4 = _slicedToArray(_ref2, 1),
      o1 = _ref4[0];
    var _ref5 = _slicedToArray(_ref3, 1),
      o2 = _ref5[0];
    return o1 - o2;
  }).forEach(function (_ref6) {
    var _ref7 = _slicedToArray(_ref6, 2),
      style = _ref7[1];
    styleText += style;
  });

  // ==================== Fill Cache Path ====================
  styleText += toStyleStr(".".concat(ATTR_CACHE_MAP, "{content:\"").concat(serializeCacheMap(cachePathMap), "\";}"), undefined, undefined, _defineProperty({}, ATTR_CACHE_MAP, ATTR_CACHE_MAP), plain);
  return styleText;
}