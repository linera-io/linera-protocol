import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import hash from '@emotion/hash';
import { updateCSS } from "rc-util/es/Dom/dynamicCSS";
import { useContext } from 'react';
import StyleContext, { ATTR_MARK, ATTR_TOKEN, CSS_IN_JS_INSTANCE } from "../StyleContext";
import { flattenToken, memoResult, token2key, toStyleStr } from "../util";
import { transformToken } from "../util/css-variables";
import useGlobalCache from "./useGlobalCache";
var EMPTY_OVERRIDE = {};

// Generate different prefix to make user selector break in production env.
// This helps developer not to do style override directly on the hash id.
var hashPrefix = process.env.NODE_ENV !== 'production' ? 'css-dev-only-do-not-override' : 'css';
var tokenKeys = new Map();
function recordCleanToken(tokenKey) {
  tokenKeys.set(tokenKey, (tokenKeys.get(tokenKey) || 0) + 1);
}
function removeStyleTags(key, instanceId) {
  if (typeof document !== 'undefined') {
    var styles = document.querySelectorAll("style[".concat(ATTR_TOKEN, "=\"").concat(key, "\"]"));
    styles.forEach(function (style) {
      if (style[CSS_IN_JS_INSTANCE] === instanceId) {
        var _style$parentNode;
        (_style$parentNode = style.parentNode) === null || _style$parentNode === void 0 || _style$parentNode.removeChild(style);
      }
    });
  }
}
var TOKEN_THRESHOLD = 0;

// Remove will check current keys first
function cleanTokenStyle(tokenKey, instanceId) {
  tokenKeys.set(tokenKey, (tokenKeys.get(tokenKey) || 0) - 1);
  var tokenKeyList = Array.from(tokenKeys.keys());
  var cleanableKeyList = tokenKeyList.filter(function (key) {
    var count = tokenKeys.get(key) || 0;
    return count <= 0;
  });

  // Should keep tokens under threshold for not to insert style too often
  if (tokenKeyList.length - cleanableKeyList.length > TOKEN_THRESHOLD) {
    cleanableKeyList.forEach(function (key) {
      removeStyleTags(key, instanceId);
      tokenKeys.delete(key);
    });
  }
}
export var getComputedToken = function getComputedToken(originToken, overrideToken, theme, format) {
  var derivativeToken = theme.getDerivativeToken(originToken);

  // Merge with override
  var mergedDerivativeToken = _objectSpread(_objectSpread({}, derivativeToken), overrideToken);

  // Format if needed
  if (format) {
    mergedDerivativeToken = format(mergedDerivativeToken);
  }
  return mergedDerivativeToken;
};
export var TOKEN_PREFIX = 'token';
/**
 * Cache theme derivative token as global shared one
 * @param theme Theme entity
 * @param tokens List of tokens, used for cache. Please do not dynamic generate object directly
 * @param option Additional config
 * @returns Call Theme.getDerivativeToken(tokenObject) to get token
 */
export default function useCacheToken(theme, tokens) {
  var option = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
  var _useContext = useContext(StyleContext),
    instanceId = _useContext.cache.instanceId,
    container = _useContext.container;
  var _option$salt = option.salt,
    salt = _option$salt === void 0 ? '' : _option$salt,
    _option$override = option.override,
    override = _option$override === void 0 ? EMPTY_OVERRIDE : _option$override,
    formatToken = option.formatToken,
    compute = option.getComputedToken,
    cssVar = option.cssVar;

  // Basic - We do basic cache here
  var mergedToken = memoResult(function () {
    return Object.assign.apply(Object, [{}].concat(_toConsumableArray(tokens)));
  }, tokens);
  var tokenStr = flattenToken(mergedToken);
  var overrideTokenStr = flattenToken(override);
  var cssVarStr = cssVar ? flattenToken(cssVar) : '';
  var cachedToken = useGlobalCache(TOKEN_PREFIX, [salt, theme.id, tokenStr, overrideTokenStr, cssVarStr], function () {
    var _cssVar$key;
    var mergedDerivativeToken = compute ? compute(mergedToken, override, theme) : getComputedToken(mergedToken, override, theme, formatToken);

    // Replace token value with css variables
    var actualToken = _objectSpread({}, mergedDerivativeToken);
    var cssVarsStr = '';
    if (!!cssVar) {
      var _transformToken = transformToken(mergedDerivativeToken, cssVar.key, {
        prefix: cssVar.prefix,
        ignore: cssVar.ignore,
        unitless: cssVar.unitless,
        preserve: cssVar.preserve
      });
      var _transformToken2 = _slicedToArray(_transformToken, 2);
      mergedDerivativeToken = _transformToken2[0];
      cssVarsStr = _transformToken2[1];
    }

    // Optimize for `useStyleRegister` performance
    var tokenKey = token2key(mergedDerivativeToken, salt);
    mergedDerivativeToken._tokenKey = tokenKey;
    actualToken._tokenKey = token2key(actualToken, salt);
    var themeKey = (_cssVar$key = cssVar === null || cssVar === void 0 ? void 0 : cssVar.key) !== null && _cssVar$key !== void 0 ? _cssVar$key : tokenKey;
    mergedDerivativeToken._themeKey = themeKey;
    recordCleanToken(themeKey);
    var hashId = "".concat(hashPrefix, "-").concat(hash(tokenKey));
    mergedDerivativeToken._hashId = hashId; // Not used

    return [mergedDerivativeToken, hashId, actualToken, cssVarsStr, (cssVar === null || cssVar === void 0 ? void 0 : cssVar.key) || ''];
  }, function (cache) {
    // Remove token will remove all related style
    cleanTokenStyle(cache[0]._themeKey, instanceId);
  }, function (_ref) {
    var _ref2 = _slicedToArray(_ref, 4),
      token = _ref2[0],
      cssVarsStr = _ref2[3];
    if (cssVar && cssVarsStr) {
      var style = updateCSS(cssVarsStr, hash("css-variables-".concat(token._themeKey)), {
        mark: ATTR_MARK,
        prepend: 'queue',
        attachTo: container,
        priority: -999
      });
      style[CSS_IN_JS_INSTANCE] = instanceId;

      // Used for `useCacheToken` to remove on batch when token removed
      style.setAttribute(ATTR_TOKEN, token._themeKey);
    }
  });
  return cachedToken;
}
export var extract = function extract(cache, effectStyles, options) {
  var _cache = _slicedToArray(cache, 5),
    realToken = _cache[2],
    styleStr = _cache[3],
    cssVarKey = _cache[4];
  var _ref3 = options || {},
    plain = _ref3.plain;
  if (!styleStr) {
    return null;
  }
  var styleId = realToken._tokenKey;
  var order = -999;

  // ====================== Style ======================
  // Used for rc-util
  var sharedAttrs = {
    'data-rc-order': 'prependQueue',
    'data-rc-priority': "".concat(order)
  };
  var styleText = toStyleStr(styleStr, cssVarKey, styleId, sharedAttrs, plain);
  return [order, styleId, styleText];
};