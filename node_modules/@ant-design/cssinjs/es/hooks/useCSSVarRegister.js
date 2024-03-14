import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import { removeCSS, updateCSS } from "rc-util/es/Dom/dynamicCSS";
import { useContext } from 'react';
import StyleContext, { ATTR_MARK, ATTR_TOKEN, CSS_IN_JS_INSTANCE } from "../StyleContext";
import { isClientSide, toStyleStr } from "../util";
import { transformToken } from "../util/css-variables";
import useGlobalCache from "./useGlobalCache";
import { uniqueHash } from "./useStyleRegister";
export var CSS_VAR_PREFIX = 'cssVar';
var useCSSVarRegister = function useCSSVarRegister(config, fn) {
  var key = config.key,
    prefix = config.prefix,
    unitless = config.unitless,
    ignore = config.ignore,
    token = config.token,
    _config$scope = config.scope,
    scope = _config$scope === void 0 ? '' : _config$scope;
  var _useContext = useContext(StyleContext),
    instanceId = _useContext.cache.instanceId,
    container = _useContext.container;
  var tokenKey = token._tokenKey;
  var stylePath = [].concat(_toConsumableArray(config.path), [key, scope, tokenKey]);
  var cache = useGlobalCache(CSS_VAR_PREFIX, stylePath, function () {
    var originToken = fn();
    var _transformToken = transformToken(originToken, key, {
        prefix: prefix,
        unitless: unitless,
        ignore: ignore,
        scope: scope
      }),
      _transformToken2 = _slicedToArray(_transformToken, 2),
      mergedToken = _transformToken2[0],
      cssVarsStr = _transformToken2[1];
    var styleId = uniqueHash(stylePath, cssVarsStr);
    return [mergedToken, cssVarsStr, styleId, key];
  }, function (_ref) {
    var _ref2 = _slicedToArray(_ref, 3),
      styleId = _ref2[2];
    if (isClientSide) {
      removeCSS(styleId, {
        mark: ATTR_MARK
      });
    }
  }, function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 3),
      cssVarsStr = _ref4[1],
      styleId = _ref4[2];
    if (!cssVarsStr) {
      return;
    }
    var style = updateCSS(cssVarsStr, styleId, {
      mark: ATTR_MARK,
      prepend: 'queue',
      attachTo: container,
      priority: -999
    });
    style[CSS_IN_JS_INSTANCE] = instanceId;

    // Used for `useCacheToken` to remove on batch when token removed
    style.setAttribute(ATTR_TOKEN, key);
  });
  return cache;
};
export var extract = function extract(cache, effectStyles, options) {
  var _cache = _slicedToArray(cache, 4),
    styleStr = _cache[1],
    styleId = _cache[2],
    cssVarKey = _cache[3];
  var _ref5 = options || {},
    plain = _ref5.plain;
  if (!styleStr) {
    return null;
  }
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
export default useCSSVarRegister;