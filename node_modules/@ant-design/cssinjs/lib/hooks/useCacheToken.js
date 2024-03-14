"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.TOKEN_PREFIX = void 0;
exports.default = useCacheToken;
exports.getComputedToken = exports.extract = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _hash = _interopRequireDefault(require("@emotion/hash"));
var _dynamicCSS = require("rc-util/lib/Dom/dynamicCSS");
var _react = require("react");
var _StyleContext = _interopRequireWildcard(require("../StyleContext"));
var _util = require("../util");
var _cssVariables = require("../util/css-variables");
var _useGlobalCache = _interopRequireDefault(require("./useGlobalCache"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
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
    var styles = document.querySelectorAll("style[".concat(_StyleContext.ATTR_TOKEN, "=\"").concat(key, "\"]"));
    styles.forEach(function (style) {
      if (style[_StyleContext.CSS_IN_JS_INSTANCE] === instanceId) {
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
var getComputedToken = exports.getComputedToken = function getComputedToken(originToken, overrideToken, theme, format) {
  var derivativeToken = theme.getDerivativeToken(originToken);

  // Merge with override
  var mergedDerivativeToken = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, derivativeToken), overrideToken);

  // Format if needed
  if (format) {
    mergedDerivativeToken = format(mergedDerivativeToken);
  }
  return mergedDerivativeToken;
};
var TOKEN_PREFIX = exports.TOKEN_PREFIX = 'token';
/**
 * Cache theme derivative token as global shared one
 * @param theme Theme entity
 * @param tokens List of tokens, used for cache. Please do not dynamic generate object directly
 * @param option Additional config
 * @returns Call Theme.getDerivativeToken(tokenObject) to get token
 */
function useCacheToken(theme, tokens) {
  var option = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
  var _useContext = (0, _react.useContext)(_StyleContext.default),
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
  var mergedToken = (0, _util.memoResult)(function () {
    return Object.assign.apply(Object, [{}].concat((0, _toConsumableArray2.default)(tokens)));
  }, tokens);
  var tokenStr = (0, _util.flattenToken)(mergedToken);
  var overrideTokenStr = (0, _util.flattenToken)(override);
  var cssVarStr = cssVar ? (0, _util.flattenToken)(cssVar) : '';
  var cachedToken = (0, _useGlobalCache.default)(TOKEN_PREFIX, [salt, theme.id, tokenStr, overrideTokenStr, cssVarStr], function () {
    var _cssVar$key;
    var mergedDerivativeToken = compute ? compute(mergedToken, override, theme) : getComputedToken(mergedToken, override, theme, formatToken);

    // Replace token value with css variables
    var actualToken = (0, _objectSpread2.default)({}, mergedDerivativeToken);
    var cssVarsStr = '';
    if (!!cssVar) {
      var _transformToken = (0, _cssVariables.transformToken)(mergedDerivativeToken, cssVar.key, {
        prefix: cssVar.prefix,
        ignore: cssVar.ignore,
        unitless: cssVar.unitless,
        preserve: cssVar.preserve
      });
      var _transformToken2 = (0, _slicedToArray2.default)(_transformToken, 2);
      mergedDerivativeToken = _transformToken2[0];
      cssVarsStr = _transformToken2[1];
    }

    // Optimize for `useStyleRegister` performance
    var tokenKey = (0, _util.token2key)(mergedDerivativeToken, salt);
    mergedDerivativeToken._tokenKey = tokenKey;
    actualToken._tokenKey = (0, _util.token2key)(actualToken, salt);
    var themeKey = (_cssVar$key = cssVar === null || cssVar === void 0 ? void 0 : cssVar.key) !== null && _cssVar$key !== void 0 ? _cssVar$key : tokenKey;
    mergedDerivativeToken._themeKey = themeKey;
    recordCleanToken(themeKey);
    var hashId = "".concat(hashPrefix, "-").concat((0, _hash.default)(tokenKey));
    mergedDerivativeToken._hashId = hashId; // Not used

    return [mergedDerivativeToken, hashId, actualToken, cssVarsStr, (cssVar === null || cssVar === void 0 ? void 0 : cssVar.key) || ''];
  }, function (cache) {
    // Remove token will remove all related style
    cleanTokenStyle(cache[0]._themeKey, instanceId);
  }, function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 4),
      token = _ref2[0],
      cssVarsStr = _ref2[3];
    if (cssVar && cssVarsStr) {
      var style = (0, _dynamicCSS.updateCSS)(cssVarsStr, (0, _hash.default)("css-variables-".concat(token._themeKey)), {
        mark: _StyleContext.ATTR_MARK,
        prepend: 'queue',
        attachTo: container,
        priority: -999
      });
      style[_StyleContext.CSS_IN_JS_INSTANCE] = instanceId;

      // Used for `useCacheToken` to remove on batch when token removed
      style.setAttribute(_StyleContext.ATTR_TOKEN, token._themeKey);
    }
  });
  return cachedToken;
}
var extract = exports.extract = function extract(cache, effectStyles, options) {
  var _cache = (0, _slicedToArray2.default)(cache, 5),
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
  var styleText = (0, _util.toStyleStr)(styleStr, cssVarKey, styleId, sharedAttrs, plain);
  return [order, styleId, styleText];
};