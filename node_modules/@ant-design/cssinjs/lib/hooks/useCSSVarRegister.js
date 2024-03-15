"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.extract = exports.default = exports.CSS_VAR_PREFIX = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _dynamicCSS = require("rc-util/lib/Dom/dynamicCSS");
var _react = require("react");
var _StyleContext = _interopRequireWildcard(require("../StyleContext"));
var _util = require("../util");
var _cssVariables = require("../util/css-variables");
var _useGlobalCache = _interopRequireDefault(require("./useGlobalCache"));
var _useStyleRegister = require("./useStyleRegister");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var CSS_VAR_PREFIX = exports.CSS_VAR_PREFIX = 'cssVar';
var useCSSVarRegister = function useCSSVarRegister(config, fn) {
  var key = config.key,
    prefix = config.prefix,
    unitless = config.unitless,
    ignore = config.ignore,
    token = config.token,
    _config$scope = config.scope,
    scope = _config$scope === void 0 ? '' : _config$scope;
  var _useContext = (0, _react.useContext)(_StyleContext.default),
    instanceId = _useContext.cache.instanceId,
    container = _useContext.container;
  var tokenKey = token._tokenKey;
  var stylePath = [].concat((0, _toConsumableArray2.default)(config.path), [key, scope, tokenKey]);
  var cache = (0, _useGlobalCache.default)(CSS_VAR_PREFIX, stylePath, function () {
    var originToken = fn();
    var _transformToken = (0, _cssVariables.transformToken)(originToken, key, {
        prefix: prefix,
        unitless: unitless,
        ignore: ignore,
        scope: scope
      }),
      _transformToken2 = (0, _slicedToArray2.default)(_transformToken, 2),
      mergedToken = _transformToken2[0],
      cssVarsStr = _transformToken2[1];
    var styleId = (0, _useStyleRegister.uniqueHash)(stylePath, cssVarsStr);
    return [mergedToken, cssVarsStr, styleId, key];
  }, function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 3),
      styleId = _ref2[2];
    if (_util.isClientSide) {
      (0, _dynamicCSS.removeCSS)(styleId, {
        mark: _StyleContext.ATTR_MARK
      });
    }
  }, function (_ref3) {
    var _ref4 = (0, _slicedToArray2.default)(_ref3, 3),
      cssVarsStr = _ref4[1],
      styleId = _ref4[2];
    if (!cssVarsStr) {
      return;
    }
    var style = (0, _dynamicCSS.updateCSS)(cssVarsStr, styleId, {
      mark: _StyleContext.ATTR_MARK,
      prepend: 'queue',
      attachTo: container,
      priority: -999
    });
    style[_StyleContext.CSS_IN_JS_INSTANCE] = instanceId;

    // Used for `useCacheToken` to remove on batch when token removed
    style.setAttribute(_StyleContext.ATTR_TOKEN, key);
  });
  return cache;
};
var extract = exports.extract = function extract(cache, effectStyles, options) {
  var _cache = (0, _slicedToArray2.default)(cache, 4),
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
  var styleText = (0, _util.toStyleStr)(styleStr, cssVarKey, styleId, sharedAttrs, plain);
  return [order, styleId, styleText];
};
var _default = exports.default = useCSSVarRegister;