"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof3 = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.STYLE_PREFIX = void 0;
exports.default = useStyleRegister;
exports.extract = void 0;
exports.normalizeStyle = normalizeStyle;
exports.parseStyle = void 0;
exports.uniqueHash = uniqueHash;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _hash = _interopRequireDefault(require("@emotion/hash"));
var _dynamicCSS = require("rc-util/lib/Dom/dynamicCSS");
var React = _interopRequireWildcard(require("react"));
var _unitless = _interopRequireDefault(require("@emotion/unitless"));
var _stylis = require("stylis");
var _linters = require("../linters");
var _StyleContext = _interopRequireWildcard(require("../StyleContext"));
var _util = require("../util");
var _cacheMapUtil = require("../util/cacheMapUtil");
var _useGlobalCache3 = _interopRequireDefault(require("./useGlobalCache"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof3(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
// @ts-ignore

var SKIP_CHECK = '_skip_check_';
var MULTI_VALUE = '_multi_value_';
// ============================================================================
// ==                                 Parser                                 ==
// ============================================================================
// Preprocessor style content to browser support one
function normalizeStyle(styleStr) {
  var serialized = (0, _stylis.serialize)((0, _stylis.compile)(styleStr), _stylis.stringify);
  return serialized.replace(/\{%%%\:[^;];}/g, ';');
}
function isCompoundCSSProperty(value) {
  return (0, _typeof2.default)(value) === 'object' && value && (SKIP_CHECK in value || MULTI_VALUE in value);
}

// 注入 hash 值
function injectSelectorHash(key, hashId, hashPriority) {
  if (!hashId) {
    return key;
  }
  var hashClassName = ".".concat(hashId);
  var hashSelector = hashPriority === 'low' ? ":where(".concat(hashClassName, ")") : hashClassName;

  // 注入 hashId
  var keys = key.split(',').map(function (k) {
    var _firstPath$match;
    var fullPath = k.trim().split(/\s+/);

    // 如果 Selector 第一个是 HTML Element，那我们就插到它的后面。反之，就插到最前面。
    var firstPath = fullPath[0] || '';
    var htmlElement = ((_firstPath$match = firstPath.match(/^\w+/)) === null || _firstPath$match === void 0 ? void 0 : _firstPath$match[0]) || '';
    firstPath = "".concat(htmlElement).concat(hashSelector).concat(firstPath.slice(htmlElement.length));
    return [firstPath].concat((0, _toConsumableArray2.default)(fullPath.slice(1))).join(' ');
  });
  return keys.join(',');
}
// Parse CSSObject to style content
var parseStyle = exports.parseStyle = function parseStyle(interpolation) {
  var config = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var _ref = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {
      root: true,
      parentSelectors: []
    },
    root = _ref.root,
    injectHash = _ref.injectHash,
    parentSelectors = _ref.parentSelectors;
  var hashId = config.hashId,
    layer = config.layer,
    path = config.path,
    hashPriority = config.hashPriority,
    _config$transformers = config.transformers,
    transformers = _config$transformers === void 0 ? [] : _config$transformers,
    _config$linters = config.linters,
    linters = _config$linters === void 0 ? [] : _config$linters;
  var styleStr = '';
  var effectStyle = {};
  function parseKeyframes(keyframes) {
    var animationName = keyframes.getName(hashId);
    if (!effectStyle[animationName]) {
      var _parseStyle = parseStyle(keyframes.style, config, {
          root: false,
          parentSelectors: parentSelectors
        }),
        _parseStyle2 = (0, _slicedToArray2.default)(_parseStyle, 1),
        _parsedStr = _parseStyle2[0];
      effectStyle[animationName] = "@keyframes ".concat(keyframes.getName(hashId)).concat(_parsedStr);
    }
  }
  function flattenList(list) {
    var fullList = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
    list.forEach(function (item) {
      if (Array.isArray(item)) {
        flattenList(item, fullList);
      } else if (item) {
        fullList.push(item);
      }
    });
    return fullList;
  }
  var flattenStyleList = flattenList(Array.isArray(interpolation) ? interpolation : [interpolation]);
  flattenStyleList.forEach(function (originStyle) {
    // Only root level can use raw string
    var style = typeof originStyle === 'string' && !root ? {} : originStyle;
    if (typeof style === 'string') {
      styleStr += "".concat(style, "\n");
    } else if (style._keyframe) {
      // Keyframe
      parseKeyframes(style);
    } else {
      var mergedStyle = transformers.reduce(function (prev, trans) {
        var _trans$visit;
        return (trans === null || trans === void 0 || (_trans$visit = trans.visit) === null || _trans$visit === void 0 ? void 0 : _trans$visit.call(trans, prev)) || prev;
      }, style);

      // Normal CSSObject
      Object.keys(mergedStyle).forEach(function (key) {
        var value = mergedStyle[key];
        if ((0, _typeof2.default)(value) === 'object' && value && (key !== 'animationName' || !value._keyframe) && !isCompoundCSSProperty(value)) {
          var subInjectHash = false;

          // 当成嵌套对象来处理
          var mergedKey = key.trim();
          // Whether treat child as root. In most case it is false.
          var nextRoot = false;

          // 拆分多个选择器
          if ((root || injectHash) && hashId) {
            if (mergedKey.startsWith('@')) {
              // 略过媒体查询，交给子节点继续插入 hashId
              subInjectHash = true;
            } else {
              // 注入 hashId
              mergedKey = injectSelectorHash(key, hashId, hashPriority);
            }
          } else if (root && !hashId && (mergedKey === '&' || mergedKey === '')) {
            // In case of `{ '&': { a: { color: 'red' } } }` or `{ '': { a: { color: 'red' } } }` without hashId,
            // we will get `&{a:{color:red;}}` or `{a:{color:red;}}` string for stylis to compile.
            // But it does not conform to stylis syntax,
            // and finally we will get `{color:red;}` as css, which is wrong.
            // So we need to remove key in root, and treat child `{ a: { color: 'red' } }` as root.
            mergedKey = '';
            nextRoot = true;
          }
          var _parseStyle3 = parseStyle(value, config, {
              root: nextRoot,
              injectHash: subInjectHash,
              parentSelectors: [].concat((0, _toConsumableArray2.default)(parentSelectors), [mergedKey])
            }),
            _parseStyle4 = (0, _slicedToArray2.default)(_parseStyle3, 2),
            _parsedStr2 = _parseStyle4[0],
            childEffectStyle = _parseStyle4[1];
          effectStyle = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, effectStyle), childEffectStyle);
          styleStr += "".concat(mergedKey).concat(_parsedStr2);
        } else {
          var _value;
          function appendStyle(cssKey, cssValue) {
            if (process.env.NODE_ENV !== 'production' && ((0, _typeof2.default)(value) !== 'object' || !(value !== null && value !== void 0 && value[SKIP_CHECK]))) {
              [_linters.contentQuotesLinter, _linters.hashedAnimationLinter].concat((0, _toConsumableArray2.default)(linters)).forEach(function (linter) {
                return linter(cssKey, cssValue, {
                  path: path,
                  hashId: hashId,
                  parentSelectors: parentSelectors
                });
              });
            }

            // 如果是样式则直接插入
            var styleName = cssKey.replace(/[A-Z]/g, function (match) {
              return "-".concat(match.toLowerCase());
            });

            // Auto suffix with px
            var formatValue = cssValue;
            if (!_unitless.default[cssKey] && typeof formatValue === 'number' && formatValue !== 0) {
              formatValue = "".concat(formatValue, "px");
            }

            // handle animationName & Keyframe value
            if (cssKey === 'animationName' && cssValue !== null && cssValue !== void 0 && cssValue._keyframe) {
              parseKeyframes(cssValue);
              formatValue = cssValue.getName(hashId);
            }
            styleStr += "".concat(styleName, ":").concat(formatValue, ";");
          }
          var actualValue = (_value = value === null || value === void 0 ? void 0 : value.value) !== null && _value !== void 0 ? _value : value;
          if ((0, _typeof2.default)(value) === 'object' && value !== null && value !== void 0 && value[MULTI_VALUE] && Array.isArray(actualValue)) {
            actualValue.forEach(function (item) {
              appendStyle(key, item);
            });
          } else {
            appendStyle(key, actualValue);
          }
        }
      });
    }
  });
  if (!root) {
    styleStr = "{".concat(styleStr, "}");
  } else if (layer && (0, _util.supportLayer)()) {
    var layerCells = layer.split(',');
    var layerName = layerCells[layerCells.length - 1].trim();
    styleStr = "@layer ".concat(layerName, " {").concat(styleStr, "}");

    // Order of layer if needed
    if (layerCells.length > 1) {
      // zombieJ: stylis do not support layer order, so we need to handle it manually.
      styleStr = "@layer ".concat(layer, "{%%%:%}").concat(styleStr);
    }
  }
  return [styleStr, effectStyle];
};

// ============================================================================
// ==                                Register                                ==
// ============================================================================
function uniqueHash(path, styleStr) {
  return (0, _hash.default)("".concat(path.join('%')).concat(styleStr));
}
function Empty() {
  return null;
}
var STYLE_PREFIX = exports.STYLE_PREFIX = 'style';
/**
 * Register a style to the global style sheet.
 */
function useStyleRegister(info, styleFn) {
  var token = info.token,
    path = info.path,
    hashId = info.hashId,
    layer = info.layer,
    nonce = info.nonce,
    clientOnly = info.clientOnly,
    _info$order = info.order,
    order = _info$order === void 0 ? 0 : _info$order;
  var _React$useContext = React.useContext(_StyleContext.default),
    autoClear = _React$useContext.autoClear,
    mock = _React$useContext.mock,
    defaultCache = _React$useContext.defaultCache,
    hashPriority = _React$useContext.hashPriority,
    container = _React$useContext.container,
    ssrInline = _React$useContext.ssrInline,
    transformers = _React$useContext.transformers,
    linters = _React$useContext.linters,
    cache = _React$useContext.cache;
  var tokenKey = token._tokenKey;
  var fullPath = [tokenKey].concat((0, _toConsumableArray2.default)(path));

  // Check if need insert style
  var isMergedClientSide = _util.isClientSide;
  if (process.env.NODE_ENV !== 'production' && mock !== undefined) {
    isMergedClientSide = mock === 'client';
  }
  var _useGlobalCache = (0, _useGlobalCache3.default)(STYLE_PREFIX, fullPath,
    // Create cache if needed
    function () {
      var cachePath = fullPath.join('|');

      // Get style from SSR inline style directly
      if ((0, _cacheMapUtil.existPath)(cachePath)) {
        var _getStyleAndHash = (0, _cacheMapUtil.getStyleAndHash)(cachePath),
          _getStyleAndHash2 = (0, _slicedToArray2.default)(_getStyleAndHash, 2),
          inlineCacheStyleStr = _getStyleAndHash2[0],
          styleHash = _getStyleAndHash2[1];
        if (inlineCacheStyleStr) {
          return [inlineCacheStyleStr, tokenKey, styleHash, {}, clientOnly, order];
        }
      }

      // Generate style
      var styleObj = styleFn();
      var _parseStyle5 = parseStyle(styleObj, {
          hashId: hashId,
          hashPriority: hashPriority,
          layer: layer,
          path: path.join('-'),
          transformers: transformers,
          linters: linters
        }),
        _parseStyle6 = (0, _slicedToArray2.default)(_parseStyle5, 2),
        parsedStyle = _parseStyle6[0],
        effectStyle = _parseStyle6[1];
      var styleStr = normalizeStyle(parsedStyle);
      var styleId = uniqueHash(fullPath, styleStr);
      return [styleStr, tokenKey, styleId, effectStyle, clientOnly, order];
    },
    // Remove cache if no need
    function (_ref2, fromHMR) {
      var _ref3 = (0, _slicedToArray2.default)(_ref2, 3),
        styleId = _ref3[2];
      if ((fromHMR || autoClear) && _util.isClientSide) {
        (0, _dynamicCSS.removeCSS)(styleId, {
          mark: _StyleContext.ATTR_MARK
        });
      }
    },
    // Effect: Inject style here
    function (_ref4) {
      var _ref5 = (0, _slicedToArray2.default)(_ref4, 4),
        styleStr = _ref5[0],
        _ = _ref5[1],
        styleId = _ref5[2],
        effectStyle = _ref5[3];
      if (isMergedClientSide && styleStr !== _cacheMapUtil.CSS_FILE_STYLE) {
        var mergedCSSConfig = {
          mark: _StyleContext.ATTR_MARK,
          prepend: 'queue',
          attachTo: container,
          priority: order
        };
        var nonceStr = typeof nonce === 'function' ? nonce() : nonce;
        if (nonceStr) {
          mergedCSSConfig.csp = {
            nonce: nonceStr
          };
        }
        var style = (0, _dynamicCSS.updateCSS)(styleStr, styleId, mergedCSSConfig);
        style[_StyleContext.CSS_IN_JS_INSTANCE] = cache.instanceId;

        // Used for `useCacheToken` to remove on batch when token removed
        style.setAttribute(_StyleContext.ATTR_TOKEN, tokenKey);

        // Debug usage. Dev only
        if (process.env.NODE_ENV !== 'production') {
          style.setAttribute(_StyleContext.ATTR_CACHE_PATH, fullPath.join('|'));
        }

        // Inject client side effect style
        Object.keys(effectStyle).forEach(function (effectKey) {
          (0, _dynamicCSS.updateCSS)(normalizeStyle(effectStyle[effectKey]), "_effect-".concat(effectKey), mergedCSSConfig);
        });
      }
    }),
    _useGlobalCache2 = (0, _slicedToArray2.default)(_useGlobalCache, 3),
    cachedStyleStr = _useGlobalCache2[0],
    cachedTokenKey = _useGlobalCache2[1],
    cachedStyleId = _useGlobalCache2[2];
  return function (node) {
    var styleNode;
    if (!ssrInline || isMergedClientSide || !defaultCache) {
      styleNode = /*#__PURE__*/React.createElement(Empty, null);
    } else {
      var _ref6;
      styleNode = /*#__PURE__*/React.createElement("style", (0, _extends2.default)({}, (_ref6 = {}, (0, _defineProperty2.default)(_ref6, _StyleContext.ATTR_TOKEN, cachedTokenKey), (0, _defineProperty2.default)(_ref6, _StyleContext.ATTR_MARK, cachedStyleId), _ref6), {
        dangerouslySetInnerHTML: {
          __html: cachedStyleStr
        }
      }));
    }
    return /*#__PURE__*/React.createElement(React.Fragment, null, styleNode, node);
  };
}
var extract = exports.extract = function extract(cache, effectStyles, options) {
  var _cache = (0, _slicedToArray2.default)(cache, 6),
    styleStr = _cache[0],
    tokenKey = _cache[1],
    styleId = _cache[2],
    effectStyle = _cache[3],
    clientOnly = _cache[4],
    order = _cache[5];
  var _ref7 = options || {},
    plain = _ref7.plain;

  // Skip client only style
  if (clientOnly) {
    return null;
  }
  var keyStyleText = styleStr;

  // ====================== Style ======================
  // Used for rc-util
  var sharedAttrs = {
    'data-rc-order': 'prependQueue',
    'data-rc-priority': "".concat(order)
  };
  keyStyleText = (0, _util.toStyleStr)(styleStr, tokenKey, styleId, sharedAttrs, plain);

  // =============== Create effect style ===============
  if (effectStyle) {
    Object.keys(effectStyle).forEach(function (effectKey) {
      // Effect style can be reused
      if (!effectStyles[effectKey]) {
        effectStyles[effectKey] = true;
        var effectStyleStr = normalizeStyle(effectStyle[effectKey]);
        keyStyleText += (0, _util.toStyleStr)(effectStyleStr, tokenKey, "_effect-".concat(effectKey), sharedAttrs, plain);
      }
    });
  }
  return [order, styleId, keyStyleText];
};