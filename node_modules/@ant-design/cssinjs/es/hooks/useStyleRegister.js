import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import hash from '@emotion/hash';
import { removeCSS, updateCSS } from "rc-util/es/Dom/dynamicCSS";
import * as React from 'react';
// @ts-ignore
import unitless from '@emotion/unitless';
import { compile, serialize, stringify } from 'stylis';
import { contentQuotesLinter, hashedAnimationLinter } from "../linters";
import StyleContext, { ATTR_CACHE_PATH, ATTR_MARK, ATTR_TOKEN, CSS_IN_JS_INSTANCE } from "../StyleContext";
import { isClientSide, supportLayer, toStyleStr } from "../util";
import { CSS_FILE_STYLE, existPath, getStyleAndHash } from "../util/cacheMapUtil";
import useGlobalCache from "./useGlobalCache";
var SKIP_CHECK = '_skip_check_';
var MULTI_VALUE = '_multi_value_';
// ============================================================================
// ==                                 Parser                                 ==
// ============================================================================
// Preprocessor style content to browser support one
export function normalizeStyle(styleStr) {
  var serialized = serialize(compile(styleStr), stringify);
  return serialized.replace(/\{%%%\:[^;];}/g, ';');
}
function isCompoundCSSProperty(value) {
  return _typeof(value) === 'object' && value && (SKIP_CHECK in value || MULTI_VALUE in value);
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
    return [firstPath].concat(_toConsumableArray(fullPath.slice(1))).join(' ');
  });
  return keys.join(',');
}
// Parse CSSObject to style content
export var parseStyle = function parseStyle(interpolation) {
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
        _parseStyle2 = _slicedToArray(_parseStyle, 1),
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
        if (_typeof(value) === 'object' && value && (key !== 'animationName' || !value._keyframe) && !isCompoundCSSProperty(value)) {
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
              parentSelectors: [].concat(_toConsumableArray(parentSelectors), [mergedKey])
            }),
            _parseStyle4 = _slicedToArray(_parseStyle3, 2),
            _parsedStr2 = _parseStyle4[0],
            childEffectStyle = _parseStyle4[1];
          effectStyle = _objectSpread(_objectSpread({}, effectStyle), childEffectStyle);
          styleStr += "".concat(mergedKey).concat(_parsedStr2);
        } else {
          var _value;
          function appendStyle(cssKey, cssValue) {
            if (process.env.NODE_ENV !== 'production' && (_typeof(value) !== 'object' || !(value !== null && value !== void 0 && value[SKIP_CHECK]))) {
              [contentQuotesLinter, hashedAnimationLinter].concat(_toConsumableArray(linters)).forEach(function (linter) {
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
            if (!unitless[cssKey] && typeof formatValue === 'number' && formatValue !== 0) {
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
          if (_typeof(value) === 'object' && value !== null && value !== void 0 && value[MULTI_VALUE] && Array.isArray(actualValue)) {
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
  } else if (layer && supportLayer()) {
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
export function uniqueHash(path, styleStr) {
  return hash("".concat(path.join('%')).concat(styleStr));
}
function Empty() {
  return null;
}
export var STYLE_PREFIX = 'style';
/**
 * Register a style to the global style sheet.
 */
export default function useStyleRegister(info, styleFn) {
  var token = info.token,
    path = info.path,
    hashId = info.hashId,
    layer = info.layer,
    nonce = info.nonce,
    clientOnly = info.clientOnly,
    _info$order = info.order,
    order = _info$order === void 0 ? 0 : _info$order;
  var _React$useContext = React.useContext(StyleContext),
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
  var fullPath = [tokenKey].concat(_toConsumableArray(path));

  // Check if need insert style
  var isMergedClientSide = isClientSide;
  if (process.env.NODE_ENV !== 'production' && mock !== undefined) {
    isMergedClientSide = mock === 'client';
  }
  var _useGlobalCache = useGlobalCache(STYLE_PREFIX, fullPath,
    // Create cache if needed
    function () {
      var cachePath = fullPath.join('|');

      // Get style from SSR inline style directly
      if (existPath(cachePath)) {
        var _getStyleAndHash = getStyleAndHash(cachePath),
          _getStyleAndHash2 = _slicedToArray(_getStyleAndHash, 2),
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
        _parseStyle6 = _slicedToArray(_parseStyle5, 2),
        parsedStyle = _parseStyle6[0],
        effectStyle = _parseStyle6[1];
      var styleStr = normalizeStyle(parsedStyle);
      var styleId = uniqueHash(fullPath, styleStr);
      return [styleStr, tokenKey, styleId, effectStyle, clientOnly, order];
    },
    // Remove cache if no need
    function (_ref2, fromHMR) {
      var _ref3 = _slicedToArray(_ref2, 3),
        styleId = _ref3[2];
      if ((fromHMR || autoClear) && isClientSide) {
        removeCSS(styleId, {
          mark: ATTR_MARK
        });
      }
    },
    // Effect: Inject style here
    function (_ref4) {
      var _ref5 = _slicedToArray(_ref4, 4),
        styleStr = _ref5[0],
        _ = _ref5[1],
        styleId = _ref5[2],
        effectStyle = _ref5[3];
      if (isMergedClientSide && styleStr !== CSS_FILE_STYLE) {
        var mergedCSSConfig = {
          mark: ATTR_MARK,
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
        var style = updateCSS(styleStr, styleId, mergedCSSConfig);
        style[CSS_IN_JS_INSTANCE] = cache.instanceId;

        // Used for `useCacheToken` to remove on batch when token removed
        style.setAttribute(ATTR_TOKEN, tokenKey);

        // Debug usage. Dev only
        if (process.env.NODE_ENV !== 'production') {
          style.setAttribute(ATTR_CACHE_PATH, fullPath.join('|'));
        }

        // Inject client side effect style
        Object.keys(effectStyle).forEach(function (effectKey) {
          updateCSS(normalizeStyle(effectStyle[effectKey]), "_effect-".concat(effectKey), mergedCSSConfig);
        });
      }
    }),
    _useGlobalCache2 = _slicedToArray(_useGlobalCache, 3),
    cachedStyleStr = _useGlobalCache2[0],
    cachedTokenKey = _useGlobalCache2[1],
    cachedStyleId = _useGlobalCache2[2];
  return function (node) {
    var styleNode;
    if (!ssrInline || isMergedClientSide || !defaultCache) {
      styleNode = /*#__PURE__*/React.createElement(Empty, null);
    } else {
      var _ref6;
      styleNode = /*#__PURE__*/React.createElement("style", _extends({}, (_ref6 = {}, _defineProperty(_ref6, ATTR_TOKEN, cachedTokenKey), _defineProperty(_ref6, ATTR_MARK, cachedStyleId), _ref6), {
        dangerouslySetInnerHTML: {
          __html: cachedStyleStr
        }
      }));
    }
    return /*#__PURE__*/React.createElement(React.Fragment, null, styleNode, node);
  };
}
export var extract = function extract(cache, effectStyles, options) {
  var _cache = _slicedToArray(cache, 6),
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
  keyStyleText = toStyleStr(styleStr, tokenKey, styleId, sharedAttrs, plain);

  // =============== Create effect style ===============
  if (effectStyle) {
    Object.keys(effectStyle).forEach(function (effectKey) {
      // Effect style can be reused
      if (!effectStyles[effectKey]) {
        effectStyles[effectKey] = true;
        var effectStyleStr = normalizeStyle(effectStyle[effectKey]);
        keyStyleText += toStyleStr(effectStyleStr, tokenKey, "_effect-".concat(effectKey), sharedAttrs, plain);
      }
    });
  }
  return [order, styleId, keyStyleText];
};