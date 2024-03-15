"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
Object.defineProperty(exports, "default", {
    enumerable: true,
    get: function() {
        return create;
    }
});
var _react = /*#__PURE__*/ _interop_require_wildcard(require("react"));
var _Icon = /*#__PURE__*/ _interop_require_default(require("./Icon"));
function _define_property(obj, key, value) {
    if (key in obj) {
        Object.defineProperty(obj, key, {
            value: value,
            enumerable: true,
            configurable: true,
            writable: true
        });
    } else {
        obj[key] = value;
    }
    return obj;
}
function _interop_require_default(obj) {
    return obj && obj.__esModule ? obj : {
        default: obj
    };
}
function _getRequireWildcardCache(nodeInterop) {
    if (typeof WeakMap !== "function") return null;
    var cacheBabelInterop = new WeakMap();
    var cacheNodeInterop = new WeakMap();
    return (_getRequireWildcardCache = function(nodeInterop) {
        return nodeInterop ? cacheNodeInterop : cacheBabelInterop;
    })(nodeInterop);
}
function _interop_require_wildcard(obj, nodeInterop) {
    if (!nodeInterop && obj && obj.__esModule) {
        return obj;
    }
    if (obj === null || typeof obj !== "object" && typeof obj !== "function") {
        return {
            default: obj
        };
    }
    var cache = _getRequireWildcardCache(nodeInterop);
    if (cache && cache.has(obj)) {
        return cache.get(obj);
    }
    var newObj = {
        __proto__: null
    };
    var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;
    for(var key in obj){
        if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {
            var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;
            if (desc && (desc.get || desc.set)) {
                Object.defineProperty(newObj, key, desc);
            } else {
                newObj[key] = obj[key];
            }
        }
    }
    newObj.default = obj;
    if (cache) {
        cache.set(obj, newObj);
    }
    return newObj;
}
function _object_spread(target) {
    for(var i = 1; i < arguments.length; i++){
        var source = arguments[i] != null ? arguments[i] : {};
        var ownKeys = Object.keys(source);
        if (typeof Object.getOwnPropertySymbols === "function") {
            ownKeys = ownKeys.concat(Object.getOwnPropertySymbols(source).filter(function(sym) {
                return Object.getOwnPropertyDescriptor(source, sym).enumerable;
            }));
        }
        ownKeys.forEach(function(key) {
            _define_property(target, key, source[key]);
        });
    }
    return target;
}
function ownKeys(object, enumerableOnly) {
    var keys = Object.keys(object);
    if (Object.getOwnPropertySymbols) {
        var symbols = Object.getOwnPropertySymbols(object);
        if (enumerableOnly) {
            symbols = symbols.filter(function(sym) {
                return Object.getOwnPropertyDescriptor(object, sym).enumerable;
            });
        }
        keys.push.apply(keys, symbols);
    }
    return keys;
}
function _object_spread_props(target, source) {
    source = source != null ? source : {};
    if (Object.getOwnPropertyDescriptors) {
        Object.defineProperties(target, Object.getOwnPropertyDescriptors(source));
    } else {
        ownKeys(Object(source)).forEach(function(key) {
            Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key));
        });
    }
    return target;
}
function _object_without_properties(source, excluded) {
    if (source == null) return {};
    var target = _object_without_properties_loose(source, excluded);
    var key, i;
    if (Object.getOwnPropertySymbols) {
        var sourceSymbolKeys = Object.getOwnPropertySymbols(source);
        for(i = 0; i < sourceSymbolKeys.length; i++){
            key = sourceSymbolKeys[i];
            if (excluded.indexOf(key) >= 0) continue;
            if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue;
            target[key] = source[key];
        }
    }
    return target;
}
function _object_without_properties_loose(source, excluded) {
    if (source == null) return {};
    var target = {};
    var sourceKeys = Object.keys(source);
    var key, i;
    for(i = 0; i < sourceKeys.length; i++){
        key = sourceKeys[i];
        if (excluded.indexOf(key) >= 0) continue;
        target[key] = source[key];
    }
    return target;
}
var customCache = new Set();
function isValidCustomScriptUrl(scriptUrl) {
    return Boolean(typeof scriptUrl === "string" && scriptUrl.length && !customCache.has(scriptUrl));
}
function createScriptUrlElements(scriptUrls) {
    var index = arguments.length > 1 && arguments[1] !== void 0 ? arguments[1] : 0;
    var currentScriptUrl = scriptUrls[index];
    if (isValidCustomScriptUrl(currentScriptUrl)) {
        var script = document.createElement("script");
        script.setAttribute("src", currentScriptUrl);
        script.setAttribute("data-namespace", currentScriptUrl);
        if (scriptUrls.length > index + 1) {
            script.onload = function() {
                createScriptUrlElements(scriptUrls, index + 1);
            };
            script.onerror = function() {
                createScriptUrlElements(scriptUrls, index + 1);
            };
        }
        customCache.add(currentScriptUrl);
        document.body.appendChild(script);
    }
}
function create() {
    var options = arguments.length > 0 && arguments[0] !== void 0 ? arguments[0] : {};
    var scriptUrl = options.scriptUrl, _options_extraCommonProps = options.extraCommonProps, extraCommonProps = _options_extraCommonProps === void 0 ? {} : _options_extraCommonProps;
    /**
   * DOM API required.
   * Make sure in browser environment.
   * The Custom Icon will create a <script/>
   * that loads SVG symbols and insert the SVG Element into the document body.
   */ if (scriptUrl && typeof document !== "undefined" && typeof window !== "undefined" && typeof document.createElement === "function") {
        if (Array.isArray(scriptUrl)) {
            // 因为iconfont资源会把svg插入before，所以前加载相同type会覆盖后加载，为了数组覆盖顺序，倒叙插入
            createScriptUrlElements(scriptUrl.reverse());
        } else {
            createScriptUrlElements([
                scriptUrl
            ]);
        }
    }
    var Iconfont = /*#__PURE__*/ _react.forwardRef(function(props, ref) {
        var type = props.type, children = props.children, restProps = _object_without_properties(props, [
            "type",
            "children"
        ]);
        // children > type
        var content = null;
        if (props.type) {
            content = /*#__PURE__*/ _react.createElement("use", {
                xlinkHref: "#".concat(type)
            });
        }
        if (children) {
            content = children;
        }
        return /*#__PURE__*/ _react.createElement(_Icon.default, _object_spread_props(_object_spread({}, extraCommonProps, restProps), {
            ref: ref
        }), content);
    });
    Iconfont.displayName = "Iconfont";
    return Iconfont;
}
