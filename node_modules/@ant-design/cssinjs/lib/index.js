"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Keyframes", {
  enumerable: true,
  get: function get() {
    return _Keyframes.default;
  }
});
Object.defineProperty(exports, "NaNLinter", {
  enumerable: true,
  get: function get() {
    return _linters.NaNLinter;
  }
});
Object.defineProperty(exports, "StyleProvider", {
  enumerable: true,
  get: function get() {
    return _StyleContext.StyleProvider;
  }
});
Object.defineProperty(exports, "Theme", {
  enumerable: true,
  get: function get() {
    return _theme.Theme;
  }
});
exports._experimental = void 0;
Object.defineProperty(exports, "createCache", {
  enumerable: true,
  get: function get() {
    return _StyleContext.createCache;
  }
});
Object.defineProperty(exports, "createTheme", {
  enumerable: true,
  get: function get() {
    return _theme.createTheme;
  }
});
Object.defineProperty(exports, "extractStyle", {
  enumerable: true,
  get: function get() {
    return _extractStyle.default;
  }
});
Object.defineProperty(exports, "getComputedToken", {
  enumerable: true,
  get: function get() {
    return _useCacheToken.getComputedToken;
  }
});
Object.defineProperty(exports, "legacyLogicalPropertiesTransformer", {
  enumerable: true,
  get: function get() {
    return _legacyLogicalProperties.default;
  }
});
Object.defineProperty(exports, "legacyNotSelectorLinter", {
  enumerable: true,
  get: function get() {
    return _linters.legacyNotSelectorLinter;
  }
});
Object.defineProperty(exports, "logicalPropertiesLinter", {
  enumerable: true,
  get: function get() {
    return _linters.logicalPropertiesLinter;
  }
});
Object.defineProperty(exports, "parentSelectorLinter", {
  enumerable: true,
  get: function get() {
    return _linters.parentSelectorLinter;
  }
});
Object.defineProperty(exports, "px2remTransformer", {
  enumerable: true,
  get: function get() {
    return _px2rem.default;
  }
});
Object.defineProperty(exports, "token2CSSVar", {
  enumerable: true,
  get: function get() {
    return _cssVariables.token2CSSVar;
  }
});
Object.defineProperty(exports, "unit", {
  enumerable: true,
  get: function get() {
    return _util.unit;
  }
});
Object.defineProperty(exports, "useCSSVarRegister", {
  enumerable: true,
  get: function get() {
    return _useCSSVarRegister.default;
  }
});
Object.defineProperty(exports, "useCacheToken", {
  enumerable: true,
  get: function get() {
    return _useCacheToken.default;
  }
});
Object.defineProperty(exports, "useStyleRegister", {
  enumerable: true,
  get: function get() {
    return _useStyleRegister.default;
  }
});
var _extractStyle = _interopRequireDefault(require("./extractStyle"));
var _useCacheToken = _interopRequireWildcard(require("./hooks/useCacheToken"));
var _useCSSVarRegister = _interopRequireDefault(require("./hooks/useCSSVarRegister"));
var _useStyleRegister = _interopRequireDefault(require("./hooks/useStyleRegister"));
var _Keyframes = _interopRequireDefault(require("./Keyframes"));
var _linters = require("./linters");
var _StyleContext = require("./StyleContext");
var _theme = require("./theme");
var _legacyLogicalProperties = _interopRequireDefault(require("./transformers/legacyLogicalProperties"));
var _px2rem = _interopRequireDefault(require("./transformers/px2rem"));
var _util = require("./util");
var _cssVariables = require("./util/css-variables");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var _experimental = exports._experimental = {
  supportModernCSS: function supportModernCSS() {
    return (0, _util.supportWhere)() && (0, _util.supportLogicProps)();
  }
};