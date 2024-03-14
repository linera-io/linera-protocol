"use strict";
Object.defineProperty(exports, "__esModule", {
    value: true
});
function _export(target, all) {
    for(var name in all)Object.defineProperty(target, name, {
        enumerable: true,
        get: all[name]
    });
}
_export(exports, {
    IconProvider: function() {
        return IconProvider;
    },
    createFromIconfontCN: function() {
        return _IconFont.default;
    },
    default: function() {
        return _Icon.default;
    }
});
var _Context = /*#__PURE__*/ _interop_require_default(require("./components/Context"));
_export_star(require("./icons"), exports);
_export_star(require("./components/twoTonePrimaryColor"), exports);
var _IconFont = /*#__PURE__*/ _interop_require_default(require("./components/IconFont"));
var _Icon = /*#__PURE__*/ _interop_require_default(require("./components/Icon"));
function _export_star(from, to) {
    Object.keys(from).forEach(function(k) {
        if (k !== "default" && !Object.prototype.hasOwnProperty.call(to, k)) {
            Object.defineProperty(to, k, {
                enumerable: true,
                get: function() {
                    return from[k];
                }
            });
        }
    });
    return from;
}
function _interop_require_default(obj) {
    return obj && obj.__esModule ? obj : {
        default: obj
    };
}
var IconProvider = _Context.default.Provider;
