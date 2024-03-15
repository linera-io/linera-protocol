"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.renderIconDefinitionToSVGElement = void 0;
var defaultColors = {
    primaryColor: '#333',
    secondaryColor: '#E6E6E6'
};
function renderIconDefinitionToSVGElement(icond, options) {
    if (options === void 0) { options = {}; }
    if (typeof icond.icon === 'function') {
        // two-tone
        var placeholders = options.placeholders || defaultColors;
        return renderAbstractNodeToSVGElement(icond.icon(placeholders.primaryColor, placeholders.secondaryColor), options);
    }
    // fill, outline
    return renderAbstractNodeToSVGElement(icond.icon, options);
}
exports.renderIconDefinitionToSVGElement = renderIconDefinitionToSVGElement;
function renderAbstractNodeToSVGElement(node, options) {
    var targetAttrs = node.tag === 'svg'
        ? __assign(__assign({}, node.attrs), (options.extraSVGAttrs || {})) : node.attrs;
    var attrs = Object.keys(targetAttrs).reduce(function (acc, nextKey) {
        var key = nextKey;
        var value = targetAttrs[key];
        var token = "".concat(key, "=\"").concat(value, "\"");
        acc.push(token);
        return acc;
    }, []);
    var attrsToken = attrs.length ? ' ' + attrs.join(' ') : '';
    var children = (node.children || [])
        .map(function (child) { return renderAbstractNodeToSVGElement(child, options); })
        .join('');
    if (children && children.length) {
        return "<".concat(node.tag).concat(attrsToken, ">").concat(children, "</").concat(node.tag, ">");
    }
    return "<".concat(node.tag).concat(attrsToken, " />");
}
