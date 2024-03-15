"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _utils = require("./utils");
var linter = function linter(key, value, info) {
  if (info.parentSelectors.some(function (selector) {
    var selectors = selector.split(',');
    return selectors.some(function (item) {
      return item.split('&').length > 2;
    });
  })) {
    (0, _utils.lintWarning)('Should not use more than one `&` in a selector.', info);
  }
};
var _default = exports.default = linter;