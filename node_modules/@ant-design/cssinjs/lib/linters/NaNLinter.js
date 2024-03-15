"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _utils = require("./utils");
var linter = function linter(key, value, info) {
  if (typeof value === 'string' && /NaN/g.test(value) || Number.isNaN(value)) {
    (0, _utils.lintWarning)("Unexpected 'NaN' in property '".concat(key, ": ").concat(value, "'."), info);
  }
};
var _default = exports.default = linter;