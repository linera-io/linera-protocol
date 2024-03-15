"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = createTheme;
var _ThemeCache = _interopRequireDefault(require("./ThemeCache"));
var _Theme = _interopRequireDefault(require("./Theme"));
var cacheThemes = new _ThemeCache.default();

/**
 * Same as new Theme, but will always return same one if `derivative` not changed.
 */
function createTheme(derivatives) {
  var derivativeArr = Array.isArray(derivatives) ? derivatives : [derivatives];
  // Create new theme if not exist
  if (!cacheThemes.has(derivativeArr)) {
    cacheThemes.set(derivativeArr, new _Theme.default(derivativeArr));
  }

  // Get theme from cache and return
  return cacheThemes.get(derivativeArr);
}