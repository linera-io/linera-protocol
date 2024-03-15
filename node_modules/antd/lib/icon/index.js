"use strict";
"use client";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _warning = require("../_util/warning");
const Icon = () => {
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Icon');
    process.env.NODE_ENV !== "production" ? warning(false, 'usage', 'Empty Icon') : void 0;
  }
  return null;
};
var _default = exports.default = Icon;