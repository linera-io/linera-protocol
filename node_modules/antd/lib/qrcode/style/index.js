"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _style = require("../../qr-code/style");
Object.keys(_style).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _style[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _style[key];
    }
  });
});