"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _colors = require("./colors");
Object.keys(_colors).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _colors[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _colors[key];
    }
  });
});
var _font = require("./font");
Object.keys(_font).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _font[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _font[key];
    }
  });
});
var _size = require("./size");
Object.keys(_size).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _size[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _size[key];
    }
  });
});
var _style = require("./style");
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