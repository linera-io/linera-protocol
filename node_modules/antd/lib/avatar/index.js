"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Group", {
  enumerable: true,
  get: function () {
    return _group.default;
  }
});
exports.default = void 0;
var _avatar = _interopRequireDefault(require("./avatar"));
var _group = _interopRequireDefault(require("./group"));
const Avatar = _avatar.default;
Avatar.Group = _group.default;
var _default = exports.default = Avatar;