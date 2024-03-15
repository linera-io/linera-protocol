"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Anchor = _interopRequireDefault(require("./Anchor"));
var _AnchorLink = _interopRequireDefault(require("./AnchorLink"));
const Anchor = _Anchor.default;
Anchor.Link = _AnchorLink.default;
var _default = exports.default = Anchor;