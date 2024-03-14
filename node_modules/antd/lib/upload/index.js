"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Dragger = _interopRequireDefault(require("./Dragger"));
var _Upload = _interopRequireWildcard(require("./Upload"));
const Upload = _Upload.default;
Upload.Dragger = _Dragger.default;
Upload.LIST_IGNORE = _Upload.LIST_IGNORE;
var _default = exports.default = Upload;