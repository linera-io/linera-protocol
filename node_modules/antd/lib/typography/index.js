"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _Link = _interopRequireDefault(require("./Link"));
var _Paragraph = _interopRequireDefault(require("./Paragraph"));
var _Text = _interopRequireDefault(require("./Text"));
var _Title = _interopRequireDefault(require("./Title"));
var _Typography = _interopRequireDefault(require("./Typography"));
const Typography = _Typography.default;
Typography.Text = _Text.default;
Typography.Link = _Link.default;
Typography.Title = _Title.default;
Typography.Paragraph = _Paragraph.default;
var _default = exports.default = Typography;