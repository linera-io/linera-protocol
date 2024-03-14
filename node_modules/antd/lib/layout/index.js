"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _layout = _interopRequireWildcard(require("./layout"));
var _Sider = _interopRequireWildcard(require("./Sider"));
const Layout = _layout.default;
Layout.Header = _layout.Header;
Layout.Footer = _layout.Footer;
Layout.Content = _layout.Content;
Layout.Sider = _Sider.default;
Layout._InternalSiderContext = _Sider.SiderContext;
var _default = exports.default = Layout;