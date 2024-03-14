"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useImmutableMark = exports.responseImmutable = exports.makeImmutable = exports.default = void 0;
var _context = require("@rc-component/context");
var _createImmutable = (0, _context.createImmutable)(),
  makeImmutable = exports.makeImmutable = _createImmutable.makeImmutable,
  responseImmutable = exports.responseImmutable = _createImmutable.responseImmutable,
  useImmutableMark = exports.useImmutableMark = _createImmutable.useImmutableMark;
var TableContext = (0, _context.createContext)();
var _default = exports.default = TableContext;