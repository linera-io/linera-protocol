"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "createContext", {
  enumerable: true,
  get: function get() {
    return _context.createContext;
  }
});
Object.defineProperty(exports, "createImmutable", {
  enumerable: true,
  get: function get() {
    return _Immutable.default;
  }
});
exports.responseImmutable = exports.makeImmutable = void 0;
Object.defineProperty(exports, "useContext", {
  enumerable: true,
  get: function get() {
    return _context.useContext;
  }
});
exports.useImmutableMark = void 0;
var _context = require("./context");
var _Immutable = _interopRequireDefault(require("./Immutable"));
// For legacy usage, we export it directly
var _createImmutable = (0, _Immutable.default)(),
  makeImmutable = _createImmutable.makeImmutable,
  responseImmutable = _createImmutable.responseImmutable,
  useImmutableMark = _createImmutable.useImmutableMark;
exports.useImmutableMark = useImmutableMark;
exports.responseImmutable = responseImmutable;
exports.makeImmutable = makeImmutable;