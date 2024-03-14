"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useRegisterImage;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _context = require("../context");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var uid = 0;
function useRegisterImage(canPreview, data) {
  var _React$useState = React.useState(function () {
      uid += 1;
      return String(uid);
    }),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 1),
    id = _React$useState2[0];
  var groupContext = React.useContext(_context.PreviewGroupContext);
  var registerData = {
    data: data,
    canPreview: canPreview
  };

  // Keep order start
  // Resolve https://github.com/ant-design/ant-design/issues/28881
  // Only need unRegister when component unMount
  React.useEffect(function () {
    if (groupContext) {
      return groupContext.register(id, registerData);
    }
  }, []);
  React.useEffect(function () {
    if (groupContext) {
      groupContext.register(id, registerData);
    }
  }, [canPreview, data]);
  return id;
}