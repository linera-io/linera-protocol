"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useEffectState;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = require("react");
/**
 * Trigger a callback on state change
 */
function useEffectState() {
  var _useState = (0, _react.useState)({
      id: 0,
      callback: null
    }),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    effectId = _useState2[0],
    setEffectId = _useState2[1];
  var update = (0, _react.useCallback)(function (callback) {
    setEffectId(function (_ref) {
      var id = _ref.id;
      return {
        id: id + 1,
        callback: callback
      };
    });
  }, []);
  (0, _react.useEffect)(function () {
    var _effectId$callback;
    (_effectId$callback = effectId.callback) === null || _effectId$callback === void 0 || _effectId$callback.call(effectId);
  }, [effectId]);
  return update;
}