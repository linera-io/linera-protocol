"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useFormWarning;
var _react = require("react");
var _warning = require("../../_util/warning");
const names = {};
function useFormWarning(_ref) {
  let {
    name
  } = _ref;
  const warning = (0, _warning.devUseWarning)('Form');
  (0, _react.useEffect)(() => {
    if (name) {
      names[name] = (names[name] || 0) + 1;
      process.env.NODE_ENV !== "production" ? warning(names[name] <= 1, 'usage', 'There exist multiple Form with same `name`.') : void 0;
      return () => {
        names[name] -= 1;
      };
    }
  }, [name]);
}