"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = require("react");
var _warning = require("../../_util/warning");
var _context = require("../context");
const useFormItemStatus = () => {
  const {
    status,
    errors = [],
    warnings = []
  } = (0, _react.useContext)(_context.FormItemInputContext);
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Form.Item');
    process.env.NODE_ENV !== "production" ? warning(status !== undefined, 'usage', 'Form.Item.useStatus should be used under Form.Item component. For more information: https://u.ant.design/form-item-usestatus') : void 0;
  }
  return {
    status,
    errors,
    warnings
  };
};
// Only used for compatible package. Not promise this will work on future version.
useFormItemStatus.Context = _context.FormItemInputContext;
var _default = exports.default = useFormItemStatus;