"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useFormInstance;
var _react = require("react");
var _context = require("../context");
function useFormInstance() {
  const {
    form
  } = (0, _react.useContext)(_context.FormContext);
  return form;
}