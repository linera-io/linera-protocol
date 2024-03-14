"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "Field", {
  enumerable: true,
  get: function get() {
    return _Field.default;
  }
});
Object.defineProperty(exports, "FieldContext", {
  enumerable: true,
  get: function get() {
    return _FieldContext.default;
  }
});
Object.defineProperty(exports, "FormProvider", {
  enumerable: true,
  get: function get() {
    return _FormContext.FormProvider;
  }
});
Object.defineProperty(exports, "List", {
  enumerable: true,
  get: function get() {
    return _List.default;
  }
});
Object.defineProperty(exports, "ListContext", {
  enumerable: true,
  get: function get() {
    return _ListContext.default;
  }
});
exports.default = void 0;
Object.defineProperty(exports, "useForm", {
  enumerable: true,
  get: function get() {
    return _useForm.default;
  }
});
Object.defineProperty(exports, "useWatch", {
  enumerable: true,
  get: function get() {
    return _useWatch.default;
  }
});
var React = _interopRequireWildcard(require("react"));
var _Field = _interopRequireDefault(require("./Field"));
var _List = _interopRequireDefault(require("./List"));
var _useForm = _interopRequireDefault(require("./useForm"));
var _Form = _interopRequireDefault(require("./Form"));
var _FormContext = require("./FormContext");
var _FieldContext = _interopRequireDefault(require("./FieldContext"));
var _ListContext = _interopRequireDefault(require("./ListContext"));
var _useWatch = _interopRequireDefault(require("./useWatch"));
var InternalForm = /*#__PURE__*/React.forwardRef(_Form.default);
var RefForm = InternalForm;
RefForm.FormProvider = _FormContext.FormProvider;
RefForm.Field = _Field.default;
RefForm.List = _List.default;
RefForm.useForm = _useForm.default;
RefForm.useWatch = _useWatch.default;
var _default = exports.default = RefForm;