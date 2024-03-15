"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _warning = _interopRequireDefault(require("../_util/warning"));
var _ErrorList = _interopRequireDefault(require("./ErrorList"));
var _Form = _interopRequireWildcard(require("./Form"));
var _FormItem = _interopRequireDefault(require("./FormItem"));
var _FormList = _interopRequireDefault(require("./FormList"));
var _context = require("./context");
var _useFormInstance = _interopRequireDefault(require("./hooks/useFormInstance"));
const Form = _Form.default;
Form.Item = _FormItem.default;
Form.List = _FormList.default;
Form.ErrorList = _ErrorList.default;
Form.useForm = _Form.useForm;
Form.useFormInstance = _useFormInstance.default;
Form.useWatch = _Form.useWatch;
Form.Provider = _context.FormProvider;
Form.create = () => {
  process.env.NODE_ENV !== "production" ? (0, _warning.default)(false, 'Form', 'antd v4 removed `Form.create`. Please remove or use `@ant-design/compatible` instead.') : void 0;
};
var _default = exports.default = Form;