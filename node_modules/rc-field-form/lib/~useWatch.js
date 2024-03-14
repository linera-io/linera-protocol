"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
exports.stringify = stringify;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _FieldContext = _interopRequireWildcard(require("./FieldContext"));
var _react = require("react");
var _valueUtil = require("./utils/valueUtil");
var _typeUtil = require("./utils/typeUtil");
function stringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return Math.random();
  }
}
var useWatchWarning = process.env.NODE_ENV !== 'production' ? function (namePath) {
  var fullyStr = namePath.join('__RC_FIELD_FORM_SPLIT__');
  var nameStrRef = (0, _react.useRef)(fullyStr);
  (0, _warning.default)(nameStrRef.current === fullyStr, '`useWatch` is not support dynamic `namePath`. Please provide static instead.');
} : function () {};
function useWatch() {
  for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
    args[_key] = arguments[_key];
  }
  var _args$ = args[0],
    dependencies = _args$ === void 0 ? [] : _args$,
    _args$2 = args[1],
    _form = _args$2 === void 0 ? {} : _args$2;
  var options = (0, _typeUtil.isFormInstance)(_form) ? {
    form: _form
  } : _form;
  var form = options.form;
  var _useState = (0, _react.useState)(),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    value = _useState2[0],
    setValue = _useState2[1];
  var valueStr = (0, _react.useMemo)(function () {
    return stringify(value);
  }, [value]);
  var valueStrRef = (0, _react.useRef)(valueStr);
  valueStrRef.current = valueStr;
  var fieldContext = (0, _react.useContext)(_FieldContext.default);
  var formInstance = form || fieldContext;
  var isValidForm = formInstance && formInstance._init;

  // Warning if not exist form instance
  if (process.env.NODE_ENV !== 'production') {
    (0, _warning.default)(args.length === 2 ? form ? isValidForm : true : isValidForm, 'useWatch requires a form instance since it can not auto detect from context.');
  }
  var namePath = (0, _valueUtil.getNamePath)(dependencies);
  var namePathRef = (0, _react.useRef)(namePath);
  namePathRef.current = namePath;
  useWatchWarning(namePath);
  (0, _react.useEffect)(function () {
    // Skip if not exist form instance
    if (!isValidForm) {
      return;
    }
    var getFieldsValue = formInstance.getFieldsValue,
      getInternalHooks = formInstance.getInternalHooks;
    var _getInternalHooks = getInternalHooks(_FieldContext.HOOK_MARK),
      registerWatch = _getInternalHooks.registerWatch;
    var cancelRegister = registerWatch(function (values, allValues) {
      var newValue = (0, _valueUtil.getValue)(options.preserve ? allValues : values, namePathRef.current);
      var nextValueStr = stringify(newValue);

      // Compare stringify in case it's nest object
      if (valueStrRef.current !== nextValueStr) {
        valueStrRef.current = nextValueStr;
        setValue(newValue);
      }
    });

    // TODO: We can improve this perf in future
    var initialValue = (0, _valueUtil.getValue)(options.preserve ? getFieldsValue(true) : getFieldsValue(), namePathRef.current);
    setValue(initialValue);
    return cancelRegister;
  },
  // We do not need re-register since namePath content is the same
  // eslint-disable-next-line react-hooks/exhaustive-deps
  [isValidForm]);
  return value;
}
var _default = exports.default = useWatch;