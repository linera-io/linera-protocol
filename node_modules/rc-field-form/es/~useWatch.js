import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import warning from "rc-util/es/warning";
import FieldContext, { HOOK_MARK } from "./FieldContext";
import { useState, useContext, useEffect, useRef, useMemo } from 'react';
import { getNamePath, getValue } from "./utils/valueUtil";
import { isFormInstance } from "./utils/typeUtil";
export function stringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return Math.random();
  }
}
var useWatchWarning = process.env.NODE_ENV !== 'production' ? function (namePath) {
  var fullyStr = namePath.join('__RC_FIELD_FORM_SPLIT__');
  var nameStrRef = useRef(fullyStr);
  warning(nameStrRef.current === fullyStr, '`useWatch` is not support dynamic `namePath`. Please provide static instead.');
} : function () {};
function useWatch() {
  for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
    args[_key] = arguments[_key];
  }
  var _args$ = args[0],
    dependencies = _args$ === void 0 ? [] : _args$,
    _args$2 = args[1],
    _form = _args$2 === void 0 ? {} : _args$2;
  var options = isFormInstance(_form) ? {
    form: _form
  } : _form;
  var form = options.form;
  var _useState = useState(),
    _useState2 = _slicedToArray(_useState, 2),
    value = _useState2[0],
    setValue = _useState2[1];
  var valueStr = useMemo(function () {
    return stringify(value);
  }, [value]);
  var valueStrRef = useRef(valueStr);
  valueStrRef.current = valueStr;
  var fieldContext = useContext(FieldContext);
  var formInstance = form || fieldContext;
  var isValidForm = formInstance && formInstance._init;

  // Warning if not exist form instance
  if (process.env.NODE_ENV !== 'production') {
    warning(args.length === 2 ? form ? isValidForm : true : isValidForm, 'useWatch requires a form instance since it can not auto detect from context.');
  }
  var namePath = getNamePath(dependencies);
  var namePathRef = useRef(namePath);
  namePathRef.current = namePath;
  useWatchWarning(namePath);
  useEffect(function () {
    // Skip if not exist form instance
    if (!isValidForm) {
      return;
    }
    var getFieldsValue = formInstance.getFieldsValue,
      getInternalHooks = formInstance.getInternalHooks;
    var _getInternalHooks = getInternalHooks(HOOK_MARK),
      registerWatch = _getInternalHooks.registerWatch;
    var cancelRegister = registerWatch(function (values, allValues) {
      var newValue = getValue(options.preserve ? allValues : values, namePathRef.current);
      var nextValueStr = stringify(newValue);

      // Compare stringify in case it's nest object
      if (valueStrRef.current !== nextValueStr) {
        valueStrRef.current = nextValueStr;
        setValue(newValue);
      }
    });

    // TODO: We can improve this perf in future
    var initialValue = getValue(options.preserve ? getFieldsValue(true) : getFieldsValue(), namePathRef.current);
    setValue(initialValue);
    return cancelRegister;
  },
  // We do not need re-register since namePath content is the same
  // eslint-disable-next-line react-hooks/exhaustive-deps
  [isValidForm]);
  return value;
}
export default useWatch;