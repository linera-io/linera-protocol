"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useForm;
var _rcFieldForm = require("rc-field-form");
var React = _interopRequireWildcard(require("react"));
var _scrollIntoViewIfNeeded = _interopRequireDefault(require("scroll-into-view-if-needed"));
var _util = require("../util");
function toNamePathStr(name) {
  const namePath = (0, _util.toArray)(name);
  return namePath.join('_');
}
function useForm(form) {
  const [rcForm] = (0, _rcFieldForm.useForm)();
  const itemsRef = React.useRef({});
  const wrapForm = React.useMemo(() => form !== null && form !== void 0 ? form : Object.assign(Object.assign({}, rcForm), {
    __INTERNAL__: {
      itemRef: name => node => {
        const namePathStr = toNamePathStr(name);
        if (node) {
          itemsRef.current[namePathStr] = node;
        } else {
          delete itemsRef.current[namePathStr];
        }
      }
    },
    scrollToField: function (name) {
      let options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      const namePath = (0, _util.toArray)(name);
      const fieldId = (0, _util.getFieldId)(namePath, wrapForm.__INTERNAL__.name);
      const node = fieldId ? document.getElementById(fieldId) : null;
      if (node) {
        (0, _scrollIntoViewIfNeeded.default)(node, Object.assign({
          scrollMode: 'if-needed',
          block: 'nearest'
        }, options));
      }
    },
    getFieldInstance: name => {
      const namePathStr = toNamePathStr(name);
      return itemsRef.current[namePathStr];
    }
  }), [form, rcForm]);
  return [wrapForm];
}