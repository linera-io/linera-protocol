import { useForm as useRcForm } from 'rc-field-form';
import * as React from 'react';
import scrollIntoView from 'scroll-into-view-if-needed';
import { getFieldId, toArray } from '../util';
function toNamePathStr(name) {
  const namePath = toArray(name);
  return namePath.join('_');
}
export default function useForm(form) {
  const [rcForm] = useRcForm();
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
      const namePath = toArray(name);
      const fieldId = getFieldId(namePath, wrapForm.__INTERNAL__.name);
      const node = fieldId ? document.getElementById(fieldId) : null;
      if (node) {
        scrollIntoView(node, Object.assign({
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