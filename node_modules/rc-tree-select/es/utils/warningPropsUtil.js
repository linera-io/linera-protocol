import _typeof from "@babel/runtime/helpers/esm/typeof";
import warning from "rc-util/es/warning";
import { toArray } from "./valueUtil";
function warningProps(props) {
  var searchPlaceholder = props.searchPlaceholder,
    treeCheckStrictly = props.treeCheckStrictly,
    treeCheckable = props.treeCheckable,
    labelInValue = props.labelInValue,
    value = props.value,
    multiple = props.multiple;
  warning(!searchPlaceholder, '`searchPlaceholder` has been removed.');
  if (treeCheckStrictly && labelInValue === false) {
    warning(false, '`treeCheckStrictly` will force set `labelInValue` to `true`.');
  }
  if (labelInValue || treeCheckStrictly) {
    warning(toArray(value).every(function (val) {
      return val && _typeof(val) === 'object' && 'value' in val;
    }), 'Invalid prop `value` supplied to `TreeSelect`. You should use { label: string, value: string | number } or [{ label: string, value: string | number }] instead.');
  }
  if (treeCheckStrictly || multiple || treeCheckable) {
    warning(!value || Array.isArray(value), '`value` should be an array when `TreeSelect` is checkable or multiple.');
  } else {
    warning(!Array.isArray(value), '`value` should not be array when `TreeSelect` is single mode.');
  }
}
export default warningProps;