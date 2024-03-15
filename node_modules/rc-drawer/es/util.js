import warning from "rc-util/es/warning";
import canUseDom from "rc-util/es/Dom/canUseDom";
export function parseWidthHeight(value) {
  if (typeof value === 'string' && String(Number(value)) === value) {
    warning(false, 'Invalid value type of `width` or `height` which should be number type instead.');
    return Number(value);
  }
  return value;
}
export function warnCheck(props) {
  warning(!('wrapperClassName' in props), "'wrapperClassName' is removed. Please use 'rootClassName' instead.");
  warning(canUseDom() || !props.open, "Drawer with 'open' in SSR is not work since no place to createPortal. Please move to 'useEffect' instead.");
}