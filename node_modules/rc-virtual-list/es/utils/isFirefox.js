import _typeof from "@babel/runtime/helpers/esm/typeof";
var isFF = (typeof navigator === "undefined" ? "undefined" : _typeof(navigator)) === 'object' && /Firefox/i.test(navigator.userAgent);
export default isFF;