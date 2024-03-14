export default function useInputReadOnly(formatList, inputReadOnly, multiple) {
  if (typeof formatList[0] === 'function' || multiple) {
    return true;
  }
  return inputReadOnly;
}