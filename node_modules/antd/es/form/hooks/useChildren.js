import toArray from "rc-util/es/Children/toArray";
export default function useChildren(children) {
  if (typeof children === 'function') {
    return children;
  }
  const childList = toArray(children);
  return childList.length <= 1 ? childList[0] : childList;
}