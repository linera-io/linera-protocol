var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import toArray from "rc-util/es/Children/toArray";
import { matchScreen } from '../../_util/responsiveObserver';
// Convert children into items
const transChildren2Items = childNodes => toArray(childNodes).map(node => Object.assign(Object.assign({}, node === null || node === void 0 ? void 0 : node.props), {
  key: node.key
}));
export default function useItems(screens, items, children) {
  const mergedItems = React.useMemo(() =>
  // Take `items` first or convert `children` into items
  items || transChildren2Items(children), [items, children]);
  const responsiveItems = React.useMemo(() => mergedItems.map(_a => {
    var {
        span
      } = _a,
      restItem = __rest(_a, ["span"]);
    return Object.assign(Object.assign({}, restItem), {
      span: typeof span === 'number' ? span : matchScreen(screens, span)
    });
  }), [mergedItems, screens]);
  return responsiveItems;
}