var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import { useMemo } from 'react';
function route2item(route) {
  const {
      breadcrumbName,
      children
    } = route,
    rest = __rest(route, ["breadcrumbName", "children"]);
  const clone = Object.assign({
    title: breadcrumbName
  }, rest);
  if (children) {
    clone.menu = {
      items: children.map(_a => {
        var {
            breadcrumbName: itemBreadcrumbName
          } = _a,
          itemProps = __rest(_a, ["breadcrumbName"]);
        return Object.assign(Object.assign({}, itemProps), {
          title: itemBreadcrumbName
        });
      })
    };
  }
  return clone;
}
export default function useItems(items, routes) {
  return useMemo(() => {
    if (items) {
      return items;
    }
    if (routes) {
      return routes.map(route2item);
    }
    return null;
  }, [items, routes]);
}