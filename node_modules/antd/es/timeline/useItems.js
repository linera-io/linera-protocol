import toArray from "rc-util/es/Children/toArray";
function useItems(items, children) {
  if (items && Array.isArray(items)) {
    return items;
  }
  return toArray(children).map(ele => {
    var _a, _b;
    return Object.assign({
      children: (_b = (_a = ele === null || ele === void 0 ? void 0 : ele.props) === null || _a === void 0 ? void 0 : _a.children) !== null && _b !== void 0 ? _b : ''
    }, ele.props);
  });
}
export default useItems;