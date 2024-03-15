"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.InternalBreadcrumbItem = void 0;
var _DownOutlined = _interopRequireDefault(require("@ant-design/icons/DownOutlined"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _dropdown = _interopRequireDefault(require("../dropdown/dropdown"));
var _BreadcrumbSeparator = _interopRequireDefault(require("./BreadcrumbSeparator"));
var _useItemRender = require("./useItemRender");
var _warning = require("../_util/warning");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const InternalBreadcrumbItem = props => {
  const {
    prefixCls,
    separator = '/',
    children,
    menu,
    overlay,
    dropdownProps,
    href
  } = props;
  // Warning for deprecated usage
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Breadcrumb.Item');
    warning.deprecated(!('overlay' in props), 'overlay', 'menu');
  }
  /** If overlay is have Wrap a Dropdown */
  const renderBreadcrumbNode = breadcrumbItem => {
    if (menu || overlay) {
      const mergeDropDownProps = Object.assign({}, dropdownProps);
      if (menu) {
        const _a = menu || {},
          {
            items
          } = _a,
          menuProps = __rest(_a, ["items"]);
        mergeDropDownProps.menu = Object.assign(Object.assign({}, menuProps), {
          items: items === null || items === void 0 ? void 0 : items.map((_a, index) => {
            var {
                key,
                title,
                label,
                path
              } = _a,
              itemProps = __rest(_a, ["key", "title", "label", "path"]);
            let mergedLabel = label !== null && label !== void 0 ? label : title;
            if (path) {
              mergedLabel = /*#__PURE__*/React.createElement("a", {
                href: `${href}${path}`
              }, mergedLabel);
            }
            return Object.assign(Object.assign({}, itemProps), {
              key: key !== null && key !== void 0 ? key : index,
              label: mergedLabel
            });
          })
        });
      } else if (overlay) {
        mergeDropDownProps.overlay = overlay;
      }
      return /*#__PURE__*/React.createElement(_dropdown.default, Object.assign({
        placement: "bottom"
      }, mergeDropDownProps), /*#__PURE__*/React.createElement("span", {
        className: `${prefixCls}-overlay-link`
      }, breadcrumbItem, /*#__PURE__*/React.createElement(_DownOutlined.default, null)));
    }
    return breadcrumbItem;
  };
  // wrap to dropDown
  const link = renderBreadcrumbNode(children);
  if (link !== undefined && link !== null) {
    return /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("li", null, link), separator && /*#__PURE__*/React.createElement(_BreadcrumbSeparator.default, null, separator));
  }
  return null;
};
exports.InternalBreadcrumbItem = InternalBreadcrumbItem;
const BreadcrumbItem = props => {
  const {
      prefixCls: customizePrefixCls,
      children,
      href
    } = props,
    restProps = __rest(props, ["prefixCls", "children", "href"]);
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('breadcrumb', customizePrefixCls);
  return /*#__PURE__*/React.createElement(InternalBreadcrumbItem, Object.assign({}, restProps, {
    prefixCls: prefixCls
  }), (0, _useItemRender.renderItem)(prefixCls, restProps, children, href));
};
BreadcrumbItem.__ANT_BREADCRUMB_ITEM = true;
var _default = exports.default = BreadcrumbItem;