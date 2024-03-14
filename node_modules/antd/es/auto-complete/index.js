"use client";

import * as React from 'react';
import classNames from 'classnames';
import toArray from "rc-util/es/Children/toArray";
import omit from "rc-util/es/omit";
import { useZIndex } from '../_util/hooks/useZIndex';
import genPurePanel from '../_util/PurePanel';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import Select from '../select';
const {
  Option
} = Select;
function isSelectOptionOrSelectOptGroup(child) {
  return child && child.type && (child.type.isSelectOption || child.type.isSelectOptGroup);
}
const AutoComplete = (props, ref) => {
  var _a;
  const {
    prefixCls: customizePrefixCls,
    className,
    popupClassName,
    dropdownClassName,
    children,
    dataSource
  } = props;
  const childNodes = toArray(children);
  // ============================= Input =============================
  let customizeInput;
  if (childNodes.length === 1 && /*#__PURE__*/React.isValidElement(childNodes[0]) && !isSelectOptionOrSelectOptGroup(childNodes[0])) {
    [customizeInput] = childNodes;
  }
  const getInputElement = customizeInput ? () => customizeInput : undefined;
  // ============================ Options ============================
  let optionChildren;
  // [Legacy] convert `children` or `dataSource` into option children
  if (childNodes.length && isSelectOptionOrSelectOptGroup(childNodes[0])) {
    optionChildren = children;
  } else {
    optionChildren = dataSource ? dataSource.map(item => {
      if ( /*#__PURE__*/React.isValidElement(item)) {
        return item;
      }
      switch (typeof item) {
        case 'string':
          return /*#__PURE__*/React.createElement(Option, {
            key: item,
            value: item
          }, item);
        case 'object':
          {
            const {
              value: optionValue
            } = item;
            return /*#__PURE__*/React.createElement(Option, {
              key: optionValue,
              value: optionValue
            }, item.text);
          }
        default:
          return undefined;
      }
    }) : [];
  }
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('AutoComplete');
    warning.deprecated(!('dataSource' in props), 'dataSource', 'options');
    process.env.NODE_ENV !== "production" ? warning(!customizeInput || !('size' in props), 'usage', 'You need to control style self instead of setting `size` when using customize input.') : void 0;
    warning.deprecated(!dropdownClassName, 'dropdownClassName', 'popupClassName');
  }
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('select', customizePrefixCls);
  // ============================ zIndex ============================
  const [zIndex] = useZIndex('SelectLike', (_a = props.dropdownStyle) === null || _a === void 0 ? void 0 : _a.zIndex);
  return /*#__PURE__*/React.createElement(Select, Object.assign({
    ref: ref,
    suffixIcon: null
  }, omit(props, ['dataSource', 'dropdownClassName']), {
    prefixCls: prefixCls,
    popupClassName: popupClassName || dropdownClassName,
    dropdownStyle: Object.assign(Object.assign({}, props.dropdownStyle), {
      zIndex
    }),
    className: classNames(`${prefixCls}-auto-complete`, className),
    mode: Select.SECRET_COMBOBOX_MODE_DO_NOT_USE,
    // Internal api
    getInputElement
  }), optionChildren);
};
const RefAutoComplete = /*#__PURE__*/React.forwardRef(AutoComplete);
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = genPurePanel(RefAutoComplete);
RefAutoComplete.Option = Option;
RefAutoComplete._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
if (process.env.NODE_ENV !== 'production') {
  RefAutoComplete.displayName = 'AutoComplete';
}
export default RefAutoComplete;