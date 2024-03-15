import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import classNames from 'classnames';
import * as React from 'react';
import CascaderContext from "../context";
import { SEARCH_MARK } from "../hooks/useSearchOptions";
import { isLeaf, toPathKey } from "../utils/commonUtil";
import Checkbox from "./Checkbox";
export var FIX_LABEL = '__cascader_fix_label__';
export default function Column(_ref) {
  var prefixCls = _ref.prefixCls,
    multiple = _ref.multiple,
    options = _ref.options,
    activeValue = _ref.activeValue,
    prevValuePath = _ref.prevValuePath,
    onToggleOpen = _ref.onToggleOpen,
    onSelect = _ref.onSelect,
    onActive = _ref.onActive,
    checkedSet = _ref.checkedSet,
    halfCheckedSet = _ref.halfCheckedSet,
    loadingKeys = _ref.loadingKeys,
    isSelectable = _ref.isSelectable,
    searchValue = _ref.searchValue;
  var menuPrefixCls = "".concat(prefixCls, "-menu");
  var menuItemPrefixCls = "".concat(prefixCls, "-menu-item");
  var _React$useContext = React.useContext(CascaderContext),
    fieldNames = _React$useContext.fieldNames,
    changeOnSelect = _React$useContext.changeOnSelect,
    expandTrigger = _React$useContext.expandTrigger,
    expandIcon = _React$useContext.expandIcon,
    loadingIcon = _React$useContext.loadingIcon,
    dropdownMenuColumnStyle = _React$useContext.dropdownMenuColumnStyle,
    optionRender = _React$useContext.optionRender;
  var hoverOpen = expandTrigger === 'hover';

  // ============================ Option ============================
  var optionInfoList = React.useMemo(function () {
    return options.map(function (option) {
      var _option$FIX_LABEL;
      var disabled = option.disabled,
        disableCheckbox = option.disableCheckbox;
      var searchOptions = option[SEARCH_MARK];
      var label = (_option$FIX_LABEL = option[FIX_LABEL]) !== null && _option$FIX_LABEL !== void 0 ? _option$FIX_LABEL : option[fieldNames.label];
      var value = option[fieldNames.value];
      var isMergedLeaf = isLeaf(option, fieldNames);

      // Get real value of option. Search option is different way.
      var fullPath = searchOptions ? searchOptions.map(function (opt) {
        return opt[fieldNames.value];
      }) : [].concat(_toConsumableArray(prevValuePath), [value]);
      var fullPathKey = toPathKey(fullPath);
      var isLoading = loadingKeys.includes(fullPathKey);

      // >>>>> checked
      var checked = checkedSet.has(fullPathKey);

      // >>>>> halfChecked
      var halfChecked = halfCheckedSet.has(fullPathKey);
      return {
        disabled: disabled,
        label: label,
        value: value,
        isLeaf: isMergedLeaf,
        isLoading: isLoading,
        checked: checked,
        halfChecked: halfChecked,
        option: option,
        disableCheckbox: disableCheckbox,
        fullPath: fullPath,
        fullPathKey: fullPathKey
      };
    });
  }, [options, checkedSet, fieldNames, halfCheckedSet, loadingKeys, prevValuePath]);

  // ============================ Render ============================
  return /*#__PURE__*/React.createElement("ul", {
    className: menuPrefixCls,
    role: "menu"
  }, optionInfoList.map(function (_ref2) {
    var _classNames;
    var disabled = _ref2.disabled,
      label = _ref2.label,
      value = _ref2.value,
      isMergedLeaf = _ref2.isLeaf,
      isLoading = _ref2.isLoading,
      checked = _ref2.checked,
      halfChecked = _ref2.halfChecked,
      option = _ref2.option,
      fullPath = _ref2.fullPath,
      fullPathKey = _ref2.fullPathKey,
      disableCheckbox = _ref2.disableCheckbox;
    // >>>>> Open
    var triggerOpenPath = function triggerOpenPath() {
      if (disabled || searchValue) {
        return;
      }
      var nextValueCells = _toConsumableArray(fullPath);
      if (hoverOpen && isMergedLeaf) {
        nextValueCells.pop();
      }
      onActive(nextValueCells);
    };

    // >>>>> Selection
    var triggerSelect = function triggerSelect() {
      if (isSelectable(option)) {
        onSelect(fullPath, isMergedLeaf);
      }
    };

    // >>>>> Title
    var title;
    if (typeof option.title === 'string') {
      title = option.title;
    } else if (typeof label === 'string') {
      title = label;
    }

    // >>>>> Render
    return /*#__PURE__*/React.createElement("li", {
      key: fullPathKey,
      className: classNames(menuItemPrefixCls, (_classNames = {}, _defineProperty(_classNames, "".concat(menuItemPrefixCls, "-expand"), !isMergedLeaf), _defineProperty(_classNames, "".concat(menuItemPrefixCls, "-active"), activeValue === value || activeValue === fullPathKey), _defineProperty(_classNames, "".concat(menuItemPrefixCls, "-disabled"), disabled), _defineProperty(_classNames, "".concat(menuItemPrefixCls, "-loading"), isLoading), _classNames)),
      style: dropdownMenuColumnStyle,
      role: "menuitemcheckbox",
      title: title,
      "aria-checked": checked,
      "data-path-key": fullPathKey,
      onClick: function onClick() {
        triggerOpenPath();
        if (disableCheckbox) {
          return;
        }
        if (!multiple || isMergedLeaf) {
          triggerSelect();
        }
      },
      onDoubleClick: function onDoubleClick() {
        if (changeOnSelect) {
          onToggleOpen(false);
        }
      },
      onMouseEnter: function onMouseEnter() {
        if (hoverOpen) {
          triggerOpenPath();
        }
      },
      onMouseDown: function onMouseDown(e) {
        // Prevent selector from blurring
        e.preventDefault();
      }
    }, multiple && /*#__PURE__*/React.createElement(Checkbox, {
      prefixCls: "".concat(prefixCls, "-checkbox"),
      checked: checked,
      halfChecked: halfChecked,
      disabled: disabled || disableCheckbox,
      disableCheckbox: disableCheckbox,
      onClick: function onClick(e) {
        if (disableCheckbox) {
          return;
        }
        e.stopPropagation();
        triggerSelect();
      }
    }), /*#__PURE__*/React.createElement("div", {
      className: "".concat(menuItemPrefixCls, "-content")
    }, optionRender ? optionRender(option) : label), !isLoading && expandIcon && !isMergedLeaf && /*#__PURE__*/React.createElement("div", {
      className: "".concat(menuItemPrefixCls, "-expand-icon")
    }, expandIcon), isLoading && loadingIcon && /*#__PURE__*/React.createElement("div", {
      className: "".concat(menuItemPrefixCls, "-loading-icon")
    }, loadingIcon));
  }));
}