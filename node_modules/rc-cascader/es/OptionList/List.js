import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
/* eslint-disable default-case */
import classNames from 'classnames';
import * as React from 'react';
import CascaderContext from "../context";
import { getFullPathKeys, isLeaf, scrollIntoParentView, toPathKey, toPathKeys, toPathValueStr } from "../utils/commonUtil";
import { toPathOptions } from "../utils/treeUtil";
import CacheContent from "./CacheContent";
import Column, { FIX_LABEL } from "./Column";
import useActive from "./useActive";
import useKeyboard from "./useKeyboard";
var RawOptionList = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var _optionColumns$, _ref3, _classNames;
  var prefixCls = props.prefixCls,
    multiple = props.multiple,
    searchValue = props.searchValue,
    toggleOpen = props.toggleOpen,
    notFoundContent = props.notFoundContent,
    direction = props.direction,
    open = props.open;
  var containerRef = React.useRef();
  var rtl = direction === 'rtl';
  var _React$useContext = React.useContext(CascaderContext),
    options = _React$useContext.options,
    values = _React$useContext.values,
    halfValues = _React$useContext.halfValues,
    fieldNames = _React$useContext.fieldNames,
    changeOnSelect = _React$useContext.changeOnSelect,
    onSelect = _React$useContext.onSelect,
    searchOptions = _React$useContext.searchOptions,
    dropdownPrefixCls = _React$useContext.dropdownPrefixCls,
    loadData = _React$useContext.loadData,
    expandTrigger = _React$useContext.expandTrigger;
  var mergedPrefixCls = dropdownPrefixCls || prefixCls;

  // ========================= loadData =========================
  var _React$useState = React.useState([]),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    loadingKeys = _React$useState2[0],
    setLoadingKeys = _React$useState2[1];
  var internalLoadData = function internalLoadData(valueCells) {
    // Do not load when search
    if (!loadData || searchValue) {
      return;
    }
    var optionList = toPathOptions(valueCells, options, fieldNames);
    var rawOptions = optionList.map(function (_ref) {
      var option = _ref.option;
      return option;
    });
    var lastOption = rawOptions[rawOptions.length - 1];
    if (lastOption && !isLeaf(lastOption, fieldNames)) {
      var pathKey = toPathKey(valueCells);
      setLoadingKeys(function (keys) {
        return [].concat(_toConsumableArray(keys), [pathKey]);
      });
      loadData(rawOptions);
    }
  };

  // zombieJ: This is bad. We should make this same as `rc-tree` to use Promise instead.
  React.useEffect(function () {
    if (loadingKeys.length) {
      loadingKeys.forEach(function (loadingKey) {
        var valueStrCells = toPathValueStr(loadingKey);
        var optionList = toPathOptions(valueStrCells, options, fieldNames, true).map(function (_ref2) {
          var option = _ref2.option;
          return option;
        });
        var lastOption = optionList[optionList.length - 1];
        if (!lastOption || lastOption[fieldNames.children] || isLeaf(lastOption, fieldNames)) {
          setLoadingKeys(function (keys) {
            return keys.filter(function (key) {
              return key !== loadingKey;
            });
          });
        }
      });
    }
  }, [options, loadingKeys, fieldNames]);

  // ========================== Values ==========================
  var checkedSet = React.useMemo(function () {
    return new Set(toPathKeys(values));
  }, [values]);
  var halfCheckedSet = React.useMemo(function () {
    return new Set(toPathKeys(halfValues));
  }, [halfValues]);

  // ====================== Accessibility =======================
  var _useActive = useActive(multiple, open),
    _useActive2 = _slicedToArray(_useActive, 2),
    activeValueCells = _useActive2[0],
    setActiveValueCells = _useActive2[1];

  // =========================== Path ===========================
  var onPathOpen = function onPathOpen(nextValueCells) {
    setActiveValueCells(nextValueCells);

    // Trigger loadData
    internalLoadData(nextValueCells);
  };
  var isSelectable = function isSelectable(option) {
    var disabled = option.disabled;
    var isMergedLeaf = isLeaf(option, fieldNames);
    return !disabled && (isMergedLeaf || changeOnSelect || multiple);
  };
  var onPathSelect = function onPathSelect(valuePath, leaf) {
    var fromKeyboard = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
    onSelect(valuePath);
    if (!multiple && (leaf || changeOnSelect && (expandTrigger === 'hover' || fromKeyboard))) {
      toggleOpen(false);
    }
  };

  // ========================== Option ==========================
  var mergedOptions = React.useMemo(function () {
    if (searchValue) {
      return searchOptions;
    }
    return options;
  }, [searchValue, searchOptions, options]);

  // ========================== Column ==========================
  var optionColumns = React.useMemo(function () {
    var optionList = [{
      options: mergedOptions
    }];
    var currentList = mergedOptions;
    var fullPathKeys = getFullPathKeys(currentList, fieldNames);
    var _loop = function _loop() {
      var activeValueCell = activeValueCells[i];
      var currentOption = currentList.find(function (option, index) {
        return (fullPathKeys[index] ? toPathKey(fullPathKeys[index]) : option[fieldNames.value]) === activeValueCell;
      });
      var subOptions = currentOption === null || currentOption === void 0 ? void 0 : currentOption[fieldNames.children];
      if (!(subOptions !== null && subOptions !== void 0 && subOptions.length)) {
        return 1; // break
      }
      currentList = subOptions;
      optionList.push({
        options: subOptions
      });
    };
    for (var i = 0; i < activeValueCells.length; i += 1) {
      if (_loop()) break;
    }
    return optionList;
  }, [mergedOptions, activeValueCells, fieldNames]);

  // ========================= Keyboard =========================
  var onKeyboardSelect = function onKeyboardSelect(selectValueCells, option) {
    if (isSelectable(option)) {
      onPathSelect(selectValueCells, isLeaf(option, fieldNames), true);
    }
  };
  useKeyboard(ref, mergedOptions, fieldNames, activeValueCells, onPathOpen, onKeyboardSelect, {
    direction: direction,
    searchValue: searchValue,
    toggleOpen: toggleOpen,
    open: open
  });

  // >>>>> Active Scroll
  React.useEffect(function () {
    for (var i = 0; i < activeValueCells.length; i += 1) {
      var _containerRef$current;
      var cellPath = activeValueCells.slice(0, i + 1);
      var cellKeyPath = toPathKey(cellPath);
      var ele = (_containerRef$current = containerRef.current) === null || _containerRef$current === void 0 ? void 0 : _containerRef$current.querySelector("li[data-path-key=\"".concat(cellKeyPath.replace(/\\{0,2}"/g, '\\"'), "\"]") // matches unescaped double quotes
      );
      if (ele) {
        scrollIntoParentView(ele);
      }
    }
  }, [activeValueCells]);

  // ========================== Render ==========================
  // >>>>> Empty
  var isEmpty = !((_optionColumns$ = optionColumns[0]) !== null && _optionColumns$ !== void 0 && (_optionColumns$ = _optionColumns$.options) !== null && _optionColumns$ !== void 0 && _optionColumns$.length);
  var emptyList = [(_ref3 = {}, _defineProperty(_ref3, fieldNames.value, '__EMPTY__'), _defineProperty(_ref3, FIX_LABEL, notFoundContent), _defineProperty(_ref3, "disabled", true), _ref3)];
  var columnProps = _objectSpread(_objectSpread({}, props), {}, {
    multiple: !isEmpty && multiple,
    onSelect: onPathSelect,
    onActive: onPathOpen,
    onToggleOpen: toggleOpen,
    checkedSet: checkedSet,
    halfCheckedSet: halfCheckedSet,
    loadingKeys: loadingKeys,
    isSelectable: isSelectable
  });

  // >>>>> Columns
  var mergedOptionColumns = isEmpty ? [{
    options: emptyList
  }] : optionColumns;
  var columnNodes = mergedOptionColumns.map(function (col, index) {
    var prevValuePath = activeValueCells.slice(0, index);
    var activeValue = activeValueCells[index];
    return /*#__PURE__*/React.createElement(Column, _extends({
      key: index
    }, columnProps, {
      searchValue: searchValue,
      prefixCls: mergedPrefixCls,
      options: col.options,
      prevValuePath: prevValuePath,
      activeValue: activeValue
    }));
  });

  // >>>>> Render
  return /*#__PURE__*/React.createElement(CacheContent, {
    open: open
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames("".concat(mergedPrefixCls, "-menus"), (_classNames = {}, _defineProperty(_classNames, "".concat(mergedPrefixCls, "-menu-empty"), isEmpty), _defineProperty(_classNames, "".concat(mergedPrefixCls, "-rtl"), rtl), _classNames)),
    ref: containerRef
  }, columnNodes));
});
if (process.env.NODE_ENV !== 'production') {
  RawOptionList.displayName = 'RawOptionList';
}
export default RawOptionList;