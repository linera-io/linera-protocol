"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _context = _interopRequireDefault(require("../context"));
var _commonUtil = require("../utils/commonUtil");
var _treeUtil = require("../utils/treeUtil");
var _CacheContent = _interopRequireDefault(require("./CacheContent"));
var _Column = _interopRequireWildcard(require("./Column"));
var _useActive3 = _interopRequireDefault(require("./useActive"));
var _useKeyboard = _interopRequireDefault(require("./useKeyboard"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
/* eslint-disable default-case */

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
  var _React$useContext = React.useContext(_context.default),
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
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    loadingKeys = _React$useState2[0],
    setLoadingKeys = _React$useState2[1];
  var internalLoadData = function internalLoadData(valueCells) {
    // Do not load when search
    if (!loadData || searchValue) {
      return;
    }
    var optionList = (0, _treeUtil.toPathOptions)(valueCells, options, fieldNames);
    var rawOptions = optionList.map(function (_ref) {
      var option = _ref.option;
      return option;
    });
    var lastOption = rawOptions[rawOptions.length - 1];
    if (lastOption && !(0, _commonUtil.isLeaf)(lastOption, fieldNames)) {
      var pathKey = (0, _commonUtil.toPathKey)(valueCells);
      setLoadingKeys(function (keys) {
        return [].concat((0, _toConsumableArray2.default)(keys), [pathKey]);
      });
      loadData(rawOptions);
    }
  };

  // zombieJ: This is bad. We should make this same as `rc-tree` to use Promise instead.
  React.useEffect(function () {
    if (loadingKeys.length) {
      loadingKeys.forEach(function (loadingKey) {
        var valueStrCells = (0, _commonUtil.toPathValueStr)(loadingKey);
        var optionList = (0, _treeUtil.toPathOptions)(valueStrCells, options, fieldNames, true).map(function (_ref2) {
          var option = _ref2.option;
          return option;
        });
        var lastOption = optionList[optionList.length - 1];
        if (!lastOption || lastOption[fieldNames.children] || (0, _commonUtil.isLeaf)(lastOption, fieldNames)) {
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
    return new Set((0, _commonUtil.toPathKeys)(values));
  }, [values]);
  var halfCheckedSet = React.useMemo(function () {
    return new Set((0, _commonUtil.toPathKeys)(halfValues));
  }, [halfValues]);

  // ====================== Accessibility =======================
  var _useActive = (0, _useActive3.default)(multiple, open),
    _useActive2 = (0, _slicedToArray2.default)(_useActive, 2),
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
    var isMergedLeaf = (0, _commonUtil.isLeaf)(option, fieldNames);
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
    var fullPathKeys = (0, _commonUtil.getFullPathKeys)(currentList, fieldNames);
    var _loop = function _loop() {
      var activeValueCell = activeValueCells[i];
      var currentOption = currentList.find(function (option, index) {
        return (fullPathKeys[index] ? (0, _commonUtil.toPathKey)(fullPathKeys[index]) : option[fieldNames.value]) === activeValueCell;
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
      onPathSelect(selectValueCells, (0, _commonUtil.isLeaf)(option, fieldNames), true);
    }
  };
  (0, _useKeyboard.default)(ref, mergedOptions, fieldNames, activeValueCells, onPathOpen, onKeyboardSelect, {
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
      var cellKeyPath = (0, _commonUtil.toPathKey)(cellPath);
      var ele = (_containerRef$current = containerRef.current) === null || _containerRef$current === void 0 ? void 0 : _containerRef$current.querySelector("li[data-path-key=\"".concat(cellKeyPath.replace(/\\{0,2}"/g, '\\"'), "\"]") // matches unescaped double quotes
      );
      if (ele) {
        (0, _commonUtil.scrollIntoParentView)(ele);
      }
    }
  }, [activeValueCells]);

  // ========================== Render ==========================
  // >>>>> Empty
  var isEmpty = !((_optionColumns$ = optionColumns[0]) !== null && _optionColumns$ !== void 0 && (_optionColumns$ = _optionColumns$.options) !== null && _optionColumns$ !== void 0 && _optionColumns$.length);
  var emptyList = [(_ref3 = {}, (0, _defineProperty2.default)(_ref3, fieldNames.value, '__EMPTY__'), (0, _defineProperty2.default)(_ref3, _Column.FIX_LABEL, notFoundContent), (0, _defineProperty2.default)(_ref3, "disabled", true), _ref3)];
  var columnProps = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, props), {}, {
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
    return /*#__PURE__*/React.createElement(_Column.default, (0, _extends2.default)({
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
  return /*#__PURE__*/React.createElement(_CacheContent.default, {
    open: open
  }, /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)("".concat(mergedPrefixCls, "-menus"), (_classNames = {}, (0, _defineProperty2.default)(_classNames, "".concat(mergedPrefixCls, "-menu-empty"), isEmpty), (0, _defineProperty2.default)(_classNames, "".concat(mergedPrefixCls, "-rtl"), rtl), _classNames)),
    ref: containerRef
  }, columnNodes));
});
if (process.env.NODE_ENV !== 'production') {
  RawOptionList.displayName = 'RawOptionList';
}
var _default = exports.default = RawOptionList;