import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _extends from "@babel/runtime/helpers/esm/extends";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
var _excluded = ["disabled", "title", "children", "style", "className"];
import classNames from 'classnames';
import KeyCode from "rc-util/es/KeyCode";
import useMemo from "rc-util/es/hooks/useMemo";
import omit from "rc-util/es/omit";
import pickAttrs from "rc-util/es/pickAttrs";
import List from 'rc-virtual-list';
import * as React from 'react';
import { useEffect } from 'react';
import SelectContext from "./SelectContext";
import TransBtn from "./TransBtn";
import useBaseProps from "./hooks/useBaseProps";
import { isPlatformMac } from "./utils/platformUtil";
import { isValidCount } from "./utils/valueUtil";

// export interface OptionListProps<OptionsType extends object[]> {

function isTitleType(content) {
  return typeof content === 'string' || typeof content === 'number';
}

/**
 * Using virtual list of option display.
 * Will fallback to dom if use customize render.
 */
var OptionList = function OptionList(_, ref) {
  var _useBaseProps = useBaseProps(),
    prefixCls = _useBaseProps.prefixCls,
    id = _useBaseProps.id,
    open = _useBaseProps.open,
    multiple = _useBaseProps.multiple,
    mode = _useBaseProps.mode,
    searchValue = _useBaseProps.searchValue,
    toggleOpen = _useBaseProps.toggleOpen,
    notFoundContent = _useBaseProps.notFoundContent,
    onPopupScroll = _useBaseProps.onPopupScroll;
  var _React$useContext = React.useContext(SelectContext),
    maxCount = _React$useContext.maxCount,
    flattenOptions = _React$useContext.flattenOptions,
    onActiveValue = _React$useContext.onActiveValue,
    defaultActiveFirstOption = _React$useContext.defaultActiveFirstOption,
    onSelect = _React$useContext.onSelect,
    menuItemSelectedIcon = _React$useContext.menuItemSelectedIcon,
    rawValues = _React$useContext.rawValues,
    fieldNames = _React$useContext.fieldNames,
    virtual = _React$useContext.virtual,
    direction = _React$useContext.direction,
    listHeight = _React$useContext.listHeight,
    listItemHeight = _React$useContext.listItemHeight,
    optionRender = _React$useContext.optionRender;
  var itemPrefixCls = "".concat(prefixCls, "-item");
  var memoFlattenOptions = useMemo(function () {
    return flattenOptions;
  }, [open, flattenOptions], function (prev, next) {
    return next[0] && prev[1] !== next[1];
  });

  // =========================== List ===========================
  var listRef = React.useRef(null);
  var overMaxCount = React.useMemo(function () {
    return multiple && isValidCount(maxCount) && (rawValues === null || rawValues === void 0 ? void 0 : rawValues.size) >= maxCount;
  }, [multiple, maxCount, rawValues === null || rawValues === void 0 ? void 0 : rawValues.size]);
  var onListMouseDown = function onListMouseDown(event) {
    event.preventDefault();
  };
  var scrollIntoView = function scrollIntoView(args) {
    var _listRef$current;
    (_listRef$current = listRef.current) === null || _listRef$current === void 0 || _listRef$current.scrollTo(typeof args === 'number' ? {
      index: args
    } : args);
  };

  // ========================== Active ==========================
  var getEnabledActiveIndex = function getEnabledActiveIndex(index) {
    var offset = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 1;
    var len = memoFlattenOptions.length;
    for (var i = 0; i < len; i += 1) {
      var current = (index + i * offset + len) % len;
      var _ref = memoFlattenOptions[current] || {},
        group = _ref.group,
        data = _ref.data;
      if (!group && !(data !== null && data !== void 0 && data.disabled) && !overMaxCount) {
        return current;
      }
    }
    return -1;
  };
  var _React$useState = React.useState(function () {
      return getEnabledActiveIndex(0);
    }),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    activeIndex = _React$useState2[0],
    setActiveIndex = _React$useState2[1];
  var setActive = function setActive(index) {
    var fromKeyboard = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
    setActiveIndex(index);
    var info = {
      source: fromKeyboard ? 'keyboard' : 'mouse'
    };

    // Trigger active event
    var flattenItem = memoFlattenOptions[index];
    if (!flattenItem) {
      onActiveValue(null, -1, info);
      return;
    }
    onActiveValue(flattenItem.value, index, info);
  };

  // Auto active first item when list length or searchValue changed
  useEffect(function () {
    setActive(defaultActiveFirstOption !== false ? getEnabledActiveIndex(0) : -1);
  }, [memoFlattenOptions.length, searchValue]);

  // https://github.com/ant-design/ant-design/issues/34975
  var isSelected = React.useCallback(function (value) {
    return rawValues.has(value) && mode !== 'combobox';
  }, [mode, _toConsumableArray(rawValues).toString(), rawValues.size]);

  // Auto scroll to item position in single mode
  useEffect(function () {
    /**
     * React will skip `onChange` when component update.
     * `setActive` function will call root accessibility state update which makes re-render.
     * So we need to delay to let Input component trigger onChange first.
     */
    var timeoutId = setTimeout(function () {
      if (!multiple && open && rawValues.size === 1) {
        var value = Array.from(rawValues)[0];
        var index = memoFlattenOptions.findIndex(function (_ref2) {
          var data = _ref2.data;
          return data.value === value;
        });
        if (index !== -1) {
          setActive(index);
          scrollIntoView(index);
        }
      }
    });

    // Force trigger scrollbar visible when open
    if (open) {
      var _listRef$current2;
      (_listRef$current2 = listRef.current) === null || _listRef$current2 === void 0 || _listRef$current2.scrollTo(undefined);
    }
    return function () {
      return clearTimeout(timeoutId);
    };
  }, [open, searchValue]);

  // ========================== Values ==========================
  var onSelectValue = function onSelectValue(value) {
    if (value !== undefined) {
      onSelect(value, {
        selected: !rawValues.has(value)
      });
    }

    // Single mode should always close by select
    if (!multiple) {
      toggleOpen(false);
    }
  };

  // ========================= Keyboard =========================
  React.useImperativeHandle(ref, function () {
    return {
      onKeyDown: function onKeyDown(event) {
        var which = event.which,
          ctrlKey = event.ctrlKey;
        switch (which) {
          // >>> Arrow keys & ctrl + n/p on Mac
          case KeyCode.N:
          case KeyCode.P:
          case KeyCode.UP:
          case KeyCode.DOWN:
            {
              var offset = 0;
              if (which === KeyCode.UP) {
                offset = -1;
              } else if (which === KeyCode.DOWN) {
                offset = 1;
              } else if (isPlatformMac() && ctrlKey) {
                if (which === KeyCode.N) {
                  offset = 1;
                } else if (which === KeyCode.P) {
                  offset = -1;
                }
              }
              if (offset !== 0) {
                var nextActiveIndex = getEnabledActiveIndex(activeIndex + offset, offset);
                scrollIntoView(nextActiveIndex);
                setActive(nextActiveIndex, true);
              }
              break;
            }

          // >>> Select
          case KeyCode.ENTER:
            {
              var _item$data;
              // value
              var item = memoFlattenOptions[activeIndex];
              if (item && !(item !== null && item !== void 0 && (_item$data = item.data) !== null && _item$data !== void 0 && _item$data.disabled) && !overMaxCount) {
                onSelectValue(item.value);
              } else {
                onSelectValue(undefined);
              }
              if (open) {
                event.preventDefault();
              }
              break;
            }

          // >>> Close
          case KeyCode.ESC:
            {
              toggleOpen(false);
              if (open) {
                event.stopPropagation();
              }
            }
        }
      },
      onKeyUp: function onKeyUp() {},
      scrollTo: function scrollTo(index) {
        scrollIntoView(index);
      }
    };
  });

  // ========================== Render ==========================
  if (memoFlattenOptions.length === 0) {
    return /*#__PURE__*/React.createElement("div", {
      role: "listbox",
      id: "".concat(id, "_list"),
      className: "".concat(itemPrefixCls, "-empty"),
      onMouseDown: onListMouseDown
    }, notFoundContent);
  }
  var omitFieldNameList = Object.keys(fieldNames).map(function (key) {
    return fieldNames[key];
  });
  var getLabel = function getLabel(item) {
    return item.label;
  };
  function getItemAriaProps(item, index) {
    var group = item.group;
    return {
      role: group ? 'presentation' : 'option',
      id: "".concat(id, "_list_").concat(index)
    };
  }
  var renderItem = function renderItem(index) {
    var item = memoFlattenOptions[index];
    if (!item) {
      return null;
    }
    var itemData = item.data || {};
    var value = itemData.value;
    var group = item.group;
    var attrs = pickAttrs(itemData, true);
    var mergedLabel = getLabel(item);
    return item ? /*#__PURE__*/React.createElement("div", _extends({
      "aria-label": typeof mergedLabel === 'string' && !group ? mergedLabel : null
    }, attrs, {
      key: index
    }, getItemAriaProps(item, index), {
      "aria-selected": isSelected(value)
    }), value) : null;
  };
  var a11yProps = {
    role: 'listbox',
    id: "".concat(id, "_list")
  };
  return /*#__PURE__*/React.createElement(React.Fragment, null, virtual && /*#__PURE__*/React.createElement("div", _extends({}, a11yProps, {
    style: {
      height: 0,
      width: 0,
      overflow: 'hidden'
    }
  }), renderItem(activeIndex - 1), renderItem(activeIndex), renderItem(activeIndex + 1)), /*#__PURE__*/React.createElement(List, {
    itemKey: "key",
    ref: listRef,
    data: memoFlattenOptions,
    height: listHeight,
    itemHeight: listItemHeight,
    fullHeight: false,
    onMouseDown: onListMouseDown,
    onScroll: onPopupScroll,
    virtual: virtual,
    direction: direction,
    innerProps: virtual ? null : a11yProps
  }, function (item, itemIndex) {
    var group = item.group,
      groupOption = item.groupOption,
      data = item.data,
      label = item.label,
      value = item.value;
    var key = data.key;

    // Group
    if (group) {
      var _data$title;
      var groupTitle = (_data$title = data.title) !== null && _data$title !== void 0 ? _data$title : isTitleType(label) ? label.toString() : undefined;
      return /*#__PURE__*/React.createElement("div", {
        className: classNames(itemPrefixCls, "".concat(itemPrefixCls, "-group"), data.className),
        title: groupTitle
      }, label !== undefined ? label : key);
    }
    var disabled = data.disabled,
      title = data.title,
      children = data.children,
      style = data.style,
      className = data.className,
      otherProps = _objectWithoutProperties(data, _excluded);
    var passedProps = omit(otherProps, omitFieldNameList);

    // Option
    var selected = isSelected(value);
    var mergedDisabled = disabled || !selected && overMaxCount;
    var optionPrefixCls = "".concat(itemPrefixCls, "-option");
    var optionClassName = classNames(itemPrefixCls, optionPrefixCls, className, _defineProperty(_defineProperty(_defineProperty(_defineProperty({}, "".concat(optionPrefixCls, "-grouped"), groupOption), "".concat(optionPrefixCls, "-active"), activeIndex === itemIndex && !mergedDisabled), "".concat(optionPrefixCls, "-disabled"), mergedDisabled), "".concat(optionPrefixCls, "-selected"), selected));
    var mergedLabel = getLabel(item);
    var iconVisible = !menuItemSelectedIcon || typeof menuItemSelectedIcon === 'function' || selected;

    // https://github.com/ant-design/ant-design/issues/34145
    var content = typeof mergedLabel === 'number' ? mergedLabel : mergedLabel || value;
    // https://github.com/ant-design/ant-design/issues/26717
    var optionTitle = isTitleType(content) ? content.toString() : undefined;
    if (title !== undefined) {
      optionTitle = title;
    }
    return /*#__PURE__*/React.createElement("div", _extends({}, pickAttrs(passedProps), !virtual ? getItemAriaProps(item, itemIndex) : {}, {
      "aria-selected": selected,
      className: optionClassName,
      title: optionTitle,
      onMouseMove: function onMouseMove() {
        if (activeIndex === itemIndex || mergedDisabled) {
          return;
        }
        setActive(itemIndex);
      },
      onClick: function onClick() {
        if (!mergedDisabled) {
          onSelectValue(value);
        }
      },
      style: style
    }), /*#__PURE__*/React.createElement("div", {
      className: "".concat(optionPrefixCls, "-content")
    }, typeof optionRender === 'function' ? optionRender(item, {
      index: itemIndex
    }) : content), /*#__PURE__*/React.isValidElement(menuItemSelectedIcon) || selected, iconVisible && /*#__PURE__*/React.createElement(TransBtn, {
      className: "".concat(itemPrefixCls, "-option-state"),
      customizeIcon: menuItemSelectedIcon,
      customizeIconProps: {
        value: value,
        disabled: mergedDisabled,
        isSelected: selected
      }
    }, selected ? '✓' : null));
  }));
};
var RefOptionList = /*#__PURE__*/React.forwardRef(OptionList);
if (process.env.NODE_ENV !== 'production') {
  RefOptionList.displayName = 'OptionList';
}
export default RefOptionList;