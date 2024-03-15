import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import classNames from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import KeyCode from "rc-util/es/KeyCode";
import pickAttrs from "rc-util/es/pickAttrs";
import warning from "rc-util/es/warning";
import React, { useEffect } from 'react';
import zhCN from "./locale/zh_CN";
import Options from "./Options";
import Pager from "./Pager";
var defaultItemRender = function defaultItemRender(page, type, element) {
  return element;
};
function noop() {}
function isInteger(v) {
  var value = Number(v);
  return typeof value === 'number' && !Number.isNaN(value) && isFinite(value) && Math.floor(value) === value;
}
function calculatePage(p, pageSize, total) {
  var _pageSize = typeof p === 'undefined' ? pageSize : p;
  return Math.floor((total - 1) / _pageSize) + 1;
}
var Pagination = function Pagination(props) {
  var _classNames5;
  var _props$prefixCls = props.prefixCls,
    prefixCls = _props$prefixCls === void 0 ? 'rc-pagination' : _props$prefixCls,
    _props$selectPrefixCl = props.selectPrefixCls,
    selectPrefixCls = _props$selectPrefixCl === void 0 ? 'rc-select' : _props$selectPrefixCl,
    className = props.className,
    selectComponentClass = props.selectComponentClass,
    currentProp = props.current,
    _props$defaultCurrent = props.defaultCurrent,
    defaultCurrent = _props$defaultCurrent === void 0 ? 1 : _props$defaultCurrent,
    _props$total = props.total,
    total = _props$total === void 0 ? 0 : _props$total,
    pageSizeProp = props.pageSize,
    _props$defaultPageSiz = props.defaultPageSize,
    defaultPageSize = _props$defaultPageSiz === void 0 ? 10 : _props$defaultPageSiz,
    _props$onChange = props.onChange,
    onChange = _props$onChange === void 0 ? noop : _props$onChange,
    hideOnSinglePage = props.hideOnSinglePage,
    _props$showPrevNextJu = props.showPrevNextJumpers,
    showPrevNextJumpers = _props$showPrevNextJu === void 0 ? true : _props$showPrevNextJu,
    showQuickJumper = props.showQuickJumper,
    showLessItems = props.showLessItems,
    _props$showTitle = props.showTitle,
    showTitle = _props$showTitle === void 0 ? true : _props$showTitle,
    _props$onShowSizeChan = props.onShowSizeChange,
    onShowSizeChange = _props$onShowSizeChan === void 0 ? noop : _props$onShowSizeChan,
    _props$locale = props.locale,
    locale = _props$locale === void 0 ? zhCN : _props$locale,
    style = props.style,
    _props$totalBoundaryS = props.totalBoundaryShowSizeChanger,
    totalBoundaryShowSizeChanger = _props$totalBoundaryS === void 0 ? 50 : _props$totalBoundaryS,
    disabled = props.disabled,
    simple = props.simple,
    showTotal = props.showTotal,
    showSizeChangerProp = props.showSizeChanger,
    pageSizeOptions = props.pageSizeOptions,
    _props$itemRender = props.itemRender,
    itemRender = _props$itemRender === void 0 ? defaultItemRender : _props$itemRender,
    jumpPrevIcon = props.jumpPrevIcon,
    jumpNextIcon = props.jumpNextIcon,
    prevIcon = props.prevIcon,
    nextIcon = props.nextIcon;
  var paginationRef = React.useRef(null);
  var _useMergedState = useMergedState(10, {
      value: pageSizeProp,
      defaultValue: defaultPageSize
    }),
    _useMergedState2 = _slicedToArray(_useMergedState, 2),
    pageSize = _useMergedState2[0],
    setPageSize = _useMergedState2[1];
  var _useMergedState3 = useMergedState(1, {
      value: currentProp,
      defaultValue: defaultCurrent,
      postState: function postState(c) {
        return Math.max(1, Math.min(c, calculatePage(undefined, pageSize, total)));
      }
    }),
    _useMergedState4 = _slicedToArray(_useMergedState3, 2),
    current = _useMergedState4[0],
    setCurrent = _useMergedState4[1];
  var _React$useState = React.useState(current),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    internalInputVal = _React$useState2[0],
    setInternalInputVal = _React$useState2[1];
  useEffect(function () {
    setInternalInputVal(current);
  }, [current]);
  var hasOnChange = onChange !== noop;
  var hasCurrent = ('current' in props);
  if (process.env.NODE_ENV !== 'production') {
    warning(hasCurrent ? hasOnChange : true, 'You provided a `current` prop to a Pagination component without an `onChange` handler. This will render a read-only component.');
  }
  var jumpPrevPage = Math.max(1, current - (showLessItems ? 3 : 5));
  var jumpNextPage = Math.min(calculatePage(undefined, pageSize, total), current + (showLessItems ? 3 : 5));
  function getItemIcon(icon, label) {
    var iconNode = icon || /*#__PURE__*/React.createElement("button", {
      type: "button",
      "aria-label": label,
      className: "".concat(prefixCls, "-item-link")
    });
    if (typeof icon === 'function') {
      iconNode = /*#__PURE__*/React.createElement(icon, _objectSpread({}, props));
    }
    return iconNode;
  }
  function getValidValue(e) {
    var inputValue = e.target.value;
    var allPages = calculatePage(undefined, pageSize, total);
    var value;
    if (inputValue === '') {
      value = inputValue;
    } else if (Number.isNaN(Number(inputValue))) {
      value = internalInputVal;
    } else if (inputValue >= allPages) {
      value = allPages;
    } else {
      value = Number(inputValue);
    }
    return value;
  }
  function isValid(page) {
    return isInteger(page) && page !== current && isInteger(total) && total > 0;
  }
  var shouldDisplayQuickJumper = total > pageSize ? showQuickJumper : false;

  /**
   * prevent "up arrow" key reseting cursor position within textbox
   * @see https://stackoverflow.com/a/1081114
   */
  function handleKeyDown(event) {
    if (event.keyCode === KeyCode.UP || event.keyCode === KeyCode.DOWN) {
      event.preventDefault();
    }
  }
  function handleKeyUp(event) {
    var value = getValidValue(event);
    if (value !== internalInputVal) {
      setInternalInputVal(value);
    }
    switch (event.keyCode) {
      case KeyCode.ENTER:
        handleChange(value);
        break;
      case KeyCode.UP:
        handleChange(value - 1);
        break;
      case KeyCode.DOWN:
        handleChange(value + 1);
        break;
      default:
        break;
    }
  }
  function handleBlur(event) {
    handleChange(getValidValue(event));
  }
  function changePageSize(size) {
    var newCurrent = calculatePage(size, pageSize, total);
    var nextCurrent = current > newCurrent && newCurrent !== 0 ? newCurrent : current;
    setPageSize(size);
    setInternalInputVal(nextCurrent);
    onShowSizeChange === null || onShowSizeChange === void 0 || onShowSizeChange(current, size);
    setCurrent(nextCurrent);
    onChange === null || onChange === void 0 || onChange(nextCurrent, size);
  }
  function handleChange(page) {
    if (isValid(page) && !disabled) {
      var currentPage = calculatePage(undefined, pageSize, total);
      var newPage = page;
      if (page > currentPage) {
        newPage = currentPage;
      } else if (page < 1) {
        newPage = 1;
      }
      if (newPage !== internalInputVal) {
        setInternalInputVal(newPage);
      }
      setCurrent(newPage);
      onChange === null || onChange === void 0 || onChange(newPage, pageSize);
      return newPage;
    }
    return current;
  }
  var hasPrev = current > 1;
  var hasNext = current < calculatePage(undefined, pageSize, total);
  var showSizeChanger = showSizeChangerProp !== null && showSizeChangerProp !== void 0 ? showSizeChangerProp : total > totalBoundaryShowSizeChanger;
  function prevHandle() {
    if (hasPrev) handleChange(current - 1);
  }
  function nextHandle() {
    if (hasNext) handleChange(current + 1);
  }
  function jumpPrevHandle() {
    handleChange(jumpPrevPage);
  }
  function jumpNextHandle() {
    handleChange(jumpNextPage);
  }
  function runIfEnter(event, callback) {
    if (event.key === 'Enter' || event.charCode === KeyCode.ENTER || event.keyCode === KeyCode.ENTER) {
      for (var _len = arguments.length, restParams = new Array(_len > 2 ? _len - 2 : 0), _key = 2; _key < _len; _key++) {
        restParams[_key - 2] = arguments[_key];
      }
      callback.apply(void 0, restParams);
    }
  }
  function runIfEnterPrev(event) {
    runIfEnter(event, prevHandle);
  }
  function runIfEnterNext(event) {
    runIfEnter(event, nextHandle);
  }
  function runIfEnterJumpPrev(event) {
    runIfEnter(event, jumpPrevHandle);
  }
  function runIfEnterJumpNext(event) {
    runIfEnter(event, jumpNextHandle);
  }
  function renderPrev(prevPage) {
    var prevButton = itemRender(prevPage, 'prev', getItemIcon(prevIcon, 'prev page'));
    return /*#__PURE__*/React.isValidElement(prevButton) ? /*#__PURE__*/React.cloneElement(prevButton, {
      disabled: !hasPrev
    }) : prevButton;
  }
  function renderNext(nextPage) {
    var nextButton = itemRender(nextPage, 'next', getItemIcon(nextIcon, 'next page'));
    return /*#__PURE__*/React.isValidElement(nextButton) ? /*#__PURE__*/React.cloneElement(nextButton, {
      disabled: !hasNext
    }) : nextButton;
  }
  function handleGoTO(event) {
    if (event.type === 'click' || event.keyCode === KeyCode.ENTER) {
      handleChange(internalInputVal);
    }
  }
  var jumpPrev = null;
  var dataOrAriaAttributeProps = pickAttrs(props, {
    aria: true,
    data: true
  });
  var totalText = showTotal && /*#__PURE__*/React.createElement("li", {
    className: "".concat(prefixCls, "-total-text")
  }, showTotal(total, [total === 0 ? 0 : (current - 1) * pageSize + 1, current * pageSize > total ? total : current * pageSize]));
  var jumpNext = null;
  var allPages = calculatePage(undefined, pageSize, total);

  // ================== Render ==================
  // When hideOnSinglePage is true and there is only 1 page, hide the pager
  if (hideOnSinglePage && total <= pageSize) {
    return null;
  }
  var pagerList = [];
  var pagerProps = {
    rootPrefixCls: prefixCls,
    onClick: handleChange,
    onKeyPress: runIfEnter,
    showTitle: showTitle,
    itemRender: itemRender,
    page: -1
  };
  var prevPage = current - 1 > 0 ? current - 1 : 0;
  var nextPage = current + 1 < allPages ? current + 1 : allPages;
  var goButton = showQuickJumper && showQuickJumper.goButton;

  // ================== Simple ==================
  // FIXME: ts type
  var gotoButton = goButton;
  var simplePager = null;
  if (simple) {
    // ====== Simple quick jump ======
    if (goButton) {
      if (typeof goButton === 'boolean') {
        gotoButton = /*#__PURE__*/React.createElement("button", {
          type: "button",
          onClick: handleGoTO,
          onKeyUp: handleGoTO
        }, locale.jump_to_confirm);
      } else {
        gotoButton = /*#__PURE__*/React.createElement("span", {
          onClick: handleGoTO,
          onKeyUp: handleGoTO
        }, goButton);
      }
      gotoButton = /*#__PURE__*/React.createElement("li", {
        title: showTitle ? "".concat(locale.jump_to).concat(current, "/").concat(allPages) : null,
        className: "".concat(prefixCls, "-simple-pager")
      }, gotoButton);
    }
    simplePager = /*#__PURE__*/React.createElement("li", {
      title: showTitle ? "".concat(current, "/").concat(allPages) : null,
      className: "".concat(prefixCls, "-simple-pager")
    }, /*#__PURE__*/React.createElement("input", {
      type: "text",
      value: internalInputVal,
      disabled: disabled,
      onKeyDown: handleKeyDown,
      onKeyUp: handleKeyUp,
      onChange: handleKeyUp,
      onBlur: handleBlur,
      size: 3
    }), /*#__PURE__*/React.createElement("span", {
      className: "".concat(prefixCls, "-slash")
    }, "/"), allPages);
  }

  // ====================== Normal ======================
  var pageBufferSize = showLessItems ? 1 : 2;
  if (allPages <= 3 + pageBufferSize * 2) {
    if (!allPages) {
      pagerList.push( /*#__PURE__*/React.createElement(Pager, _extends({}, pagerProps, {
        key: "noPager",
        page: 1,
        className: "".concat(prefixCls, "-item-disabled")
      })));
    }
    for (var i = 1; i <= allPages; i += 1) {
      pagerList.push( /*#__PURE__*/React.createElement(Pager, _extends({}, pagerProps, {
        key: i,
        page: i,
        active: current === i
      })));
    }
  } else {
    var prevItemTitle = showLessItems ? locale.prev_3 : locale.prev_5;
    var nextItemTitle = showLessItems ? locale.next_3 : locale.next_5;
    var jumpPrevContent = itemRender(jumpPrevPage, 'jump-prev', getItemIcon(jumpPrevIcon, 'prev page'));
    var jumpNextContent = itemRender(jumpNextPage, 'jump-next', getItemIcon(jumpNextIcon, 'next page'));
    if (showPrevNextJumpers) {
      jumpPrev = jumpPrevContent ? /*#__PURE__*/React.createElement("li", {
        title: showTitle ? prevItemTitle : null,
        key: "prev",
        onClick: jumpPrevHandle,
        tabIndex: 0,
        onKeyDown: runIfEnterJumpPrev,
        className: classNames("".concat(prefixCls, "-jump-prev"), _defineProperty({}, "".concat(prefixCls, "-jump-prev-custom-icon"), !!jumpPrevIcon))
      }, jumpPrevContent) : null;
      jumpNext = jumpNextContent ? /*#__PURE__*/React.createElement("li", {
        title: showTitle ? nextItemTitle : null,
        key: "next",
        onClick: jumpNextHandle,
        tabIndex: 0,
        onKeyDown: runIfEnterJumpNext,
        className: classNames("".concat(prefixCls, "-jump-next"), _defineProperty({}, "".concat(prefixCls, "-jump-next-custom-icon"), !!jumpNextIcon))
      }, jumpNextContent) : null;
    }
    var left = Math.max(1, current - pageBufferSize);
    var right = Math.min(current + pageBufferSize, allPages);
    if (current - 1 <= pageBufferSize) {
      right = 1 + pageBufferSize * 2;
    }
    if (allPages - current <= pageBufferSize) {
      left = allPages - pageBufferSize * 2;
    }
    for (var _i = left; _i <= right; _i += 1) {
      pagerList.push( /*#__PURE__*/React.createElement(Pager, _extends({}, pagerProps, {
        key: _i,
        page: _i,
        active: current === _i
      })));
    }
    if (current - 1 >= pageBufferSize * 2 && current !== 1 + 2) {
      pagerList[0] = /*#__PURE__*/React.cloneElement(pagerList[0], {
        className: classNames("".concat(prefixCls, "-item-after-jump-prev"), pagerList[0].props.className)
      });
      pagerList.unshift(jumpPrev);
    }
    if (allPages - current >= pageBufferSize * 2 && current !== allPages - 2) {
      var lastOne = pagerList[pagerList.length - 1];
      pagerList[pagerList.length - 1] = /*#__PURE__*/React.cloneElement(lastOne, {
        className: classNames("".concat(prefixCls, "-item-before-jump-next"), lastOne.props.className)
      });
      pagerList.push(jumpNext);
    }
    if (left !== 1) {
      pagerList.unshift( /*#__PURE__*/React.createElement(Pager, _extends({}, pagerProps, {
        key: 1,
        page: 1
      })));
    }
    if (right !== allPages) {
      pagerList.push( /*#__PURE__*/React.createElement(Pager, _extends({}, pagerProps, {
        key: allPages,
        page: allPages
      })));
    }
  }
  var prev = renderPrev(prevPage);
  if (prev) {
    var prevDisabled = !hasPrev || !allPages;
    prev = /*#__PURE__*/React.createElement("li", {
      title: showTitle ? locale.prev_page : null,
      onClick: prevHandle,
      tabIndex: prevDisabled ? null : 0,
      onKeyDown: runIfEnterPrev,
      className: classNames("".concat(prefixCls, "-prev"), _defineProperty({}, "".concat(prefixCls, "-disabled"), prevDisabled)),
      "aria-disabled": prevDisabled
    }, prev);
  }
  var next = renderNext(nextPage);
  if (next) {
    var nextDisabled, nextTabIndex;
    if (simple) {
      nextDisabled = !hasNext;
      nextTabIndex = hasPrev ? 0 : null;
    } else {
      nextDisabled = !hasNext || !allPages;
      nextTabIndex = nextDisabled ? null : 0;
    }
    next = /*#__PURE__*/React.createElement("li", {
      title: showTitle ? locale.next_page : null,
      onClick: nextHandle,
      tabIndex: nextTabIndex,
      onKeyDown: runIfEnterNext,
      className: classNames("".concat(prefixCls, "-next"), _defineProperty({}, "".concat(prefixCls, "-disabled"), nextDisabled)),
      "aria-disabled": nextDisabled
    }, next);
  }
  var cls = classNames(prefixCls, className, (_classNames5 = {}, _defineProperty(_classNames5, "".concat(prefixCls, "-simple"), simple), _defineProperty(_classNames5, "".concat(prefixCls, "-disabled"), disabled), _classNames5));
  return /*#__PURE__*/React.createElement("ul", _extends({
    className: cls,
    style: style,
    ref: paginationRef
  }, dataOrAriaAttributeProps), totalText, prev, simple ? simplePager : pagerList, next, /*#__PURE__*/React.createElement(Options, {
    locale: locale,
    rootPrefixCls: prefixCls,
    disabled: disabled,
    selectComponentClass: selectComponentClass,
    selectPrefixCls: selectPrefixCls,
    changeSize: showSizeChanger ? changePageSize : null,
    pageSize: pageSize,
    pageSizeOptions: pageSizeOptions,
    quickGo: shouldDisplayQuickJumper ? handleChange : null,
    goButton: gotoButton
  }));
};
if (process.env.NODE_ENV !== 'production') {
  Pagination.displayName = 'Pagination';
}
export default Pagination;