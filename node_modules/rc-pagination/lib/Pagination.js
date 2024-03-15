"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useMergedState5 = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _react = _interopRequireWildcard(require("react"));
var _zh_CN = _interopRequireDefault(require("./locale/zh_CN"));
var _Options = _interopRequireDefault(require("./Options"));
var _Pager = _interopRequireDefault(require("./Pager"));
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
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
    locale = _props$locale === void 0 ? _zh_CN.default : _props$locale,
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
  var paginationRef = _react.default.useRef(null);
  var _useMergedState = (0, _useMergedState5.default)(10, {
      value: pageSizeProp,
      defaultValue: defaultPageSize
    }),
    _useMergedState2 = (0, _slicedToArray2.default)(_useMergedState, 2),
    pageSize = _useMergedState2[0],
    setPageSize = _useMergedState2[1];
  var _useMergedState3 = (0, _useMergedState5.default)(1, {
      value: currentProp,
      defaultValue: defaultCurrent,
      postState: function postState(c) {
        return Math.max(1, Math.min(c, calculatePage(undefined, pageSize, total)));
      }
    }),
    _useMergedState4 = (0, _slicedToArray2.default)(_useMergedState3, 2),
    current = _useMergedState4[0],
    setCurrent = _useMergedState4[1];
  var _React$useState = _react.default.useState(current),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    internalInputVal = _React$useState2[0],
    setInternalInputVal = _React$useState2[1];
  (0, _react.useEffect)(function () {
    setInternalInputVal(current);
  }, [current]);
  var hasOnChange = onChange !== noop;
  var hasCurrent = ('current' in props);
  if (process.env.NODE_ENV !== 'production') {
    (0, _warning.default)(hasCurrent ? hasOnChange : true, 'You provided a `current` prop to a Pagination component without an `onChange` handler. This will render a read-only component.');
  }
  var jumpPrevPage = Math.max(1, current - (showLessItems ? 3 : 5));
  var jumpNextPage = Math.min(calculatePage(undefined, pageSize, total), current + (showLessItems ? 3 : 5));
  function getItemIcon(icon, label) {
    var iconNode = icon || /*#__PURE__*/_react.default.createElement("button", {
      type: "button",
      "aria-label": label,
      className: "".concat(prefixCls, "-item-link")
    });
    if (typeof icon === 'function') {
      iconNode = /*#__PURE__*/_react.default.createElement(icon, (0, _objectSpread2.default)({}, props));
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
    if (event.keyCode === _KeyCode.default.UP || event.keyCode === _KeyCode.default.DOWN) {
      event.preventDefault();
    }
  }
  function handleKeyUp(event) {
    var value = getValidValue(event);
    if (value !== internalInputVal) {
      setInternalInputVal(value);
    }
    switch (event.keyCode) {
      case _KeyCode.default.ENTER:
        handleChange(value);
        break;
      case _KeyCode.default.UP:
        handleChange(value - 1);
        break;
      case _KeyCode.default.DOWN:
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
    if (event.key === 'Enter' || event.charCode === _KeyCode.default.ENTER || event.keyCode === _KeyCode.default.ENTER) {
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
    return /*#__PURE__*/_react.default.isValidElement(prevButton) ? /*#__PURE__*/_react.default.cloneElement(prevButton, {
      disabled: !hasPrev
    }) : prevButton;
  }
  function renderNext(nextPage) {
    var nextButton = itemRender(nextPage, 'next', getItemIcon(nextIcon, 'next page'));
    return /*#__PURE__*/_react.default.isValidElement(nextButton) ? /*#__PURE__*/_react.default.cloneElement(nextButton, {
      disabled: !hasNext
    }) : nextButton;
  }
  function handleGoTO(event) {
    if (event.type === 'click' || event.keyCode === _KeyCode.default.ENTER) {
      handleChange(internalInputVal);
    }
  }
  var jumpPrev = null;
  var dataOrAriaAttributeProps = (0, _pickAttrs.default)(props, {
    aria: true,
    data: true
  });
  var totalText = showTotal && /*#__PURE__*/_react.default.createElement("li", {
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
        gotoButton = /*#__PURE__*/_react.default.createElement("button", {
          type: "button",
          onClick: handleGoTO,
          onKeyUp: handleGoTO
        }, locale.jump_to_confirm);
      } else {
        gotoButton = /*#__PURE__*/_react.default.createElement("span", {
          onClick: handleGoTO,
          onKeyUp: handleGoTO
        }, goButton);
      }
      gotoButton = /*#__PURE__*/_react.default.createElement("li", {
        title: showTitle ? "".concat(locale.jump_to).concat(current, "/").concat(allPages) : null,
        className: "".concat(prefixCls, "-simple-pager")
      }, gotoButton);
    }
    simplePager = /*#__PURE__*/_react.default.createElement("li", {
      title: showTitle ? "".concat(current, "/").concat(allPages) : null,
      className: "".concat(prefixCls, "-simple-pager")
    }, /*#__PURE__*/_react.default.createElement("input", {
      type: "text",
      value: internalInputVal,
      disabled: disabled,
      onKeyDown: handleKeyDown,
      onKeyUp: handleKeyUp,
      onChange: handleKeyUp,
      onBlur: handleBlur,
      size: 3
    }), /*#__PURE__*/_react.default.createElement("span", {
      className: "".concat(prefixCls, "-slash")
    }, "/"), allPages);
  }

  // ====================== Normal ======================
  var pageBufferSize = showLessItems ? 1 : 2;
  if (allPages <= 3 + pageBufferSize * 2) {
    if (!allPages) {
      pagerList.push( /*#__PURE__*/_react.default.createElement(_Pager.default, (0, _extends2.default)({}, pagerProps, {
        key: "noPager",
        page: 1,
        className: "".concat(prefixCls, "-item-disabled")
      })));
    }
    for (var i = 1; i <= allPages; i += 1) {
      pagerList.push( /*#__PURE__*/_react.default.createElement(_Pager.default, (0, _extends2.default)({}, pagerProps, {
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
      jumpPrev = jumpPrevContent ? /*#__PURE__*/_react.default.createElement("li", {
        title: showTitle ? prevItemTitle : null,
        key: "prev",
        onClick: jumpPrevHandle,
        tabIndex: 0,
        onKeyDown: runIfEnterJumpPrev,
        className: (0, _classnames.default)("".concat(prefixCls, "-jump-prev"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-jump-prev-custom-icon"), !!jumpPrevIcon))
      }, jumpPrevContent) : null;
      jumpNext = jumpNextContent ? /*#__PURE__*/_react.default.createElement("li", {
        title: showTitle ? nextItemTitle : null,
        key: "next",
        onClick: jumpNextHandle,
        tabIndex: 0,
        onKeyDown: runIfEnterJumpNext,
        className: (0, _classnames.default)("".concat(prefixCls, "-jump-next"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-jump-next-custom-icon"), !!jumpNextIcon))
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
      pagerList.push( /*#__PURE__*/_react.default.createElement(_Pager.default, (0, _extends2.default)({}, pagerProps, {
        key: _i,
        page: _i,
        active: current === _i
      })));
    }
    if (current - 1 >= pageBufferSize * 2 && current !== 1 + 2) {
      pagerList[0] = /*#__PURE__*/_react.default.cloneElement(pagerList[0], {
        className: (0, _classnames.default)("".concat(prefixCls, "-item-after-jump-prev"), pagerList[0].props.className)
      });
      pagerList.unshift(jumpPrev);
    }
    if (allPages - current >= pageBufferSize * 2 && current !== allPages - 2) {
      var lastOne = pagerList[pagerList.length - 1];
      pagerList[pagerList.length - 1] = /*#__PURE__*/_react.default.cloneElement(lastOne, {
        className: (0, _classnames.default)("".concat(prefixCls, "-item-before-jump-next"), lastOne.props.className)
      });
      pagerList.push(jumpNext);
    }
    if (left !== 1) {
      pagerList.unshift( /*#__PURE__*/_react.default.createElement(_Pager.default, (0, _extends2.default)({}, pagerProps, {
        key: 1,
        page: 1
      })));
    }
    if (right !== allPages) {
      pagerList.push( /*#__PURE__*/_react.default.createElement(_Pager.default, (0, _extends2.default)({}, pagerProps, {
        key: allPages,
        page: allPages
      })));
    }
  }
  var prev = renderPrev(prevPage);
  if (prev) {
    var prevDisabled = !hasPrev || !allPages;
    prev = /*#__PURE__*/_react.default.createElement("li", {
      title: showTitle ? locale.prev_page : null,
      onClick: prevHandle,
      tabIndex: prevDisabled ? null : 0,
      onKeyDown: runIfEnterPrev,
      className: (0, _classnames.default)("".concat(prefixCls, "-prev"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-disabled"), prevDisabled)),
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
    next = /*#__PURE__*/_react.default.createElement("li", {
      title: showTitle ? locale.next_page : null,
      onClick: nextHandle,
      tabIndex: nextTabIndex,
      onKeyDown: runIfEnterNext,
      className: (0, _classnames.default)("".concat(prefixCls, "-next"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-disabled"), nextDisabled)),
      "aria-disabled": nextDisabled
    }, next);
  }
  var cls = (0, _classnames.default)(prefixCls, className, (_classNames5 = {}, (0, _defineProperty2.default)(_classNames5, "".concat(prefixCls, "-simple"), simple), (0, _defineProperty2.default)(_classNames5, "".concat(prefixCls, "-disabled"), disabled), _classNames5));
  return /*#__PURE__*/_react.default.createElement("ul", (0, _extends2.default)({
    className: cls,
    style: style,
    ref: paginationRef
  }, dataOrAriaAttributeProps), totalText, prev, simple ? simplePager : pagerList, next, /*#__PURE__*/_react.default.createElement(_Options.default, {
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
var _default = exports.default = Pagination;