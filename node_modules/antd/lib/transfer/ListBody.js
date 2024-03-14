"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.OmitProps = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _useMergedState = _interopRequireDefault(require("rc-util/lib/hooks/useMergedState"));
var _pagination = _interopRequireDefault(require("../pagination"));
var _ListItem = _interopRequireDefault(require("./ListItem"));
const OmitProps = exports.OmitProps = ['handleFilter', 'handleClear', 'checkedKeys'];
const parsePagination = pagination => {
  const defaultPagination = {
    simple: true,
    showSizeChanger: false,
    showLessItems: false
  };
  return Object.assign(Object.assign({}, defaultPagination), pagination);
};
const TransferListBody = (props, ref) => {
  const {
    prefixCls,
    filteredRenderItems,
    selectedKeys,
    disabled: globalDisabled,
    showRemove,
    pagination,
    onScroll,
    onItemSelect,
    onItemRemove
  } = props;
  const [current, setCurrent] = React.useState(1);
  const mergedPagination = React.useMemo(() => {
    if (!pagination) {
      return null;
    }
    const convertPagination = typeof pagination === 'object' ? pagination : {};
    return parsePagination(convertPagination);
  }, [pagination]);
  const [pageSize, setPageSize] = (0, _useMergedState.default)(10, {
    value: mergedPagination === null || mergedPagination === void 0 ? void 0 : mergedPagination.pageSize
  });
  React.useEffect(() => {
    if (mergedPagination) {
      const maxPageCount = Math.ceil(filteredRenderItems.length / pageSize);
      setCurrent(Math.min(current, maxPageCount));
    }
  }, [filteredRenderItems, mergedPagination, pageSize]);
  const onInternalClick = (item, e) => {
    onItemSelect(item.key, !selectedKeys.includes(item.key), e);
  };
  const onRemove = item => {
    onItemRemove === null || onItemRemove === void 0 ? void 0 : onItemRemove([item.key]);
  };
  const onPageChange = cur => {
    setCurrent(cur);
  };
  const onSizeChange = (cur, size) => {
    setCurrent(cur);
    setPageSize(size);
  };
  const memoizedItems = React.useMemo(() => {
    const displayItems = mergedPagination ? filteredRenderItems.slice((current - 1) * pageSize, current * pageSize) : filteredRenderItems;
    return displayItems;
  }, [current, filteredRenderItems, mergedPagination, pageSize]);
  React.useImperativeHandle(ref, () => ({
    items: memoizedItems
  }));
  const paginationNode = mergedPagination ? ( /*#__PURE__*/React.createElement(_pagination.default, {
    size: "small",
    disabled: globalDisabled,
    simple: mergedPagination.simple,
    pageSize: pageSize,
    showLessItems: mergedPagination.showLessItems,
    showSizeChanger: mergedPagination.showSizeChanger,
    className: `${prefixCls}-pagination`,
    total: filteredRenderItems.length,
    current: current,
    onChange: onPageChange,
    onShowSizeChange: onSizeChange
  })) : null;
  const cls = (0, _classnames.default)(`${prefixCls}-content`, {
    [`${prefixCls}-content-show-remove`]: showRemove
  });
  return /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("ul", {
    className: cls,
    onScroll: onScroll
  }, (memoizedItems || []).map(_ref => {
    let {
      renderedEl,
      renderedText,
      item
    } = _ref;
    return /*#__PURE__*/React.createElement(_ListItem.default, {
      key: item.key,
      item: item,
      renderedText: renderedText,
      renderedEl: renderedEl,
      prefixCls: prefixCls,
      showRemove: showRemove,
      onClick: onInternalClick,
      onRemove: onRemove,
      checked: selectedKeys.includes(item.key),
      disabled: globalDisabled || item.disabled
    });
  })), paginationNode);
};
if (process.env.NODE_ENV !== 'production') {
  TransferListBody.displayName = 'TransferListBody';
}
var _default = exports.default = /*#__PURE__*/React.forwardRef(TransferListBody);