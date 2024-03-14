"use client";

import * as React from 'react';
import classNames from 'classnames';
import useMergedState from "rc-util/es/hooks/useMergedState";
import Pagination from '../pagination';
import ListItem from './ListItem';
export const OmitProps = ['handleFilter', 'handleClear', 'checkedKeys'];
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
  const [pageSize, setPageSize] = useMergedState(10, {
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
  const paginationNode = mergedPagination ? ( /*#__PURE__*/React.createElement(Pagination, {
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
  const cls = classNames(`${prefixCls}-content`, {
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
    return /*#__PURE__*/React.createElement(ListItem, {
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
export default /*#__PURE__*/React.forwardRef(TransferListBody);