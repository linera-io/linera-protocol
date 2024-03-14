"use client";

import { genVirtualTable } from 'rc-table';
/**
 * Same as `rc-table` but we modify trigger children update logic instead.
 */
export default genVirtualTable((prev, next) => {
  const {
    _renderTimes: prevRenderTimes
  } = prev;
  const {
    _renderTimes: nextRenderTimes
  } = next;
  return prevRenderTimes !== nextRenderTimes;
});