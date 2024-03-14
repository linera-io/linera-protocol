"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isBodyOverflowing = isBodyOverflowing;
/**
 * Test usage export. Do not use in your production
 */
function isBodyOverflowing() {
  return document.body.scrollHeight > (window.innerHeight || document.documentElement.clientHeight) && window.innerWidth > document.body.offsetWidth;
}