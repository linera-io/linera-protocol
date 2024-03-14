"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.operationUnit = void 0;
// eslint-disable-next-line import/prefer-default-export
const operationUnit = token => ({
  // FIXME: This use link but is a operation unit. Seems should be a colorPrimary.
  // And Typography use this to generate link style which should not do this.
  color: token.colorLink,
  textDecoration: 'none',
  outline: 'none',
  cursor: 'pointer',
  transition: `color ${token.motionDurationSlow}`,
  '&:focus, &:hover': {
    color: token.colorLinkHover
  },
  '&:active': {
    color: token.colorLinkActive
  }
});
exports.operationUnit = operationUnit;