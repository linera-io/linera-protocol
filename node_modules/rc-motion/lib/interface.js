"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.STEP_START = exports.STEP_PREPARED = exports.STEP_PREPARE = exports.STEP_NONE = exports.STEP_ACTIVE = exports.STEP_ACTIVATED = exports.STATUS_NONE = exports.STATUS_LEAVE = exports.STATUS_ENTER = exports.STATUS_APPEAR = void 0;
var STATUS_NONE = 'none';
exports.STATUS_NONE = STATUS_NONE;
var STATUS_APPEAR = 'appear';
exports.STATUS_APPEAR = STATUS_APPEAR;
var STATUS_ENTER = 'enter';
exports.STATUS_ENTER = STATUS_ENTER;
var STATUS_LEAVE = 'leave';
exports.STATUS_LEAVE = STATUS_LEAVE;
var STEP_NONE = 'none';
exports.STEP_NONE = STEP_NONE;
var STEP_PREPARE = 'prepare';
exports.STEP_PREPARE = STEP_PREPARE;
var STEP_START = 'start';
exports.STEP_START = STEP_START;
var STEP_ACTIVE = 'active';
exports.STEP_ACTIVE = STEP_ACTIVE;
var STEP_ACTIVATED = 'end';
/**
 * Used for disabled motion case.
 * Prepare stage will still work but start & active will be skipped.
 */
exports.STEP_ACTIVATED = STEP_ACTIVATED;
var STEP_PREPARED = 'prepared';
exports.STEP_PREPARED = STEP_PREPARED;