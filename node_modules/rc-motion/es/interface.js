export var STATUS_NONE = 'none';
export var STATUS_APPEAR = 'appear';
export var STATUS_ENTER = 'enter';
export var STATUS_LEAVE = 'leave';
export var STEP_NONE = 'none';
export var STEP_PREPARE = 'prepare';
export var STEP_START = 'start';
export var STEP_ACTIVE = 'active';
export var STEP_ACTIVATED = 'end';
/**
 * Used for disabled motion case.
 * Prepare stage will still work but start & active will be skipped.
 */
export var STEP_PREPARED = 'prepared';