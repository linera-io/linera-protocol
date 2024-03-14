"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getPlacements = getPlacements;
exports.placements = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var autoAdjustOverflow = {
  adjustX: 1,
  adjustY: 1
};
var targetOffset = [0, 0];
var basePlacements = {
  left: {
    points: ['cr', 'cl'],
    offset: [-8, 0]
  },
  right: {
    points: ['cl', 'cr'],
    offset: [8, 0]
  },
  top: {
    points: ['bc', 'tc'],
    offset: [0, -8]
  },
  bottom: {
    points: ['tc', 'bc'],
    offset: [0, 8]
  },
  topLeft: {
    points: ['bl', 'tl'],
    offset: [0, -8]
  },
  leftTop: {
    points: ['tr', 'tl'],
    offset: [-8, 0]
  },
  topRight: {
    points: ['br', 'tr'],
    offset: [0, -8]
  },
  rightTop: {
    points: ['tl', 'tr'],
    offset: [8, 0]
  },
  bottomRight: {
    points: ['tr', 'br'],
    offset: [0, 8]
  },
  rightBottom: {
    points: ['bl', 'br'],
    offset: [8, 0]
  },
  bottomLeft: {
    points: ['tl', 'bl'],
    offset: [0, 8]
  },
  leftBottom: {
    points: ['br', 'bl'],
    offset: [-8, 0]
  }
};
function getPlacements() {
  var arrowPointAtCenter = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
  var placements = {};
  Object.keys(basePlacements).forEach(function (key) {
    placements[key] = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, basePlacements[key]), {}, {
      autoArrow: arrowPointAtCenter,
      targetOffset: targetOffset
    });
  });
  return placements;
}
var placements = exports.placements = getPlacements();