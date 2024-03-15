"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var FORMAT_KEYS = ['YYYY', 'MM', 'DD', 'HH', 'mm', 'ss', 'SSS'];
// Use Chinese character to avoid conflict with the mask format
var REPLACE_KEY = '顧';
var MaskFormat = exports.default = /*#__PURE__*/function () {
  function MaskFormat(format) {
    (0, _classCallCheck2.default)(this, MaskFormat);
    (0, _defineProperty2.default)(this, "format", void 0);
    (0, _defineProperty2.default)(this, "maskFormat", void 0);
    (0, _defineProperty2.default)(this, "cells", void 0);
    (0, _defineProperty2.default)(this, "maskCells", void 0);
    this.format = format;

    // Generate mask format
    var replaceKeys = FORMAT_KEYS.map(function (key) {
      return "(".concat(key, ")");
    }).join('|');
    var replaceReg = new RegExp(replaceKeys, 'g');
    this.maskFormat = format.replace(replaceReg,
    // Use Chinese character to avoid user use it in format
    function (key) {
      return REPLACE_KEY.repeat(key.length);
    });

    // Generate cells
    var cellReg = new RegExp("(".concat(FORMAT_KEYS.join('|'), ")"));
    var strCells = (format.split(cellReg) || []).filter(function (str) {
      return str;
    });
    var offset = 0;
    this.cells = strCells.map(function (text) {
      var mask = FORMAT_KEYS.includes(text);
      var start = offset;
      var end = offset + text.length;
      offset = end;
      return {
        text: text,
        mask: mask,
        start: start,
        end: end
      };
    });

    // Mask cells
    this.maskCells = this.cells.filter(function (cell) {
      return cell.mask;
    });
  }
  (0, _createClass2.default)(MaskFormat, [{
    key: "getSelection",
    value: function getSelection(maskCellIndex) {
      var _ref = this.maskCells[maskCellIndex] || {},
        start = _ref.start,
        end = _ref.end;
      return [start || 0, end || 0];
    }

    /** Check given text match format */
  }, {
    key: "match",
    value: function match(text) {
      for (var i = 0; i < this.maskFormat.length; i += 1) {
        var maskChar = this.maskFormat[i];
        var textChar = text[i];
        if (!textChar || maskChar !== REPLACE_KEY && maskChar !== textChar) {
          return false;
        }
      }
      return true;
    }

    /** Get mask cell count */
  }, {
    key: "size",
    value: function size() {
      return this.maskCells.length;
    }
  }, {
    key: "getMaskCellIndex",
    value: function getMaskCellIndex(anchorIndex) {
      var closetDist = Number.MAX_SAFE_INTEGER;
      var closetIndex = 0;
      for (var i = 0; i < this.maskCells.length; i += 1) {
        var _this$maskCells$i = this.maskCells[i],
          start = _this$maskCells$i.start,
          end = _this$maskCells$i.end;
        if (anchorIndex >= start && anchorIndex <= end) {
          return i;
        }
        var dist = Math.min(Math.abs(anchorIndex - start), Math.abs(anchorIndex - end));
        if (dist < closetDist) {
          closetDist = dist;
          closetIndex = i;
        }
      }
      return closetIndex;
    }
  }]);
  return MaskFormat;
}();