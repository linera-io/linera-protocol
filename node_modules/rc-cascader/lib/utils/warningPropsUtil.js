"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
exports.warningNullOptions = warningNullOptions;
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
function warningProps(props) {
  var onPopupVisibleChange = props.onPopupVisibleChange,
    popupVisible = props.popupVisible,
    popupClassName = props.popupClassName,
    popupPlacement = props.popupPlacement;
  (0, _warning.default)(!onPopupVisibleChange, '`onPopupVisibleChange` is deprecated. Please use `onDropdownVisibleChange` instead.');
  (0, _warning.default)(popupVisible === undefined, '`popupVisible` is deprecated. Please use `open` instead.');
  (0, _warning.default)(popupClassName === undefined, '`popupClassName` is deprecated. Please use `dropdownClassName` instead.');
  (0, _warning.default)(popupPlacement === undefined, '`popupPlacement` is deprecated. Please use `placement` instead.');
}

// value in Cascader options should not be null
function warningNullOptions(options, fieldNames) {
  if (options) {
    var recursiveOptions = function recursiveOptions(optionsList) {
      for (var i = 0; i < optionsList.length; i++) {
        var option = optionsList[i];
        if (option[fieldNames === null || fieldNames === void 0 ? void 0 : fieldNames.value] === null) {
          (0, _warning.default)(false, '`value` in Cascader options should not be `null`.');
          return true;
        }
        if (Array.isArray(option[fieldNames === null || fieldNames === void 0 ? void 0 : fieldNames.children]) && recursiveOptions(option[fieldNames === null || fieldNames === void 0 ? void 0 : fieldNames.children])) {
          return true;
        }
      }
    };
    recursiveOptions(options);
  }
}
var _default = exports.default = warningProps;