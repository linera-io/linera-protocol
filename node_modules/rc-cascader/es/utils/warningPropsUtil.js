import warning from "rc-util/es/warning";
function warningProps(props) {
  var onPopupVisibleChange = props.onPopupVisibleChange,
    popupVisible = props.popupVisible,
    popupClassName = props.popupClassName,
    popupPlacement = props.popupPlacement;
  warning(!onPopupVisibleChange, '`onPopupVisibleChange` is deprecated. Please use `onDropdownVisibleChange` instead.');
  warning(popupVisible === undefined, '`popupVisible` is deprecated. Please use `open` instead.');
  warning(popupClassName === undefined, '`popupClassName` is deprecated. Please use `dropdownClassName` instead.');
  warning(popupPlacement === undefined, '`popupPlacement` is deprecated. Please use `placement` instead.');
}

// value in Cascader options should not be null
export function warningNullOptions(options, fieldNames) {
  if (options) {
    var recursiveOptions = function recursiveOptions(optionsList) {
      for (var i = 0; i < optionsList.length; i++) {
        var option = optionsList[i];
        if (option[fieldNames === null || fieldNames === void 0 ? void 0 : fieldNames.value] === null) {
          warning(false, '`value` in Cascader options should not be `null`.');
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
export default warningProps;