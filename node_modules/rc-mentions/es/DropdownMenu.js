import Menu, { MenuItem } from 'rc-menu';
import * as React from 'react';
import MentionsContext from "./MentionsContext";
/**
 * We only use Menu to display the candidate.
 * The focus is controlled by textarea to make accessibility easy.
 */
function DropdownMenu(props) {
  var _React$useContext = React.useContext(MentionsContext),
    notFoundContent = _React$useContext.notFoundContent,
    activeIndex = _React$useContext.activeIndex,
    setActiveIndex = _React$useContext.setActiveIndex,
    selectOption = _React$useContext.selectOption,
    onFocus = _React$useContext.onFocus,
    onBlur = _React$useContext.onBlur;
  var prefixCls = props.prefixCls,
    options = props.options;
  var activeOption = options[activeIndex] || {};
  return /*#__PURE__*/React.createElement(Menu, {
    prefixCls: "".concat(prefixCls, "-menu"),
    activeKey: activeOption.key,
    onSelect: function onSelect(_ref) {
      var key = _ref.key;
      var option = options.find(function (_ref2) {
        var optionKey = _ref2.key;
        return optionKey === key;
      });
      selectOption(option);
    },
    onFocus: onFocus,
    onBlur: onBlur
  }, options.map(function (option, index) {
    var key = option.key,
      disabled = option.disabled,
      className = option.className,
      style = option.style,
      label = option.label;
    return /*#__PURE__*/React.createElement(MenuItem, {
      key: key,
      disabled: disabled,
      className: className,
      style: style,
      onMouseEnter: function onMouseEnter() {
        setActiveIndex(index);
      }
    }, label);
  }), !options.length && /*#__PURE__*/React.createElement(MenuItem, {
    disabled: true
  }, notFoundContent));
}
export default DropdownMenu;