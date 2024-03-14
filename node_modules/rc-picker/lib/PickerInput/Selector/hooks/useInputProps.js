"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useInputProps;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _rcUtil = require("rc-util");
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var React = _interopRequireWildcard(require("react"));
var _dateUtil = require("../../../utils/dateUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
function useInputProps(props, /** Used for SinglePicker */
postProps) {
  var format = props.format,
    maskFormat = props.maskFormat,
    generateConfig = props.generateConfig,
    locale = props.locale,
    preserveInvalidOnBlur = props.preserveInvalidOnBlur,
    inputReadOnly = props.inputReadOnly,
    required = props.required,
    ariaRequired = props['aria-required'],
    onSubmit = props.onSubmit,
    _onFocus = props.onFocus,
    _onBlur = props.onBlur,
    onInputChange = props.onInputChange,
    onInvalid = props.onInvalid,
    open = props.open,
    onOpenChange = props.onOpenChange,
    _onKeyDown = props.onKeyDown,
    _onChange = props.onChange,
    activeHelp = props.activeHelp,
    name = props.name,
    autoComplete = props.autoComplete,
    id = props.id,
    value = props.value,
    invalid = props.invalid,
    placeholder = props.placeholder,
    disabled = props.disabled,
    activeIndex = props.activeIndex,
    allHelp = props.allHelp,
    picker = props.picker;

  // ======================== Parser ========================
  var parseDate = function parseDate(str, formatStr) {
    var parsed = generateConfig.locale.parse(locale.locale, str, [formatStr]);
    return parsed && generateConfig.isValidate(parsed) ? parsed : null;
  };

  // ========================= Text =========================
  var firstFormat = format[0];
  var getText = React.useCallback(function (date) {
    return (0, _dateUtil.formatValue)(date, {
      locale: locale,
      format: firstFormat,
      generateConfig: generateConfig
    });
  }, [locale, generateConfig, firstFormat]);
  var valueTexts = React.useMemo(function () {
    return value.map(getText);
  }, [value, getText]);

  // ========================= Size =========================
  var size = React.useMemo(function () {
    var defaultSize = picker === 'time' ? 8 : 10;
    var length = typeof firstFormat === 'function' ? firstFormat(generateConfig.getNow()).length : firstFormat.length;
    return Math.max(defaultSize, length) + 2;
  }, [firstFormat, picker, generateConfig]);

  // ======================= Validate =======================
  var _validateFormat = function validateFormat(text) {
    for (var i = 0; i < format.length; i += 1) {
      var singleFormat = format[i];

      // Only support string type
      if (typeof singleFormat === 'string') {
        var parsed = parseDate(text, singleFormat);
        if (parsed) {
          return parsed;
        }
      }
    }
    return false;
  };

  // ======================== Input =========================
  var getInputProps = function getInputProps(index) {
    function getProp(propValue) {
      return index !== undefined ? propValue[index] : propValue;
    }
    var pickedAttrs = (0, _pickAttrs.default)(props, {
      aria: true,
      data: true
    });
    var inputProps = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, pickedAttrs), {}, {
      // ============== Shared ==============
      format: maskFormat,
      validateFormat: function validateFormat(text) {
        return !!_validateFormat(text);
      },
      preserveInvalidOnBlur: preserveInvalidOnBlur,
      readOnly: inputReadOnly,
      required: required,
      'aria-required': ariaRequired,
      name: name,
      autoComplete: autoComplete,
      size: size,
      // ============= By Index =============
      id: getProp(id),
      value: getProp(valueTexts) || '',
      invalid: getProp(invalid),
      placeholder: getProp(placeholder),
      active: activeIndex === index,
      helped: allHelp || activeHelp && activeIndex === index,
      disabled: getProp(disabled),
      onFocus: function onFocus(event) {
        _onFocus(event, index);
      },
      onBlur: function onBlur(event) {
        // Blur do not trigger close
        // Since it may focus to the popup panel
        _onBlur(event, index);
      },
      onSubmit: onSubmit,
      // Get validate text value
      onChange: function onChange(text) {
        onInputChange();
        var parsed = _validateFormat(text);
        if (parsed) {
          onInvalid(false, index);
          _onChange(parsed, index);
          return;
        }

        // Tell outer that the value typed is invalid.
        // If text is empty, it means valid.
        onInvalid(!!text, index);
      },
      onHelp: function onHelp() {
        onOpenChange(true, {
          index: index
        });
      },
      onKeyDown: function onKeyDown(event) {
        var prevented = false;
        _onKeyDown === null || _onKeyDown === void 0 || _onKeyDown(event, function () {
          if (process.env.NODE_ENV !== 'production') {
            (0, _rcUtil.warning)(false, '`preventDefault` callback is deprecated. Please call `event.preventDefault` directly.');
          }
          prevented = true;
        });
        if (!event.defaultPrevented && !prevented) {
          switch (event.key) {
            case 'Escape':
              onOpenChange(false, {
                index: index
              });
              break;
            case 'Enter':
              if (!open) {
                onOpenChange(true);
              }
              break;
          }
        }
      }
    }, postProps === null || postProps === void 0 ? void 0 : postProps({
      valueTexts: valueTexts
    }));

    // ============== Clean Up ==============
    Object.keys(inputProps).forEach(function (key) {
      if (inputProps[key] === undefined) {
        delete inputProps[key];
      }
    });
    return inputProps;
  };
  return [getInputProps, getText];
}