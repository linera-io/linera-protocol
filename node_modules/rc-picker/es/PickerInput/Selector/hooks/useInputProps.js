import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import { warning } from 'rc-util';
import pickAttrs from "rc-util/es/pickAttrs";
import * as React from 'react';
import { formatValue } from "../../../utils/dateUtil";
export default function useInputProps(props, /** Used for SinglePicker */
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
    return formatValue(date, {
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
    var pickedAttrs = pickAttrs(props, {
      aria: true,
      data: true
    });
    var inputProps = _objectSpread(_objectSpread({}, pickedAttrs), {}, {
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
            warning(false, '`preventDefault` callback is deprecated. Please call `event.preventDefault` directly.');
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