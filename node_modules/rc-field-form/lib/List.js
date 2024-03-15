"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _FieldContext = _interopRequireDefault(require("./FieldContext"));
var _Field = _interopRequireDefault(require("./Field"));
var _valueUtil = require("./utils/valueUtil");
var _ListContext = _interopRequireDefault(require("./ListContext"));
function List(_ref) {
  var name = _ref.name,
    initialValue = _ref.initialValue,
    children = _ref.children,
    rules = _ref.rules,
    validateTrigger = _ref.validateTrigger,
    isListField = _ref.isListField;
  var context = React.useContext(_FieldContext.default);
  var wrapperListContext = React.useContext(_ListContext.default);
  var keyRef = React.useRef({
    keys: [],
    id: 0
  });
  var keyManager = keyRef.current;
  var prefixName = React.useMemo(function () {
    var parentPrefixName = (0, _valueUtil.getNamePath)(context.prefixName) || [];
    return [].concat((0, _toConsumableArray2.default)(parentPrefixName), (0, _toConsumableArray2.default)((0, _valueUtil.getNamePath)(name)));
  }, [context.prefixName, name]);
  var fieldContext = React.useMemo(function () {
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, context), {}, {
      prefixName: prefixName
    });
  }, [context, prefixName]);

  // List context
  var listContext = React.useMemo(function () {
    return {
      getKey: function getKey(namePath) {
        var len = prefixName.length;
        var pathName = namePath[len];
        return [keyManager.keys[pathName], namePath.slice(len + 1)];
      }
    };
  }, [prefixName]);

  // User should not pass `children` as other type.
  if (typeof children !== 'function') {
    (0, _warning.default)(false, 'Form.List only accepts function as children.');
    return null;
  }
  var shouldUpdate = function shouldUpdate(prevValue, nextValue, _ref2) {
    var source = _ref2.source;
    if (source === 'internal') {
      return false;
    }
    return prevValue !== nextValue;
  };
  return /*#__PURE__*/React.createElement(_ListContext.default.Provider, {
    value: listContext
  }, /*#__PURE__*/React.createElement(_FieldContext.default.Provider, {
    value: fieldContext
  }, /*#__PURE__*/React.createElement(_Field.default, {
    name: [],
    shouldUpdate: shouldUpdate,
    rules: rules,
    validateTrigger: validateTrigger,
    initialValue: initialValue,
    isList: true,
    isListField: isListField !== null && isListField !== void 0 ? isListField : !!wrapperListContext
  }, function (_ref3, meta) {
    var _ref3$value = _ref3.value,
      value = _ref3$value === void 0 ? [] : _ref3$value,
      onChange = _ref3.onChange;
    var getFieldValue = context.getFieldValue;
    var getNewValue = function getNewValue() {
      var values = getFieldValue(prefixName || []);
      return values || [];
    };
    /**
     * Always get latest value in case user update fields by `form` api.
     */
    var operations = {
      add: function add(defaultValue, index) {
        // Mapping keys
        var newValue = getNewValue();
        if (index >= 0 && index <= newValue.length) {
          keyManager.keys = [].concat((0, _toConsumableArray2.default)(keyManager.keys.slice(0, index)), [keyManager.id], (0, _toConsumableArray2.default)(keyManager.keys.slice(index)));
          onChange([].concat((0, _toConsumableArray2.default)(newValue.slice(0, index)), [defaultValue], (0, _toConsumableArray2.default)(newValue.slice(index))));
        } else {
          if (process.env.NODE_ENV !== 'production' && (index < 0 || index > newValue.length)) {
            (0, _warning.default)(false, 'The second parameter of the add function should be a valid positive number.');
          }
          keyManager.keys = [].concat((0, _toConsumableArray2.default)(keyManager.keys), [keyManager.id]);
          onChange([].concat((0, _toConsumableArray2.default)(newValue), [defaultValue]));
        }
        keyManager.id += 1;
      },
      remove: function remove(index) {
        var newValue = getNewValue();
        var indexSet = new Set(Array.isArray(index) ? index : [index]);
        if (indexSet.size <= 0) {
          return;
        }
        keyManager.keys = keyManager.keys.filter(function (_, keysIndex) {
          return !indexSet.has(keysIndex);
        });

        // Trigger store change
        onChange(newValue.filter(function (_, valueIndex) {
          return !indexSet.has(valueIndex);
        }));
      },
      move: function move(from, to) {
        if (from === to) {
          return;
        }
        var newValue = getNewValue();

        // Do not handle out of range
        if (from < 0 || from >= newValue.length || to < 0 || to >= newValue.length) {
          return;
        }
        keyManager.keys = (0, _valueUtil.move)(keyManager.keys, from, to);

        // Trigger store change
        onChange((0, _valueUtil.move)(newValue, from, to));
      }
    };
    var listValue = value || [];
    if (!Array.isArray(listValue)) {
      listValue = [];
      if (process.env.NODE_ENV !== 'production') {
        (0, _warning.default)(false, "Current value of '".concat(prefixName.join(' > '), "' is not an array type."));
      }
    }
    return children(listValue.map(function (__, index) {
      var key = keyManager.keys[index];
      if (key === undefined) {
        keyManager.keys[index] = keyManager.id;
        key = keyManager.keys[index];
        keyManager.id += 1;
      }
      return {
        name: index,
        key: key,
        isListField: true
      };
    }), operations, meta);
  })));
}
var _default = exports.default = List;