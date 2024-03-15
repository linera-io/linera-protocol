"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = exports.FormStore = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _set = require("rc-util/lib/utils/set");
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var React = _interopRequireWildcard(require("react"));
var _FieldContext = require("./FieldContext");
var _asyncUtil = require("./utils/asyncUtil");
var _messages = require("./utils/messages");
var _NameMap = _interopRequireDefault(require("./utils/NameMap"));
var _valueUtil = require("./utils/valueUtil");
var _excluded = ["name"];
var FormStore = exports.FormStore = /*#__PURE__*/(0, _createClass2.default)(function FormStore(forceRootUpdate) {
  var _this = this;
  (0, _classCallCheck2.default)(this, FormStore);
  (0, _defineProperty2.default)(this, "formHooked", false);
  (0, _defineProperty2.default)(this, "forceRootUpdate", void 0);
  (0, _defineProperty2.default)(this, "subscribable", true);
  (0, _defineProperty2.default)(this, "store", {});
  (0, _defineProperty2.default)(this, "fieldEntities", []);
  (0, _defineProperty2.default)(this, "initialValues", {});
  (0, _defineProperty2.default)(this, "callbacks", {});
  (0, _defineProperty2.default)(this, "validateMessages", null);
  (0, _defineProperty2.default)(this, "preserve", null);
  (0, _defineProperty2.default)(this, "lastValidatePromise", null);
  (0, _defineProperty2.default)(this, "getForm", function () {
    return {
      getFieldValue: _this.getFieldValue,
      getFieldsValue: _this.getFieldsValue,
      getFieldError: _this.getFieldError,
      getFieldWarning: _this.getFieldWarning,
      getFieldsError: _this.getFieldsError,
      isFieldsTouched: _this.isFieldsTouched,
      isFieldTouched: _this.isFieldTouched,
      isFieldValidating: _this.isFieldValidating,
      isFieldsValidating: _this.isFieldsValidating,
      resetFields: _this.resetFields,
      setFields: _this.setFields,
      setFieldValue: _this.setFieldValue,
      setFieldsValue: _this.setFieldsValue,
      validateFields: _this.validateFields,
      submit: _this.submit,
      _init: true,
      getInternalHooks: _this.getInternalHooks
    };
  });
  // ======================== Internal Hooks ========================
  (0, _defineProperty2.default)(this, "getInternalHooks", function (key) {
    if (key === _FieldContext.HOOK_MARK) {
      _this.formHooked = true;
      return {
        dispatch: _this.dispatch,
        initEntityValue: _this.initEntityValue,
        registerField: _this.registerField,
        useSubscribe: _this.useSubscribe,
        setInitialValues: _this.setInitialValues,
        destroyForm: _this.destroyForm,
        setCallbacks: _this.setCallbacks,
        setValidateMessages: _this.setValidateMessages,
        getFields: _this.getFields,
        setPreserve: _this.setPreserve,
        getInitialValue: _this.getInitialValue,
        registerWatch: _this.registerWatch
      };
    }
    (0, _warning.default)(false, '`getInternalHooks` is internal usage. Should not call directly.');
    return null;
  });
  (0, _defineProperty2.default)(this, "useSubscribe", function (subscribable) {
    _this.subscribable = subscribable;
  });
  /**
   * Record prev Form unmount fieldEntities which config preserve false.
   * This need to be refill with initialValues instead of store value.
   */
  (0, _defineProperty2.default)(this, "prevWithoutPreserves", null);
  /**
   * First time `setInitialValues` should update store with initial value
   */
  (0, _defineProperty2.default)(this, "setInitialValues", function (initialValues, init) {
    _this.initialValues = initialValues || {};
    if (init) {
      var _this$prevWithoutPres;
      var nextStore = (0, _set.merge)(initialValues, _this.store);

      // We will take consider prev form unmount fields.
      // When the field is not `preserve`, we need fill this with initialValues instead of store.
      // eslint-disable-next-line array-callback-return
      (_this$prevWithoutPres = _this.prevWithoutPreserves) === null || _this$prevWithoutPres === void 0 || _this$prevWithoutPres.map(function (_ref) {
        var namePath = _ref.key;
        nextStore = (0, _valueUtil.setValue)(nextStore, namePath, (0, _valueUtil.getValue)(initialValues, namePath));
      });
      _this.prevWithoutPreserves = null;
      _this.updateStore(nextStore);
    }
  });
  (0, _defineProperty2.default)(this, "destroyForm", function () {
    var prevWithoutPreserves = new _NameMap.default();
    _this.getFieldEntities(true).forEach(function (entity) {
      if (!_this.isMergedPreserve(entity.isPreserve())) {
        prevWithoutPreserves.set(entity.getNamePath(), true);
      }
    });
    _this.prevWithoutPreserves = prevWithoutPreserves;
  });
  (0, _defineProperty2.default)(this, "getInitialValue", function (namePath) {
    var initValue = (0, _valueUtil.getValue)(_this.initialValues, namePath);

    // Not cloneDeep when without `namePath`
    return namePath.length ? (0, _set.merge)(initValue) : initValue;
  });
  (0, _defineProperty2.default)(this, "setCallbacks", function (callbacks) {
    _this.callbacks = callbacks;
  });
  (0, _defineProperty2.default)(this, "setValidateMessages", function (validateMessages) {
    _this.validateMessages = validateMessages;
  });
  (0, _defineProperty2.default)(this, "setPreserve", function (preserve) {
    _this.preserve = preserve;
  });
  // ============================= Watch ============================
  (0, _defineProperty2.default)(this, "watchList", []);
  (0, _defineProperty2.default)(this, "registerWatch", function (callback) {
    _this.watchList.push(callback);
    return function () {
      _this.watchList = _this.watchList.filter(function (fn) {
        return fn !== callback;
      });
    };
  });
  (0, _defineProperty2.default)(this, "notifyWatch", function () {
    var namePath = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];
    // No need to cost perf when nothing need to watch
    if (_this.watchList.length) {
      var values = _this.getFieldsValue();
      var allValues = _this.getFieldsValue(true);
      _this.watchList.forEach(function (callback) {
        callback(values, allValues, namePath);
      });
    }
  });
  // ========================== Dev Warning =========================
  (0, _defineProperty2.default)(this, "timeoutId", null);
  (0, _defineProperty2.default)(this, "warningUnhooked", function () {
    if (process.env.NODE_ENV !== 'production' && !_this.timeoutId && typeof window !== 'undefined') {
      _this.timeoutId = setTimeout(function () {
        _this.timeoutId = null;
        if (!_this.formHooked) {
          (0, _warning.default)(false, 'Instance created by `useForm` is not connected to any Form element. Forget to pass `form` prop?');
        }
      });
    }
  });
  // ============================ Store =============================
  (0, _defineProperty2.default)(this, "updateStore", function (nextStore) {
    _this.store = nextStore;
  });
  // ============================ Fields ============================
  /**
   * Get registered field entities.
   * @param pure Only return field which has a `name`. Default: false
   */
  (0, _defineProperty2.default)(this, "getFieldEntities", function () {
    var pure = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
    if (!pure) {
      return _this.fieldEntities;
    }
    return _this.fieldEntities.filter(function (field) {
      return field.getNamePath().length;
    });
  });
  (0, _defineProperty2.default)(this, "getFieldsMap", function () {
    var pure = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
    var cache = new _NameMap.default();
    _this.getFieldEntities(pure).forEach(function (field) {
      var namePath = field.getNamePath();
      cache.set(namePath, field);
    });
    return cache;
  });
  (0, _defineProperty2.default)(this, "getFieldEntitiesForNamePathList", function (nameList) {
    if (!nameList) {
      return _this.getFieldEntities(true);
    }
    var cache = _this.getFieldsMap(true);
    return nameList.map(function (name) {
      var namePath = (0, _valueUtil.getNamePath)(name);
      return cache.get(namePath) || {
        INVALIDATE_NAME_PATH: (0, _valueUtil.getNamePath)(name)
      };
    });
  });
  (0, _defineProperty2.default)(this, "getFieldsValue", function (nameList, filterFunc) {
    _this.warningUnhooked();

    // Fill args
    var mergedNameList;
    var mergedFilterFunc;
    var mergedStrict;
    if (nameList === true || Array.isArray(nameList)) {
      mergedNameList = nameList;
      mergedFilterFunc = filterFunc;
    } else if (nameList && (0, _typeof2.default)(nameList) === 'object') {
      mergedStrict = nameList.strict;
      mergedFilterFunc = nameList.filter;
    }
    if (mergedNameList === true && !mergedFilterFunc) {
      return _this.store;
    }
    var fieldEntities = _this.getFieldEntitiesForNamePathList(Array.isArray(mergedNameList) ? mergedNameList : null);
    var filteredNameList = [];
    fieldEntities.forEach(function (entity) {
      var _isListField, _ref3;
      var namePath = 'INVALIDATE_NAME_PATH' in entity ? entity.INVALIDATE_NAME_PATH : entity.getNamePath();

      // Ignore when it's a list item and not specific the namePath,
      // since parent field is already take in count
      if (mergedStrict) {
        var _isList, _ref2;
        if ((_isList = (_ref2 = entity).isList) !== null && _isList !== void 0 && _isList.call(_ref2)) {
          return;
        }
      } else if (!mergedNameList && (_isListField = (_ref3 = entity).isListField) !== null && _isListField !== void 0 && _isListField.call(_ref3)) {
        return;
      }
      if (!mergedFilterFunc) {
        filteredNameList.push(namePath);
      } else {
        var meta = 'getMeta' in entity ? entity.getMeta() : null;
        if (mergedFilterFunc(meta)) {
          filteredNameList.push(namePath);
        }
      }
    });
    return (0, _valueUtil.cloneByNamePathList)(_this.store, filteredNameList.map(_valueUtil.getNamePath));
  });
  (0, _defineProperty2.default)(this, "getFieldValue", function (name) {
    _this.warningUnhooked();
    var namePath = (0, _valueUtil.getNamePath)(name);
    return (0, _valueUtil.getValue)(_this.store, namePath);
  });
  (0, _defineProperty2.default)(this, "getFieldsError", function (nameList) {
    _this.warningUnhooked();
    var fieldEntities = _this.getFieldEntitiesForNamePathList(nameList);
    return fieldEntities.map(function (entity, index) {
      if (entity && !('INVALIDATE_NAME_PATH' in entity)) {
        return {
          name: entity.getNamePath(),
          errors: entity.getErrors(),
          warnings: entity.getWarnings()
        };
      }
      return {
        name: (0, _valueUtil.getNamePath)(nameList[index]),
        errors: [],
        warnings: []
      };
    });
  });
  (0, _defineProperty2.default)(this, "getFieldError", function (name) {
    _this.warningUnhooked();
    var namePath = (0, _valueUtil.getNamePath)(name);
    var fieldError = _this.getFieldsError([namePath])[0];
    return fieldError.errors;
  });
  (0, _defineProperty2.default)(this, "getFieldWarning", function (name) {
    _this.warningUnhooked();
    var namePath = (0, _valueUtil.getNamePath)(name);
    var fieldError = _this.getFieldsError([namePath])[0];
    return fieldError.warnings;
  });
  (0, _defineProperty2.default)(this, "isFieldsTouched", function () {
    _this.warningUnhooked();
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    var arg0 = args[0],
      arg1 = args[1];
    var namePathList;
    var isAllFieldsTouched = false;
    if (args.length === 0) {
      namePathList = null;
    } else if (args.length === 1) {
      if (Array.isArray(arg0)) {
        namePathList = arg0.map(_valueUtil.getNamePath);
        isAllFieldsTouched = false;
      } else {
        namePathList = null;
        isAllFieldsTouched = arg0;
      }
    } else {
      namePathList = arg0.map(_valueUtil.getNamePath);
      isAllFieldsTouched = arg1;
    }
    var fieldEntities = _this.getFieldEntities(true);
    var isFieldTouched = function isFieldTouched(field) {
      return field.isFieldTouched();
    };

    // ===== Will get fully compare when not config namePathList =====
    if (!namePathList) {
      return isAllFieldsTouched ? fieldEntities.every(function (entity) {
        return isFieldTouched(entity) || entity.isList();
      }) : fieldEntities.some(isFieldTouched);
    }

    // Generate a nest tree for validate
    var map = new _NameMap.default();
    namePathList.forEach(function (shortNamePath) {
      map.set(shortNamePath, []);
    });
    fieldEntities.forEach(function (field) {
      var fieldNamePath = field.getNamePath();

      // Find matched entity and put into list
      namePathList.forEach(function (shortNamePath) {
        if (shortNamePath.every(function (nameUnit, i) {
          return fieldNamePath[i] === nameUnit;
        })) {
          map.update(shortNamePath, function (list) {
            return [].concat((0, _toConsumableArray2.default)(list), [field]);
          });
        }
      });
    });

    // Check if NameMap value is touched
    var isNamePathListTouched = function isNamePathListTouched(entities) {
      return entities.some(isFieldTouched);
    };
    var namePathListEntities = map.map(function (_ref4) {
      var value = _ref4.value;
      return value;
    });
    return isAllFieldsTouched ? namePathListEntities.every(isNamePathListTouched) : namePathListEntities.some(isNamePathListTouched);
  });
  (0, _defineProperty2.default)(this, "isFieldTouched", function (name) {
    _this.warningUnhooked();
    return _this.isFieldsTouched([name]);
  });
  (0, _defineProperty2.default)(this, "isFieldsValidating", function (nameList) {
    _this.warningUnhooked();
    var fieldEntities = _this.getFieldEntities();
    if (!nameList) {
      return fieldEntities.some(function (testField) {
        return testField.isFieldValidating();
      });
    }
    var namePathList = nameList.map(_valueUtil.getNamePath);
    return fieldEntities.some(function (testField) {
      var fieldNamePath = testField.getNamePath();
      return (0, _valueUtil.containsNamePath)(namePathList, fieldNamePath) && testField.isFieldValidating();
    });
  });
  (0, _defineProperty2.default)(this, "isFieldValidating", function (name) {
    _this.warningUnhooked();
    return _this.isFieldsValidating([name]);
  });
  /**
   * Reset Field with field `initialValue` prop.
   * Can pass `entities` or `namePathList` or just nothing.
   */
  (0, _defineProperty2.default)(this, "resetWithFieldInitialValue", function () {
    var info = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    // Create cache
    var cache = new _NameMap.default();
    var fieldEntities = _this.getFieldEntities(true);
    fieldEntities.forEach(function (field) {
      var initialValue = field.props.initialValue;
      var namePath = field.getNamePath();

      // Record only if has `initialValue`
      if (initialValue !== undefined) {
        var records = cache.get(namePath) || new Set();
        records.add({
          entity: field,
          value: initialValue
        });
        cache.set(namePath, records);
      }
    });

    // Reset
    var resetWithFields = function resetWithFields(entities) {
      entities.forEach(function (field) {
        var initialValue = field.props.initialValue;
        if (initialValue !== undefined) {
          var namePath = field.getNamePath();
          var formInitialValue = _this.getInitialValue(namePath);
          if (formInitialValue !== undefined) {
            // Warning if conflict with form initialValues and do not modify value
            (0, _warning.default)(false, "Form already set 'initialValues' with path '".concat(namePath.join('.'), "'. Field can not overwrite it."));
          } else {
            var records = cache.get(namePath);
            if (records && records.size > 1) {
              // Warning if multiple field set `initialValue`and do not modify value
              (0, _warning.default)(false, "Multiple Field with path '".concat(namePath.join('.'), "' set 'initialValue'. Can not decide which one to pick."));
            } else if (records) {
              var originValue = _this.getFieldValue(namePath);
              var isListField = field.isListField();

              // Set `initialValue`
              if (!isListField && (!info.skipExist || originValue === undefined)) {
                _this.updateStore((0, _valueUtil.setValue)(_this.store, namePath, (0, _toConsumableArray2.default)(records)[0].value));
              }
            }
          }
        }
      });
    };
    var requiredFieldEntities;
    if (info.entities) {
      requiredFieldEntities = info.entities;
    } else if (info.namePathList) {
      requiredFieldEntities = [];
      info.namePathList.forEach(function (namePath) {
        var records = cache.get(namePath);
        if (records) {
          var _requiredFieldEntitie;
          (_requiredFieldEntitie = requiredFieldEntities).push.apply(_requiredFieldEntitie, (0, _toConsumableArray2.default)((0, _toConsumableArray2.default)(records).map(function (r) {
            return r.entity;
          })));
        }
      });
    } else {
      requiredFieldEntities = fieldEntities;
    }
    resetWithFields(requiredFieldEntities);
  });
  (0, _defineProperty2.default)(this, "resetFields", function (nameList) {
    _this.warningUnhooked();
    var prevStore = _this.store;
    if (!nameList) {
      _this.updateStore((0, _set.merge)(_this.initialValues));
      _this.resetWithFieldInitialValue();
      _this.notifyObservers(prevStore, null, {
        type: 'reset'
      });
      _this.notifyWatch();
      return;
    }

    // Reset by `nameList`
    var namePathList = nameList.map(_valueUtil.getNamePath);
    namePathList.forEach(function (namePath) {
      var initialValue = _this.getInitialValue(namePath);
      _this.updateStore((0, _valueUtil.setValue)(_this.store, namePath, initialValue));
    });
    _this.resetWithFieldInitialValue({
      namePathList: namePathList
    });
    _this.notifyObservers(prevStore, namePathList, {
      type: 'reset'
    });
    _this.notifyWatch(namePathList);
  });
  (0, _defineProperty2.default)(this, "setFields", function (fields) {
    _this.warningUnhooked();
    var prevStore = _this.store;
    var namePathList = [];
    fields.forEach(function (fieldData) {
      var name = fieldData.name,
        data = (0, _objectWithoutProperties2.default)(fieldData, _excluded);
      var namePath = (0, _valueUtil.getNamePath)(name);
      namePathList.push(namePath);

      // Value
      if ('value' in data) {
        _this.updateStore((0, _valueUtil.setValue)(_this.store, namePath, data.value));
      }
      _this.notifyObservers(prevStore, [namePath], {
        type: 'setField',
        data: fieldData
      });
    });
    _this.notifyWatch(namePathList);
  });
  (0, _defineProperty2.default)(this, "getFields", function () {
    var entities = _this.getFieldEntities(true);
    var fields = entities.map(function (field) {
      var namePath = field.getNamePath();
      var meta = field.getMeta();
      var fieldData = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, meta), {}, {
        name: namePath,
        value: _this.getFieldValue(namePath)
      });
      Object.defineProperty(fieldData, 'originRCField', {
        value: true
      });
      return fieldData;
    });
    return fields;
  });
  // =========================== Observer ===========================
  /**
   * This only trigger when a field is on constructor to avoid we get initialValue too late
   */
  (0, _defineProperty2.default)(this, "initEntityValue", function (entity) {
    var initialValue = entity.props.initialValue;
    if (initialValue !== undefined) {
      var namePath = entity.getNamePath();
      var prevValue = (0, _valueUtil.getValue)(_this.store, namePath);
      if (prevValue === undefined) {
        _this.updateStore((0, _valueUtil.setValue)(_this.store, namePath, initialValue));
      }
    }
  });
  (0, _defineProperty2.default)(this, "isMergedPreserve", function (fieldPreserve) {
    var mergedPreserve = fieldPreserve !== undefined ? fieldPreserve : _this.preserve;
    return mergedPreserve !== null && mergedPreserve !== void 0 ? mergedPreserve : true;
  });
  (0, _defineProperty2.default)(this, "registerField", function (entity) {
    _this.fieldEntities.push(entity);
    var namePath = entity.getNamePath();
    _this.notifyWatch([namePath]);

    // Set initial values
    if (entity.props.initialValue !== undefined) {
      var prevStore = _this.store;
      _this.resetWithFieldInitialValue({
        entities: [entity],
        skipExist: true
      });
      _this.notifyObservers(prevStore, [entity.getNamePath()], {
        type: 'valueUpdate',
        source: 'internal'
      });
    }

    // un-register field callback
    return function (isListField, preserve) {
      var subNamePath = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
      _this.fieldEntities = _this.fieldEntities.filter(function (item) {
        return item !== entity;
      });

      // Clean up store value if not preserve
      if (!_this.isMergedPreserve(preserve) && (!isListField || subNamePath.length > 1)) {
        var defaultValue = isListField ? undefined : _this.getInitialValue(namePath);
        if (namePath.length && _this.getFieldValue(namePath) !== defaultValue && _this.fieldEntities.every(function (field) {
          return (
            // Only reset when no namePath exist
            !(0, _valueUtil.matchNamePath)(field.getNamePath(), namePath)
          );
        })) {
          var _prevStore = _this.store;
          _this.updateStore((0, _valueUtil.setValue)(_prevStore, namePath, defaultValue, true));

          // Notify that field is unmount
          _this.notifyObservers(_prevStore, [namePath], {
            type: 'remove'
          });

          // Dependencies update
          _this.triggerDependenciesUpdate(_prevStore, namePath);
        }
      }
      _this.notifyWatch([namePath]);
    };
  });
  (0, _defineProperty2.default)(this, "dispatch", function (action) {
    switch (action.type) {
      case 'updateValue':
        {
          var namePath = action.namePath,
            value = action.value;
          _this.updateValue(namePath, value);
          break;
        }
      case 'validateField':
        {
          var _namePath = action.namePath,
            triggerName = action.triggerName;
          _this.validateFields([_namePath], {
            triggerName: triggerName
          });
          break;
        }
      default:
      // Currently we don't have other action. Do nothing.
    }
  });
  (0, _defineProperty2.default)(this, "notifyObservers", function (prevStore, namePathList, info) {
    if (_this.subscribable) {
      var mergedInfo = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, info), {}, {
        store: _this.getFieldsValue(true)
      });
      _this.getFieldEntities().forEach(function (_ref5) {
        var onStoreChange = _ref5.onStoreChange;
        onStoreChange(prevStore, namePathList, mergedInfo);
      });
    } else {
      _this.forceRootUpdate();
    }
  });
  /**
   * Notify dependencies children with parent update
   * We need delay to trigger validate in case Field is under render props
   */
  (0, _defineProperty2.default)(this, "triggerDependenciesUpdate", function (prevStore, namePath) {
    var childrenFields = _this.getDependencyChildrenFields(namePath);
    if (childrenFields.length) {
      _this.validateFields(childrenFields);
    }
    _this.notifyObservers(prevStore, childrenFields, {
      type: 'dependenciesUpdate',
      relatedFields: [namePath].concat((0, _toConsumableArray2.default)(childrenFields))
    });
    return childrenFields;
  });
  (0, _defineProperty2.default)(this, "updateValue", function (name, value) {
    var namePath = (0, _valueUtil.getNamePath)(name);
    var prevStore = _this.store;
    _this.updateStore((0, _valueUtil.setValue)(_this.store, namePath, value));
    _this.notifyObservers(prevStore, [namePath], {
      type: 'valueUpdate',
      source: 'internal'
    });
    _this.notifyWatch([namePath]);

    // Dependencies update
    var childrenFields = _this.triggerDependenciesUpdate(prevStore, namePath);

    // trigger callback function
    var onValuesChange = _this.callbacks.onValuesChange;
    if (onValuesChange) {
      var changedValues = (0, _valueUtil.cloneByNamePathList)(_this.store, [namePath]);
      onValuesChange(changedValues, _this.getFieldsValue());
    }
    _this.triggerOnFieldsChange([namePath].concat((0, _toConsumableArray2.default)(childrenFields)));
  });
  // Let all child Field get update.
  (0, _defineProperty2.default)(this, "setFieldsValue", function (store) {
    _this.warningUnhooked();
    var prevStore = _this.store;
    if (store) {
      var nextStore = (0, _set.merge)(_this.store, store);
      _this.updateStore(nextStore);
    }
    _this.notifyObservers(prevStore, null, {
      type: 'valueUpdate',
      source: 'external'
    });
    _this.notifyWatch();
  });
  (0, _defineProperty2.default)(this, "setFieldValue", function (name, value) {
    _this.setFields([{
      name: name,
      value: value
    }]);
  });
  (0, _defineProperty2.default)(this, "getDependencyChildrenFields", function (rootNamePath) {
    var children = new Set();
    var childrenFields = [];
    var dependencies2fields = new _NameMap.default();

    /**
     * Generate maps
     * Can use cache to save perf if user report performance issue with this
     */
    _this.getFieldEntities().forEach(function (field) {
      var dependencies = field.props.dependencies;
      (dependencies || []).forEach(function (dependency) {
        var dependencyNamePath = (0, _valueUtil.getNamePath)(dependency);
        dependencies2fields.update(dependencyNamePath, function () {
          var fields = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : new Set();
          fields.add(field);
          return fields;
        });
      });
    });
    var fillChildren = function fillChildren(namePath) {
      var fields = dependencies2fields.get(namePath) || new Set();
      fields.forEach(function (field) {
        if (!children.has(field)) {
          children.add(field);
          var fieldNamePath = field.getNamePath();
          if (field.isFieldDirty() && fieldNamePath.length) {
            childrenFields.push(fieldNamePath);
            fillChildren(fieldNamePath);
          }
        }
      });
    };
    fillChildren(rootNamePath);
    return childrenFields;
  });
  (0, _defineProperty2.default)(this, "triggerOnFieldsChange", function (namePathList, filedErrors) {
    var onFieldsChange = _this.callbacks.onFieldsChange;
    if (onFieldsChange) {
      var fields = _this.getFields();

      /**
       * Fill errors since `fields` may be replaced by controlled fields
       */
      if (filedErrors) {
        var cache = new _NameMap.default();
        filedErrors.forEach(function (_ref6) {
          var name = _ref6.name,
            errors = _ref6.errors;
          cache.set(name, errors);
        });
        fields.forEach(function (field) {
          // eslint-disable-next-line no-param-reassign
          field.errors = cache.get(field.name) || field.errors;
        });
      }
      var changedFields = fields.filter(function (_ref7) {
        var fieldName = _ref7.name;
        return (0, _valueUtil.containsNamePath)(namePathList, fieldName);
      });
      if (changedFields.length) {
        onFieldsChange(changedFields, fields);
      }
    }
  });
  // =========================== Validate ===========================
  (0, _defineProperty2.default)(this, "validateFields", function (arg1, arg2) {
    _this.warningUnhooked();
    var nameList;
    var options;
    if (Array.isArray(arg1) || typeof arg1 === 'string' || typeof arg2 === 'string') {
      nameList = arg1;
      options = arg2;
    } else {
      options = arg1;
    }
    var provideNameList = !!nameList;
    var namePathList = provideNameList ? nameList.map(_valueUtil.getNamePath) : [];

    // Collect result in promise list
    var promiseList = [];

    // We temp save the path which need trigger for `onFieldsChange`
    var TMP_SPLIT = String(Date.now());
    var validateNamePathList = new Set();
    var _ref8 = options || {},
      recursive = _ref8.recursive,
      dirty = _ref8.dirty;
    _this.getFieldEntities(true).forEach(function (field) {
      // Add field if not provide `nameList`
      if (!provideNameList) {
        namePathList.push(field.getNamePath());
      }

      // Skip if without rule
      if (!field.props.rules || !field.props.rules.length) {
        return;
      }

      // Skip if only validate dirty field
      if (dirty && !field.isFieldDirty()) {
        return;
      }
      var fieldNamePath = field.getNamePath();
      validateNamePathList.add(fieldNamePath.join(TMP_SPLIT));

      // Add field validate rule in to promise list
      if (!provideNameList || (0, _valueUtil.containsNamePath)(namePathList, fieldNamePath, recursive)) {
        var promise = field.validateRules((0, _objectSpread2.default)({
          validateMessages: (0, _objectSpread2.default)((0, _objectSpread2.default)({}, _messages.defaultValidateMessages), _this.validateMessages)
        }, options));

        // Wrap promise with field
        promiseList.push(promise.then(function () {
          return {
            name: fieldNamePath,
            errors: [],
            warnings: []
          };
        }).catch(function (ruleErrors) {
          var _ruleErrors$forEach;
          var mergedErrors = [];
          var mergedWarnings = [];
          (_ruleErrors$forEach = ruleErrors.forEach) === null || _ruleErrors$forEach === void 0 || _ruleErrors$forEach.call(ruleErrors, function (_ref9) {
            var warningOnly = _ref9.rule.warningOnly,
              errors = _ref9.errors;
            if (warningOnly) {
              mergedWarnings.push.apply(mergedWarnings, (0, _toConsumableArray2.default)(errors));
            } else {
              mergedErrors.push.apply(mergedErrors, (0, _toConsumableArray2.default)(errors));
            }
          });
          if (mergedErrors.length) {
            return Promise.reject({
              name: fieldNamePath,
              errors: mergedErrors,
              warnings: mergedWarnings
            });
          }
          return {
            name: fieldNamePath,
            errors: mergedErrors,
            warnings: mergedWarnings
          };
        }));
      }
    });
    var summaryPromise = (0, _asyncUtil.allPromiseFinish)(promiseList);
    _this.lastValidatePromise = summaryPromise;

    // Notify fields with rule that validate has finished and need update
    summaryPromise.catch(function (results) {
      return results;
    }).then(function (results) {
      var resultNamePathList = results.map(function (_ref10) {
        var name = _ref10.name;
        return name;
      });
      _this.notifyObservers(_this.store, resultNamePathList, {
        type: 'validateFinish'
      });
      _this.triggerOnFieldsChange(resultNamePathList, results);
    });
    var returnPromise = summaryPromise.then(function () {
      if (_this.lastValidatePromise === summaryPromise) {
        return Promise.resolve(_this.getFieldsValue(namePathList));
      }
      return Promise.reject([]);
    }).catch(function (results) {
      var errorList = results.filter(function (result) {
        return result && result.errors.length;
      });
      return Promise.reject({
        values: _this.getFieldsValue(namePathList),
        errorFields: errorList,
        outOfDate: _this.lastValidatePromise !== summaryPromise
      });
    });

    // Do not throw in console
    returnPromise.catch(function (e) {
      return e;
    });

    // `validating` changed. Trigger `onFieldsChange`
    var triggerNamePathList = namePathList.filter(function (namePath) {
      return validateNamePathList.has(namePath.join(TMP_SPLIT));
    });
    _this.triggerOnFieldsChange(triggerNamePathList);
    return returnPromise;
  });
  // ============================ Submit ============================
  (0, _defineProperty2.default)(this, "submit", function () {
    _this.warningUnhooked();
    _this.validateFields().then(function (values) {
      var onFinish = _this.callbacks.onFinish;
      if (onFinish) {
        try {
          onFinish(values);
        } catch (err) {
          // Should print error if user `onFinish` callback failed
          console.error(err);
        }
      }
    }).catch(function (e) {
      var onFinishFailed = _this.callbacks.onFinishFailed;
      if (onFinishFailed) {
        onFinishFailed(e);
      }
    });
  });
  this.forceRootUpdate = forceRootUpdate;
});
function useForm(form) {
  var formRef = React.useRef();
  var _React$useState = React.useState({}),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    forceUpdate = _React$useState2[1];
  if (!formRef.current) {
    if (form) {
      formRef.current = form;
    } else {
      // Create a new FormStore if not provided
      var forceReRender = function forceReRender() {
        forceUpdate({});
      };
      var formStore = new FormStore(forceReRender);
      formRef.current = formStore.getForm();
    }
  }
  return [formRef.current];
}
var _default = exports.default = useForm;