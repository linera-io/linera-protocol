"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
exports.pathKey = pathKey;
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
// [times, realValue]

var SPLIT = '%';

/** Connect key with `SPLIT` */
function pathKey(keys) {
  return keys.join(SPLIT);
}
var Entity = /*#__PURE__*/function () {
  function Entity(instanceId) {
    (0, _classCallCheck2.default)(this, Entity);
    (0, _defineProperty2.default)(this, "instanceId", void 0);
    /** @private Internal cache map. Do not access this directly */
    (0, _defineProperty2.default)(this, "cache", new Map());
    this.instanceId = instanceId;
  }
  (0, _createClass2.default)(Entity, [{
    key: "get",
    value: function get(keys) {
      return this.opGet(pathKey(keys));
    }

    /** A fast get cache with `get` concat. */
  }, {
    key: "opGet",
    value: function opGet(keyPathStr) {
      return this.cache.get(keyPathStr) || null;
    }
  }, {
    key: "update",
    value: function update(keys, valueFn) {
      return this.opUpdate(pathKey(keys), valueFn);
    }

    /** A fast get cache with `get` concat. */
  }, {
    key: "opUpdate",
    value: function opUpdate(keyPathStr, valueFn) {
      var prevValue = this.cache.get(keyPathStr);
      var nextValue = valueFn(prevValue);
      if (nextValue === null) {
        this.cache.delete(keyPathStr);
      } else {
        this.cache.set(keyPathStr, nextValue);
      }
    }
  }]);
  return Entity;
}();
var _default = exports.default = Entity;