"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));
var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));
var _createSuper2 = _interopRequireDefault(require("@babel/runtime/helpers/createSuper"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _react = _interopRequireWildcard(require("react"));
var _AjaxUploader = _interopRequireDefault(require("./AjaxUploader"));
/* eslint react/prop-types:0 */

function empty() {}
var Upload = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(Upload, _Component);
  var _super = (0, _createSuper2.default)(Upload);
  function Upload() {
    var _this;
    (0, _classCallCheck2.default)(this, Upload);
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "uploader", void 0);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "saveUploader", function (node) {
      _this.uploader = node;
    });
    return _this;
  }
  (0, _createClass2.default)(Upload, [{
    key: "abort",
    value: function abort(file) {
      this.uploader.abort(file);
    }
  }, {
    key: "render",
    value: function render() {
      return /*#__PURE__*/_react.default.createElement(_AjaxUploader.default, (0, _extends2.default)({}, this.props, {
        ref: this.saveUploader
      }));
    }
  }]);
  return Upload;
}(_react.Component);
(0, _defineProperty2.default)(Upload, "defaultProps", {
  component: 'span',
  prefixCls: 'rc-upload',
  data: {},
  headers: {},
  name: 'file',
  multipart: false,
  onStart: empty,
  onError: empty,
  onSuccess: empty,
  multiple: false,
  beforeUpload: null,
  customRequest: null,
  withCredentials: false,
  openFileDialogOnClick: true,
  hasControlInside: false
});
var _default = exports.default = Upload;