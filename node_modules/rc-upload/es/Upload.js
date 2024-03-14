import _extends from "@babel/runtime/helpers/esm/extends";
import _classCallCheck from "@babel/runtime/helpers/esm/classCallCheck";
import _createClass from "@babel/runtime/helpers/esm/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/esm/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/esm/inherits";
import _createSuper from "@babel/runtime/helpers/esm/createSuper";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
/* eslint react/prop-types:0 */
import React, { Component } from 'react';
import AjaxUpload from "./AjaxUploader";
function empty() {}
var Upload = /*#__PURE__*/function (_Component) {
  _inherits(Upload, _Component);
  var _super = _createSuper(Upload);
  function Upload() {
    var _this;
    _classCallCheck(this, Upload);
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    _this = _super.call.apply(_super, [this].concat(args));
    _defineProperty(_assertThisInitialized(_this), "uploader", void 0);
    _defineProperty(_assertThisInitialized(_this), "saveUploader", function (node) {
      _this.uploader = node;
    });
    return _this;
  }
  _createClass(Upload, [{
    key: "abort",
    value: function abort(file) {
      this.uploader.abort(file);
    }
  }, {
    key: "render",
    value: function render() {
      return /*#__PURE__*/React.createElement(AjaxUpload, _extends({}, this.props, {
        ref: this.saveUploader
      }));
    }
  }]);
  return Upload;
}(Component);
_defineProperty(Upload, "defaultProps", {
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
export default Upload;