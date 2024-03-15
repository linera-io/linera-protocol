"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _regeneratorRuntime2 = _interopRequireDefault(require("@babel/runtime/helpers/regeneratorRuntime"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _asyncToGenerator2 = _interopRequireDefault(require("@babel/runtime/helpers/asyncToGenerator"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));
var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));
var _createSuper2 = _interopRequireDefault(require("@babel/runtime/helpers/createSuper"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _classnames = _interopRequireDefault(require("classnames"));
var _pickAttrs = _interopRequireDefault(require("rc-util/lib/pickAttrs"));
var _react = _interopRequireWildcard(require("react"));
var _attrAccept = _interopRequireDefault(require("./attr-accept"));
var _request = _interopRequireDefault(require("./request"));
var _traverseFileTree = _interopRequireDefault(require("./traverseFileTree"));
var _uid = _interopRequireDefault(require("./uid"));
var _excluded = ["component", "prefixCls", "className", "classNames", "disabled", "id", "style", "styles", "multiple", "accept", "capture", "children", "directory", "openFileDialogOnClick", "onMouseEnter", "onMouseLeave", "hasControlInside"];
/* eslint react/no-is-mounted:0,react/sort-comp:0,react/prop-types:0 */
var AjaxUploader = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(AjaxUploader, _Component);
  var _super = (0, _createSuper2.default)(AjaxUploader);
  function AjaxUploader() {
    var _this;
    (0, _classCallCheck2.default)(this, AjaxUploader);
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }
    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "state", {
      uid: (0, _uid.default)()
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "reqs", {});
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "fileInput", void 0);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "_isMounted", void 0);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (e) {
      var _this$props = _this.props,
        accept = _this$props.accept,
        directory = _this$props.directory;
      var files = e.target.files;
      var acceptedFiles = (0, _toConsumableArray2.default)(files).filter(function (file) {
        return !directory || (0, _attrAccept.default)(file, accept);
      });
      _this.uploadFiles(acceptedFiles);
      _this.reset();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onClick", function (event) {
      var el = _this.fileInput;
      if (!el) {
        return;
      }
      var target = event.target;
      var onClick = _this.props.onClick;
      if (target && target.tagName === 'BUTTON') {
        var parent = el.parentNode;
        parent.focus();
        target.blur();
      }
      el.click();
      if (onClick) {
        onClick(event);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onKeyDown", function (e) {
      if (e.key === 'Enter') {
        _this.onClick(e);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFileDrop", function (e) {
      var multiple = _this.props.multiple;
      e.preventDefault();
      if (e.type === 'dragover') {
        return;
      }
      if (_this.props.directory) {
        (0, _traverseFileTree.default)(Array.prototype.slice.call(e.dataTransfer.items), _this.uploadFiles, function (_file) {
          return (0, _attrAccept.default)(_file, _this.props.accept);
        });
      } else {
        var files = (0, _toConsumableArray2.default)(e.dataTransfer.files).filter(function (file) {
          return (0, _attrAccept.default)(file, _this.props.accept);
        });
        if (multiple === false) {
          files = files.slice(0, 1);
        }
        _this.uploadFiles(files);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "uploadFiles", function (files) {
      var originFiles = (0, _toConsumableArray2.default)(files);
      var postFiles = originFiles.map(function (file) {
        // eslint-disable-next-line no-param-reassign
        file.uid = (0, _uid.default)();
        return _this.processFile(file, originFiles);
      });

      // Batch upload files
      Promise.all(postFiles).then(function (fileList) {
        var onBatchStart = _this.props.onBatchStart;
        onBatchStart === null || onBatchStart === void 0 || onBatchStart(fileList.map(function (_ref) {
          var origin = _ref.origin,
            parsedFile = _ref.parsedFile;
          return {
            file: origin,
            parsedFile: parsedFile
          };
        }));
        fileList.filter(function (file) {
          return file.parsedFile !== null;
        }).forEach(function (file) {
          _this.post(file);
        });
      });
    });
    /**
     * Process file before upload. When all the file is ready, we start upload.
     */
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "processFile", /*#__PURE__*/function () {
      var _ref2 = (0, _asyncToGenerator2.default)( /*#__PURE__*/(0, _regeneratorRuntime2.default)().mark(function _callee(file, fileList) {
        var beforeUpload, transformedFile, action, mergedAction, data, mergedData, parsedData, parsedFile, mergedParsedFile;
        return (0, _regeneratorRuntime2.default)().wrap(function _callee$(_context) {
          while (1) switch (_context.prev = _context.next) {
            case 0:
              beforeUpload = _this.props.beforeUpload;
              transformedFile = file;
              if (!beforeUpload) {
                _context.next = 14;
                break;
              }
              _context.prev = 3;
              _context.next = 6;
              return beforeUpload(file, fileList);
            case 6:
              transformedFile = _context.sent;
              _context.next = 12;
              break;
            case 9:
              _context.prev = 9;
              _context.t0 = _context["catch"](3);
              // Rejection will also trade as false
              transformedFile = false;
            case 12:
              if (!(transformedFile === false)) {
                _context.next = 14;
                break;
              }
              return _context.abrupt("return", {
                origin: file,
                parsedFile: null,
                action: null,
                data: null
              });
            case 14:
              // Get latest action
              action = _this.props.action;
              if (!(typeof action === 'function')) {
                _context.next = 21;
                break;
              }
              _context.next = 18;
              return action(file);
            case 18:
              mergedAction = _context.sent;
              _context.next = 22;
              break;
            case 21:
              mergedAction = action;
            case 22:
              // Get latest data
              data = _this.props.data;
              if (!(typeof data === 'function')) {
                _context.next = 29;
                break;
              }
              _context.next = 26;
              return data(file);
            case 26:
              mergedData = _context.sent;
              _context.next = 30;
              break;
            case 29:
              mergedData = data;
            case 30:
              parsedData =
              // string type is from legacy `transformFile`.
              // Not sure if this will work since no related test case works with it
              ((0, _typeof2.default)(transformedFile) === 'object' || typeof transformedFile === 'string') && transformedFile ? transformedFile : file;
              if (parsedData instanceof File) {
                parsedFile = parsedData;
              } else {
                parsedFile = new File([parsedData], file.name, {
                  type: file.type
                });
              }
              mergedParsedFile = parsedFile;
              mergedParsedFile.uid = file.uid;
              return _context.abrupt("return", {
                origin: file,
                data: mergedData,
                parsedFile: mergedParsedFile,
                action: mergedAction
              });
            case 35:
            case "end":
              return _context.stop();
          }
        }, _callee, null, [[3, 9]]);
      }));
      return function (_x, _x2) {
        return _ref2.apply(this, arguments);
      };
    }());
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "saveFileInput", function (node) {
      _this.fileInput = node;
    });
    return _this;
  }
  (0, _createClass2.default)(AjaxUploader, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this._isMounted = true;
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this._isMounted = false;
      this.abort();
    }
  }, {
    key: "post",
    value: function post(_ref3) {
      var _this2 = this;
      var data = _ref3.data,
        origin = _ref3.origin,
        action = _ref3.action,
        parsedFile = _ref3.parsedFile;
      if (!this._isMounted) {
        return;
      }
      var _this$props2 = this.props,
        onStart = _this$props2.onStart,
        customRequest = _this$props2.customRequest,
        name = _this$props2.name,
        headers = _this$props2.headers,
        withCredentials = _this$props2.withCredentials,
        method = _this$props2.method;
      var uid = origin.uid;
      var request = customRequest || _request.default;
      var requestOption = {
        action: action,
        filename: name,
        data: data,
        file: parsedFile,
        headers: headers,
        withCredentials: withCredentials,
        method: method || 'post',
        onProgress: function onProgress(e) {
          var onProgress = _this2.props.onProgress;
          onProgress === null || onProgress === void 0 || onProgress(e, parsedFile);
        },
        onSuccess: function onSuccess(ret, xhr) {
          var onSuccess = _this2.props.onSuccess;
          onSuccess === null || onSuccess === void 0 || onSuccess(ret, parsedFile, xhr);
          delete _this2.reqs[uid];
        },
        onError: function onError(err, ret) {
          var onError = _this2.props.onError;
          onError === null || onError === void 0 || onError(err, ret, parsedFile);
          delete _this2.reqs[uid];
        }
      };
      onStart(origin);
      this.reqs[uid] = request(requestOption);
    }
  }, {
    key: "reset",
    value: function reset() {
      this.setState({
        uid: (0, _uid.default)()
      });
    }
  }, {
    key: "abort",
    value: function abort(file) {
      var reqs = this.reqs;
      if (file) {
        var uid = file.uid ? file.uid : file;
        if (reqs[uid] && reqs[uid].abort) {
          reqs[uid].abort();
        }
        delete reqs[uid];
      } else {
        Object.keys(reqs).forEach(function (uid) {
          if (reqs[uid] && reqs[uid].abort) {
            reqs[uid].abort();
          }
          delete reqs[uid];
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _clsx;
      var _this$props3 = this.props,
        Tag = _this$props3.component,
        prefixCls = _this$props3.prefixCls,
        className = _this$props3.className,
        _this$props3$classNam = _this$props3.classNames,
        classNames = _this$props3$classNam === void 0 ? {} : _this$props3$classNam,
        disabled = _this$props3.disabled,
        id = _this$props3.id,
        style = _this$props3.style,
        _this$props3$styles = _this$props3.styles,
        styles = _this$props3$styles === void 0 ? {} : _this$props3$styles,
        multiple = _this$props3.multiple,
        accept = _this$props3.accept,
        capture = _this$props3.capture,
        children = _this$props3.children,
        directory = _this$props3.directory,
        openFileDialogOnClick = _this$props3.openFileDialogOnClick,
        onMouseEnter = _this$props3.onMouseEnter,
        onMouseLeave = _this$props3.onMouseLeave,
        hasControlInside = _this$props3.hasControlInside,
        otherProps = (0, _objectWithoutProperties2.default)(_this$props3, _excluded);
      var cls = (0, _classnames.default)((_clsx = {}, (0, _defineProperty2.default)(_clsx, prefixCls, true), (0, _defineProperty2.default)(_clsx, "".concat(prefixCls, "-disabled"), disabled), (0, _defineProperty2.default)(_clsx, className, className), _clsx));
      // because input don't have directory/webkitdirectory type declaration
      var dirProps = directory ? {
        directory: 'directory',
        webkitdirectory: 'webkitdirectory'
      } : {};
      var events = disabled ? {} : {
        onClick: openFileDialogOnClick ? this.onClick : function () {},
        onKeyDown: openFileDialogOnClick ? this.onKeyDown : function () {},
        onMouseEnter: onMouseEnter,
        onMouseLeave: onMouseLeave,
        onDrop: this.onFileDrop,
        onDragOver: this.onFileDrop,
        tabIndex: hasControlInside ? undefined : '0'
      };
      return /*#__PURE__*/_react.default.createElement(Tag, (0, _extends2.default)({}, events, {
        className: cls,
        role: hasControlInside ? undefined : 'button',
        style: style
      }), /*#__PURE__*/_react.default.createElement("input", (0, _extends2.default)({}, (0, _pickAttrs.default)(otherProps, {
        aria: true,
        data: true
      }), {
        id: id,
        disabled: disabled,
        type: "file",
        ref: this.saveFileInput,
        onClick: function onClick(e) {
          return e.stopPropagation();
        } // https://github.com/ant-design/ant-design/issues/19948
        ,
        key: this.state.uid,
        style: (0, _objectSpread2.default)({
          display: 'none'
        }, styles.input),
        className: classNames.input,
        accept: accept
      }, dirProps, {
        multiple: multiple,
        onChange: this.onChange
      }, capture != null ? {
        capture: capture
      } : {})), children);
    }
  }]);
  return AjaxUploader;
}(_react.Component);
var _default = exports.default = AjaxUploader;