"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _default = exports.default = function _default(file, acceptedFiles) {
  if (file && acceptedFiles) {
    var acceptedFilesArray = Array.isArray(acceptedFiles) ? acceptedFiles : acceptedFiles.split(',');
    var fileName = file.name || '';
    var mimeType = file.type || '';
    var baseMimeType = mimeType.replace(/\/.*$/, '');
    return acceptedFilesArray.some(function (type) {
      var validType = type.trim();
      // This is something like */*,*  allow all files
      if (/^\*(\/\*)?$/.test(type)) {
        return true;
      }

      // like .jpg, .png
      if (validType.charAt(0) === '.') {
        var lowerFileName = fileName.toLowerCase();
        var lowerType = validType.toLowerCase();
        var affixList = [lowerType];
        if (lowerType === '.jpg' || lowerType === '.jpeg') {
          affixList = ['.jpg', '.jpeg'];
        }
        return affixList.some(function (affix) {
          return lowerFileName.endsWith(affix);
        });
      }

      // This is something like a image/* mime type
      if (/\/\*$/.test(validType)) {
        return baseMimeType === validType.replace(/\/.*$/, '');
      }

      // Full match
      if (mimeType === validType) {
        return true;
      }

      // Invalidate type should skip
      if (/^\w+$/.test(validType)) {
        (0, _warning.default)(false, "Upload takes an invalidate 'accept' type '".concat(validType, "'.Skip for check."));
        return true;
      }
      return false;
    });
  }
  return true;
};