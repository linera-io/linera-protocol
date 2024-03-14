"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _en_US = _interopRequireDefault(require("rc-pagination/lib/locale/en_US"));
var _en_US2 = _interopRequireDefault(require("../calendar/locale/en_US"));
var _en_US3 = _interopRequireDefault(require("../date-picker/locale/en_US"));
var _en_US4 = _interopRequireDefault(require("../time-picker/locale/en_US"));
const localeValues = {
  locale: 'ne-np',
  Pagination: _en_US.default,
  DatePicker: _en_US3.default,
  TimePicker: _en_US4.default,
  Calendar: _en_US2.default,
  Table: {
    filterTitle: 'फिल्टर मेनु',
    filterConfirm: 'हो',
    filterReset: 'रीसेट',
    selectAll: 'सबै छान्नुुहोस्',
    selectInvert: 'छनौट उल्टाउनुहोस'
  },
  Modal: {
    okText: 'हो',
    cancelText: 'होईन',
    justOkText: 'हो'
  },
  Popconfirm: {
    okText: 'हो',
    cancelText: 'होईन'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'यहाँ खोज्नुहोस्',
    itemUnit: 'वस्तु',
    itemsUnit: 'वस्तुहरू'
  },
  Upload: {
    uploading: 'अपलोड गर्दै...',
    removeFile: 'फाइल हटाउनुहोस्',
    uploadError: 'अप्लोडमा समस्या भयो',
    previewFile: 'फाइल पूर्वावलोकन गर्नुहोस्',
    downloadFile: 'डाउनलोड फाइल'
  },
  Empty: {
    description: 'डाटा छैन'
  }
};
var _default = exports.default = localeValues;