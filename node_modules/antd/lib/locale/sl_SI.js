"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _sl_SI = _interopRequireDefault(require("rc-pagination/lib/locale/sl_SI"));
var _sl_SI2 = _interopRequireDefault(require("../calendar/locale/sl_SI"));
var _sl_SI3 = _interopRequireDefault(require("../date-picker/locale/sl_SI"));
var _sl_SI4 = _interopRequireDefault(require("../time-picker/locale/sl_SI"));
const localeValues = {
  locale: 'sl',
  Pagination: _sl_SI.default,
  DatePicker: _sl_SI3.default,
  TimePicker: _sl_SI4.default,
  Calendar: _sl_SI2.default,
  Table: {
    filterTitle: 'Filter',
    filterConfirm: 'Filtriraj',
    filterReset: 'Pobriši filter',
    selectAll: 'Izberi vse na trenutni strani',
    selectInvert: 'Obrni izbor na trenutni strani'
  },
  Modal: {
    okText: 'V redu',
    cancelText: 'Prekliči',
    justOkText: 'V redu'
  },
  Popconfirm: {
    okText: 'v redu',
    cancelText: 'Prekliči'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Išči tukaj',
    itemUnit: 'Objekt',
    itemsUnit: 'Objektov'
  },
  Upload: {
    uploading: 'Nalaganje...',
    removeFile: 'Odstrani datoteko',
    uploadError: 'Napaka pri nalaganju',
    previewFile: 'Predogled datoteke',
    downloadFile: 'Prenos datoteke'
  },
  Empty: {
    description: 'Ni podatkov'
  }
};
var _default = exports.default = localeValues;