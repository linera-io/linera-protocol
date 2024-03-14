"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _mn_MN = _interopRequireDefault(require("rc-pagination/lib/locale/mn_MN"));
var _mn_MN2 = _interopRequireDefault(require("../calendar/locale/mn_MN"));
var _mn_MN3 = _interopRequireDefault(require("../date-picker/locale/mn_MN"));
var _mn_MN4 = _interopRequireDefault(require("../time-picker/locale/mn_MN"));
const localeValues = {
  locale: 'mn-mn',
  Pagination: _mn_MN.default,
  DatePicker: _mn_MN3.default,
  TimePicker: _mn_MN4.default,
  Calendar: _mn_MN2.default,
  Table: {
    filterTitle: 'Хайх цэс',
    filterConfirm: 'Тийм',
    filterReset: 'Цэвэрлэх',
    selectAll: 'Бүгдийг сонгох',
    selectInvert: 'Бусдыг сонгох'
  },
  Modal: {
    okText: 'Тийм',
    cancelText: 'Цуцлах',
    justOkText: 'Тийм'
  },
  Popconfirm: {
    okText: 'Тийм',
    cancelText: 'Цуцлах'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Хайх',
    itemUnit: 'Зүйл',
    itemsUnit: 'Зүйлүүд'
  },
  Upload: {
    uploading: 'Хуулж байна...',
    removeFile: 'Файл устгах',
    uploadError: 'Хуулахад алдаа гарлаа',
    previewFile: 'Файлыг түргэн үзэх',
    downloadFile: 'Файлыг татах'
  },
  Empty: {
    description: 'Мэдээлэл байхгүй байна'
  }
};
var _default = exports.default = localeValues;