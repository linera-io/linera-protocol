"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _az_AZ = _interopRequireDefault(require("rc-pagination/lib/locale/az_AZ"));
var _az_AZ2 = _interopRequireDefault(require("../calendar/locale/az_AZ"));
var _az_AZ3 = _interopRequireDefault(require("../date-picker/locale/az_AZ"));
var _az_AZ4 = _interopRequireDefault(require("../time-picker/locale/az_AZ"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label}Hökmlü deyil${type}';
const localeValues = {
  locale: 'az',
  Pagination: _az_AZ.default,
  DatePicker: _az_AZ3.default,
  TimePicker: _az_AZ4.default,
  Calendar: _az_AZ2.default,
  Table: {
    filterTitle: 'Filter menyu',
    filterConfirm: 'Axtar',
    filterReset: 'Sıfırla',
    emptyText: 'Məlumat yoxdur',
    selectAll: 'Cari səhifəni seç',
    selectInvert: 'Invert current page'
  },
  Modal: {
    okText: 'Bəli',
    cancelText: 'Ləğv et',
    justOkText: 'Bəli'
  },
  Popconfirm: {
    okText: 'Bəli',
    cancelText: 'Ləğv et'
  },
  Transfer: {
    titles: ['', ''],
    notFoundContent: 'Tapılmadı',
    searchPlaceholder: 'Burada axtar',
    itemUnit: 'item',
    itemsUnit: 'items'
  },
  Select: {
    notFoundContent: 'Tapılmadı'
  },
  Upload: {
    uploading: 'Yüklənir...',
    removeFile: 'Faylı sil',
    uploadError: 'Yükləmə xətası',
    previewFile: 'Fayla önbaxış'
  },
  Form: {
    optional: '（Seçimli）',
    defaultValidateMessages: {
      default: 'Sahə təsdiq xətası${label}',
      required: 'Xahiş edirik daxil olun${label}',
      enum: '${label}Onlardan biri olmalıdır[${enum}]',
      whitespace: '${label}Null xarakter ola bilməz',
      date: {
        format: '${label}Tarix formatı hökmlü deyil',
        parse: '${label}Tarixi döndərmək mümkün deyil',
        invalid: '${label}səhv tarixdir'
      },
      types: {
        string: typeTemplate,
        method: typeTemplate,
        array: typeTemplate,
        object: typeTemplate,
        number: typeTemplate,
        date: typeTemplate,
        boolean: typeTemplate,
        integer: typeTemplate,
        float: typeTemplate,
        regexp: typeTemplate,
        email: typeTemplate,
        url: typeTemplate,
        hex: typeTemplate
      },
      string: {
        len: '${label}Olmalıdır${len}işarələr',
        min: '${label}ən az${min}işarələr',
        max: '${label}ən çox${max}işarələr',
        range: '${label}Olmalıdır${min}-${max}hərflər arasında'
      },
      number: {
        len: '${label}Bərabər olmalıdır${len}',
        min: '${label}Minimal dəyəri${min}',
        max: '${label}Maksimal qiymət:${max}',
        range: '${label}Olmalıdır${min}-${max}aralarında'
      },
      array: {
        len: 'Olmalıdır${len}parça${label}',
        min: 'ən az${min}parça${label}',
        max: 'ən çox${max}parça${label}',
        range: '${label}miqdarıOlmalıdır${min}-${max}aralarında'
      },
      pattern: {
        mismatch: '${label}Şablona uyğun gəlmir${pattern}'
      }
    }
  }
};
var _default = exports.default = localeValues;