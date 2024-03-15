"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _hr_HR = _interopRequireDefault(require("rc-pagination/lib/locale/hr_HR"));
var _hr_HR2 = _interopRequireDefault(require("../calendar/locale/hr_HR"));
var _hr_HR3 = _interopRequireDefault(require("../date-picker/locale/hr_HR"));
var _hr_HR4 = _interopRequireDefault(require("../time-picker/locale/hr_HR"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} nije valjan ${type}';
const localeValues = {
  locale: 'hr',
  Pagination: _hr_HR.default,
  DatePicker: _hr_HR3.default,
  TimePicker: _hr_HR4.default,
  Calendar: _hr_HR2.default,
  global: {
    placeholder: 'Molimo označite'
  },
  Table: {
    filterTitle: 'Filter meni',
    filterConfirm: 'OK',
    filterReset: 'Reset',
    filterEmptyText: 'Nema filtera',
    emptyText: 'Nema podataka',
    selectAll: 'Označi trenutnu stranicu',
    selectInvert: 'Invertiraj trenutnu stranicu',
    selectionAll: 'Odaberite sve podatke',
    sortTitle: 'Sortiraj',
    expand: 'Proširi redak',
    collapse: 'Sažmi redak',
    triggerDesc: 'Kliknite za sortiranje silazno',
    triggerAsc: 'Kliknite za sortiranje uzlazno',
    cancelSort: 'Kliknite da biste otkazali sortiranje'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Odustani',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Odustani'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Pretraži ovdje',
    itemUnit: 'stavka',
    itemsUnit: 'stavke',
    remove: 'Ukloniti',
    selectCurrent: 'Odaberite trenutnu stranicu',
    removeCurrent: 'Ukloni trenutnu stranicu',
    selectAll: 'Odaberite sve podatke',
    removeAll: 'Uklonite sve podatke',
    selectInvert: 'Obrni trenutnu stranicu'
  },
  Upload: {
    uploading: 'Upload u tijeku...',
    removeFile: 'Makni datoteku',
    uploadError: 'Greška kod uploada',
    previewFile: 'Pogledaj datoteku',
    downloadFile: 'Preuzmi datoteku'
  },
  Empty: {
    description: 'Nema podataka'
  },
  Icon: {
    icon: 'ikona'
  },
  Text: {
    edit: 'Uredi',
    copy: 'Kopiraj',
    copied: 'Kopiranje uspješno',
    expand: 'Proširi'
  },
  Form: {
    optional: '(neobavezno)',
    defaultValidateMessages: {
      default: 'Pogreška provjere valjanosti polja za ${label}',
      required: 'Molimo unesite ${label}',
      enum: '${label} mora biti jedan od [${enum}]',
      whitespace: '${label} ne može biti prazan znak',
      date: {
        format: '${label} format datuma je nevažeći',
        parse: '${label} ne može se pretvoriti u datum',
        invalid: '${label} je nevažeći datum'
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
        len: '${label} mora biti ${len} slova',
        min: '${label} mora biti najmanje ${min} slova',
        max: '${label} mora biti do ${max} slova',
        range: '${label} mora biti između ${min}-${max} slova'
      },
      number: {
        len: '${label} mora biti jednak ${len}',
        min: '${label} mora biti minimalano ${min}',
        max: '${label} mora biti maksimalano ${max}',
        range: '${label} mora biti između ${min}-${max}'
      },
      array: {
        len: 'Mora biti ${len} ${label}',
        min: 'Najmanje ${min} ${label}',
        max: 'Najviše ${max} ${label}',
        range: 'Količina ${label} mora biti između ${min}-${max}'
      },
      pattern: {
        mismatch: '${label} ne odgovara obrascu ${pattern}'
      }
    }
  },
  Image: {
    preview: 'Pregled'
  }
};
var _default = exports.default = localeValues;