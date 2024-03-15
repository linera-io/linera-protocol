"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _sk_SK = _interopRequireDefault(require("rc-pagination/lib/locale/sk_SK"));
var _sk_SK2 = _interopRequireDefault(require("../calendar/locale/sk_SK"));
var _sk_SK3 = _interopRequireDefault(require("../date-picker/locale/sk_SK"));
var _sk_SK4 = _interopRequireDefault(require("../time-picker/locale/sk_SK"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} nie je platný ${type}';
const localeValues = {
  locale: 'sk',
  Pagination: _sk_SK.default,
  DatePicker: _sk_SK3.default,
  TimePicker: _sk_SK4.default,
  Calendar: _sk_SK2.default,
  global: {
    placeholder: 'Prosím vyber'
  },
  Table: {
    filterTitle: 'Filter',
    filterConfirm: 'OK',
    filterReset: 'Obnoviť',
    filterEmptyText: 'Žiadne filtre',
    filterCheckall: 'Vyber všetky položky',
    filterSearchPlaceholder: 'Vyhľadaj vo filtroch',
    emptyText: 'Žiadne dáta',
    selectAll: 'Označ všetky položky',
    selectInvert: 'Opačný výber položiek',
    selectNone: 'Odznač všetko',
    selectionAll: 'Označ všetko',
    sortTitle: 'Zoradiť',
    expand: 'Rozbaliť riadok',
    collapse: 'Zbaliť riadok',
    triggerDesc: 'Kliknutím zoradíš zostupne',
    triggerAsc: 'Kliknutím zoradíš vzostupne',
    cancelSort: 'Kliknutím zrušíš zoradenie'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Zrušiť',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Zrušiť'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Vyhľadávanie',
    itemUnit: 'položka',
    itemsUnit: 'položiek',
    remove: 'Odstráň',
    selectCurrent: 'Vyber aktuálnu stranu',
    removeCurrent: 'Zmaž aktuálnu stranu',
    selectAll: 'Označ všetko',
    removeAll: 'Odznač všetko',
    selectInvert: 'Opačný výber'
  },
  Upload: {
    uploading: 'Nahrávanie...',
    removeFile: 'Odstrániť súbor',
    uploadError: 'Chyba pri nahrávaní',
    previewFile: 'Zobraziť súbor',
    downloadFile: 'Stiahnuť súbor'
  },
  Empty: {
    description: 'Žiadne dáta'
  },
  Icon: {
    icon: 'ikona'
  },
  Text: {
    edit: 'Upraviť',
    copy: 'Kopírovať',
    copied: 'Skopírované',
    expand: 'Zväčšiť'
  },
  Form: {
    optional: '(nepovinné)',
    defaultValidateMessages: {
      default: 'Validačná chyba poľa pre ${label}',
      required: 'Prosím vlož ${label}',
      enum: '${label} musí byť jeden z [${enum}]',
      whitespace: '${label} nemôže byť prázdny znak',
      date: {
        format: '${label} formát dátumu je neplatný',
        parse: '${label} nie je možné konvertovať na dátum',
        invalid: '${label} je neplatný dátum'
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
        len: '${label} musí byť ${len} znakov',
        min: '${label} musí byť aspoň ${min} znakov',
        max: '${label} musí byť do ${max} znakov',
        range: '${label} musí byť medzi ${min}-${max} znakmi'
      },
      number: {
        len: '${label} musí byť rovnaký ako ${len}',
        min: '${label} musí byť minimálne ${min}',
        max: '${label} musí byť maximálne ${max}',
        range: '${label} musí byť medzi ${min}-${max}'
      },
      array: {
        len: 'Musí byť ${len} ${label}',
        min: 'Aspoň ${min} ${label}',
        max: 'Najviac ${max} ${label}',
        range: 'Počet ${label} musí byť medzi ${min}-${max}'
      },
      pattern: {
        mismatch: '${label} nezodpovedá vzoru ${pattern}'
      }
    }
  },
  Image: {
    preview: 'Náhľad'
  }
};
var _default = exports.default = localeValues;