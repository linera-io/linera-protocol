"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _nl_NL = _interopRequireDefault(require("rc-pagination/lib/locale/nl_NL"));
var _nl_NL2 = _interopRequireDefault(require("../calendar/locale/nl_NL"));
var _nl_NL3 = _interopRequireDefault(require("../date-picker/locale/nl_NL"));
var _nl_NL4 = _interopRequireDefault(require("../time-picker/locale/nl_NL"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} is geen geldige ${type}';
const localeValues = {
  locale: 'nl',
  Pagination: _nl_NL.default,
  DatePicker: _nl_NL3.default,
  TimePicker: _nl_NL4.default,
  Calendar: _nl_NL2.default,
  global: {
    placeholder: 'Maak een selectie'
  },
  Table: {
    cancelSort: 'Klik om sortering te annuleren',
    collapse: 'Rij inklappen',
    emptyText: 'Geen data',
    expand: 'Rij uitklappen',
    filterConfirm: 'OK',
    filterEmptyText: 'Geen filters',
    filterReset: 'Reset',
    filterTitle: 'Filteren',
    selectAll: 'Selecteer huidige pagina',
    selectInvert: 'Keer volgorde om',
    selectNone: 'Maak selectie leeg',
    selectionAll: 'Selecteer alle data',
    sortTitle: 'Sorteren',
    triggerAsc: 'Klik om oplopend te sorteren',
    triggerDesc: 'Klik om aflopend te sorteren'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Annuleer',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Annuleer'
  },
  Transfer: {
    itemUnit: 'item',
    itemsUnit: 'items',
    remove: 'Verwijder',
    removeAll: 'Verwijder alles',
    removeCurrent: 'Verwijder huidige pagina',
    searchPlaceholder: 'Zoek hier',
    selectAll: 'Selecteer alles',
    selectCurrent: 'Selecteer huidige pagina',
    selectInvert: 'Huidige pagina omkeren',
    titles: ['', '']
  },
  Upload: {
    downloadFile: 'Bestand downloaden',
    previewFile: 'Preview file',
    removeFile: 'Verwijder bestand',
    uploadError: 'Fout tijdens uploaden',
    uploading: 'Uploaden...'
  },
  Empty: {
    description: 'Geen gegevens'
  },
  Icon: {
    icon: 'icoon'
  },
  Text: {
    edit: 'Bewerken',
    copy: 'kopiëren',
    copied: 'Gekopieerd',
    expand: 'Uitklappen'
  },
  Form: {
    optional: '(optioneel)',
    defaultValidateMessages: {
      default: 'Validatiefout voor ${label}',
      required: 'Gelieve ${label} in te vullen',
      enum: '${label} moet één van [${enum}] zijn',
      whitespace: '${label} mag geen blanco teken zijn',
      date: {
        format: '${label} heeft een ongeldig formaat',
        parse: '${label} kan niet naar een datum omgezet worden',
        invalid: '${label} is een ongeldige datum'
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
        len: '${label} moet ${len} karakters lang zijn',
        min: '${label} moet minimaal ${min} karakters lang zijn',
        max: '${label} mag maximaal ${max} karakters lang zijn',
        range: '${label} moet tussen ${min}-${max} karakters lang zijn'
      },
      number: {
        len: '${label} moet gelijk zijn aan ${len}',
        min: '${label} moet minimaal ${min} zijn',
        max: '${label} mag maximaal ${max} zijn',
        range: '${label} moet tussen ${min}-${max} liggen'
      },
      array: {
        len: 'Moeten ${len} ${label} zijn',
        min: 'Minimaal ${min} ${label}',
        max: 'maximaal ${max} ${label}',
        range: 'Het aantal ${label} moet tussen ${min}-${max} liggen'
      },
      pattern: {
        mismatch: '${label} komt niet overeen met het patroon ${pattern}'
      }
    }
  },
  Image: {
    preview: 'Voorbeeld'
  }
};
var _default = exports.default = localeValues;