"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _de_DE = _interopRequireDefault(require("rc-pagination/lib/locale/de_DE"));
var _de_DE2 = _interopRequireDefault(require("../calendar/locale/de_DE"));
var _de_DE3 = _interopRequireDefault(require("../date-picker/locale/de_DE"));
var _de_DE4 = _interopRequireDefault(require("../time-picker/locale/de_DE"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} ist nicht gültig. ${type} erwartet';
const localeValues = {
  locale: 'de',
  Pagination: _de_DE.default,
  DatePicker: _de_DE3.default,
  TimePicker: _de_DE4.default,
  Calendar: _de_DE2.default,
  global: {
    placeholder: 'Bitte auswählen'
  },
  Table: {
    filterTitle: 'Filter-Menü',
    filterConfirm: 'OK',
    filterReset: 'Zurücksetzen',
    filterEmptyText: 'Keine Filter',
    filterSearchPlaceholder: 'Suche in Filtern',
    filterCheckall: 'Alle auswählen',
    selectAll: 'Selektiere Alle',
    selectInvert: 'Selektion Invertieren',
    selectionAll: 'Wählen Sie alle Daten aus',
    sortTitle: 'Sortieren',
    emptyText: 'Keine Daten',
    expand: 'Zeile erweitern',
    collapse: 'Zeile reduzieren',
    triggerDesc: 'Klicken zur absteigenden Sortierung',
    triggerAsc: 'Klicken zur aufsteigenden Sortierung',
    cancelSort: 'Klicken zum Abbrechen der Sortierung'
  },
  Tour: {
    Next: 'Weiter',
    Previous: 'Zurück',
    Finish: 'Fertig'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Abbrechen',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Abbrechen'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Suchen',
    itemUnit: 'Eintrag',
    itemsUnit: 'Einträge',
    remove: 'Entfernen',
    selectCurrent: 'Alle auf aktueller Seite auswählen',
    removeCurrent: 'Auswahl auf aktueller Seite aufheben',
    selectAll: 'Alle auswählen',
    removeAll: 'Auswahl aufheben',
    selectInvert: 'Auswahl umkehren'
  },
  Upload: {
    uploading: 'Hochladen...',
    removeFile: 'Datei entfernen',
    uploadError: 'Fehler beim Hochladen',
    previewFile: 'Dateivorschau',
    downloadFile: 'Download-Datei'
  },
  Empty: {
    description: 'Keine Daten'
  },
  Text: {
    edit: 'Bearbeiten',
    copy: 'Kopieren',
    copied: 'Kopiert',
    expand: 'Erweitern'
  },
  Form: {
    defaultValidateMessages: {
      default: 'Feld-Validierungsfehler: ${label}',
      required: 'Bitte geben Sie ${label} an',
      enum: '${label} muss eines der folgenden sein [${enum}]',
      whitespace: '${label} darf kein Leerzeichen sein',
      date: {
        format: '${label} ist ein ungültiges Datumsformat',
        parse: '${label} kann nicht in ein Datum umgewandelt werden',
        invalid: '${label} ist ein ungültiges Datum'
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
        len: '${label} muss genau ${len} Zeichen lang sein',
        min: '${label} muss mindestens ${min} Zeichen lang sein',
        max: '${label} darf höchstens ${max} Zeichen lang sein',
        range: '${label} muss zwischen ${min} und ${max} Zeichen lang sein'
      },
      number: {
        len: '${label} muss gleich ${len} sein',
        min: '${label} muss mindestens ${min} sein',
        max: '${label} darf maximal ${max} sein',
        range: '${label} muss zwischen ${min} und ${max} liegen'
      },
      array: {
        len: 'Es müssen ${len} ${label} sein',
        min: 'Es müssen mindestens ${min} ${label} sein',
        max: 'Es dürfen maximal ${max} ${label} sein',
        range: 'Die Anzahl an ${label} muss zwischen ${min} und ${max} liegen'
      },
      pattern: {
        mismatch: '${label} entspricht nicht dem ${pattern} Muster'
      }
    }
  },
  Image: {
    preview: 'Vorschau'
  },
  QRCode: {
    expired: 'QR-Code abgelaufen',
    refresh: 'Aktualisieren'
  }
};
var _default = exports.default = localeValues;