"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _gl_ES = _interopRequireDefault(require("rc-pagination/lib/locale/gl_ES"));
var _gl_ES2 = _interopRequireDefault(require("../calendar/locale/gl_ES"));
var _gl_ES3 = _interopRequireDefault(require("../date-picker/locale/gl_ES"));
var _gl_ES4 = _interopRequireDefault(require("../time-picker/locale/gl_ES"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} non é un ${type} válido';
const localeValues = {
  locale: 'gl',
  Pagination: _gl_ES.default,
  DatePicker: _gl_ES3.default,
  TimePicker: _gl_ES4.default,
  Calendar: _gl_ES2.default,
  global: {
    placeholder: 'Escolla'
  },
  Table: {
    filterTitle: 'Filtrar menú',
    filterConfirm: 'Aceptar',
    filterReset: 'Reiniciar',
    selectAll: 'Seleccionar todo',
    selectInvert: 'Invertir selección',
    sortTitle: 'Ordenar'
  },
  Modal: {
    okText: 'Aceptar',
    cancelText: 'Cancelar',
    justOkText: 'Aceptar'
  },
  Popconfirm: {
    okText: 'Aceptar',
    cancelText: 'Cancelar'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Buscar aquí',
    itemUnit: 'elemento',
    itemsUnit: 'elementos'
  },
  Upload: {
    uploading: 'Subindo...',
    removeFile: 'Eliminar arquivo',
    uploadError: 'Error ao subir o arquivo',
    previewFile: 'Vista previa',
    downloadFile: 'Descargar arquivo'
  },
  Empty: {
    description: 'Non hai datos'
  },
  Icon: {
    icon: 'icona'
  },
  Text: {
    edit: 'editar',
    copy: 'copiar',
    copied: 'copiado',
    expand: 'expandir'
  },
  Form: {
    defaultValidateMessages: {
      default: 'Error de validación do campo ${label}',
      required: 'Por favor complete ${label}',
      enum: '${label} ten que ser un de [${enum}]',
      whitespace: '${label} non pode ter ningún caracter en branco',
      date: {
        format: 'O formato de data ${label} non é válido',
        parse: '${label} non se pode convertir a unha data',
        invalid: '${label} é unha data inválida'
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
        len: '${label} debe ter ${len} caracteres',
        min: '${label} debe ter como mínimo ${min} caracteres',
        max: '${label} debe ter ata ${max} caracteres',
        range: '${label} debe ter entre ${min}-${max} caracteres'
      },
      number: {
        len: '${label} debe ser igual a ${len}',
        min: '${label} valor mínimo é ${min}',
        max: '${label} valor máximo é ${max}',
        range: '${label} debe estar entre ${min}-${max}'
      },
      array: {
        len: 'Debe ser ${len} ${label}',
        min: 'Como mínimo ${min} ${label}',
        max: 'Como máximo ${max} ${label}',
        range: 'O valor de ${label} debe estar entre ${min}-${max}'
      },
      pattern: {
        mismatch: '${label} non coincide co patrón ${pattern}'
      }
    }
  }
};
var _default = exports.default = localeValues;