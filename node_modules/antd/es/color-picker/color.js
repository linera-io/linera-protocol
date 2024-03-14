import _classCallCheck from "@babel/runtime/helpers/esm/classCallCheck";
import _createClass from "@babel/runtime/helpers/esm/createClass";
import { Color as RcColor } from '@rc-component/color-picker';
export const toHexFormat = (value, alpha) => (value === null || value === void 0 ? void 0 : value.replace(/[^\w/]/gi, '').slice(0, alpha ? 8 : 6)) || '';
export const getHex = (value, alpha) => value ? toHexFormat(value, alpha) : '';
export let ColorFactory = /*#__PURE__*/function () {
  function ColorFactory(color) {
    _classCallCheck(this, ColorFactory);
    this.metaColor = new RcColor(color);
    if (!color) {
      this.metaColor.setAlpha(0);
    }
  }
  _createClass(ColorFactory, [{
    key: "toHsb",
    value: function toHsb() {
      return this.metaColor.toHsb();
    }
  }, {
    key: "toHsbString",
    value: function toHsbString() {
      return this.metaColor.toHsbString();
    }
  }, {
    key: "toHex",
    value: function toHex() {
      return getHex(this.toHexString(), this.metaColor.getAlpha() < 1);
    }
  }, {
    key: "toHexString",
    value: function toHexString() {
      return this.metaColor.getAlpha() === 1 ? this.metaColor.toHexString() : this.metaColor.toHex8String();
    }
  }, {
    key: "toRgb",
    value: function toRgb() {
      return this.metaColor.toRgb();
    }
  }, {
    key: "toRgbString",
    value: function toRgbString() {
      return this.metaColor.toRgbString();
    }
  }]);
  return ColorFactory;
}();