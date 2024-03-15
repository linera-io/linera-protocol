var should = require('should');
var hyphen2camel = require('../hyphen2camel');

describe('hyphen2camel', function () {
  it('should convert min-width to minWidth', function () {
    hyphen2camel('min-width').should.equal('minWidth');
  });

  it('should convert device-min-width to deviceMinWidth', function () {
    hyphen2camel('device-min-width').should.equal('deviceMinWidth');
  });

  it('should convert -moz-transition to MozTransition', function () {
    hyphen2camel('-moz-transition').should.equal('MozTransition');
  });

});