var should = require('should');
var camel2hyphen = require('../camel2hyphen');

describe('camel2hyphen', function () {
  it('should convert minWith to min-width', function () {
    camel2hyphen('minWidth').should.equal('min-width');
  });

  it('should convert deviceMinWith to device-min-width', function () {
    camel2hyphen('deviceMinWidth').should.equal('device-min-width');
  });

  it('should convert MozTransition to -moz-transition', function () {
    camel2hyphen('MozTransition').should.equal('-moz-transition');
  });
  
});