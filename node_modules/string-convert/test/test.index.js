var convert = require('../');

describe('string-convert', function () {
  it('convert.hyphen2camel should convert min-width to minWidth', function () {
    convert.hyphen2camel('min-width').should.equal('minWidth');
  });
  
  it('convert.camel2hyphen should convert minWidth to min-width', function () {
    convert.camel2hyphen('minWidth').should.equal('min-width');
  });
});