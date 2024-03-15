var should = require('should');
var json2mq = require('../');

describe('json2mq', function () {
  it('should return query string for media type', function () {
    json2mq({screen: true}).should.equal('screen');
  });

  it('should return query string for media type with not', function () {
    json2mq({handheld: false}).should.equal('not handheld');
  });

  it('should return query string for media features', function () {
    json2mq({minWidth: 100, maxWidth: 200}).should.equal('(min-width: 100px) and (max-width: 200px)');
  });

  it('should return query string for media type and media features', function () {
    json2mq({screen: true, minWidth: 100, maxWidth: 200}).should.equal('screen and (min-width: 100px) and (max-width: 200px)');
  });

  it('should add px unit to dimension features', function () {
    json2mq({minWidth: 100, aspectRatio: "3/4"}).should.equal('(min-width: 100px) and (aspect-ratio: 3/4)');
  });

  it('should accept other units for dimension features if passed as string', function () {
    json2mq({minWidth: '10em', aspectRatio: "3/4"}).should.equal('(min-width: 10em) and (aspect-ratio: 3/4)');
  });

  it('should return comma seperated query string for multiple media queries', function () {
    json2mq([
      {minWidth: 100},
      {handheld: true, orientation: 'landscape'}
    ]).should.equal('(min-width: 100px), handheld and (orientation: landscape)');
  });

  it('should only return feature if its value is true', function () {
    json2mq({all: true, monochrome: true}).should.equal('all and monochrome');
  });
});