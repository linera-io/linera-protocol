string-convert
==============

Set of string conversion functions

Installation
------------
  npm install string-convert --save

Methods
-------

### hyphen2camel

Converts hyphenated string to camelcase string

Example:

```javascript
var hyphen2camel = require('string-convert/hyphen2camel');
hyphen2camel('min-width'); // minWidth
hyphen2camel('-moz-transition'); // MozTransition
```

### camel2hyphen

Converts camel case string to hyphenated string

Example:

```javascript
var camel2hyphen = require('string-convert/camel2hyphen');
camel2hyphen('minWidth');  // min-width
camel2hyphen('MozTransition'); //-moz-transition 
```


