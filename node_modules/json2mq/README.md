# json2mq

json2mq is used to generate media query string from JSON or javascript object.

## Install
    npm install json2mq
    
## Usage
```javascript
var json2mq = require('json2mq');
json2mq({minWidth: 100, maxWidth: 200});
// -> '(min-width: 100px) and (max-width: 200px)'
```
* Media type 
```javascript
json2mq({screen: true});  // -> 'screen'
```
* Media type with negation 
```javascript
json2mq({handheld: false});  // -> 'not handheld'
```

* Media features can be specified in camel case
```javascript
json2mq({minWidth: 100, maxWidth: 200});
// -> '(min-width: 100px) and (max-width: 200px)'
```
* px is added to numeric dimension values
```javascript
json2mq({minWidth: 100, maxWidth: '20em'});
// -> '(min-width: 100px) and (max-width: 20em)'
```
* Multiple media queries can be passed as an array
```javascript
json2mq([{screen: true, minWidth: 100}, {handheld: true, orientation: 'landscape'}]); 
// -> 'screen and (min-width: 100px), handheld and (orientation: landscape)'
```



## Contributors

* Eric Schoffstall

