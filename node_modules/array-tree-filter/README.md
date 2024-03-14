# array-tree-filter

Filter by keys in array tree.

[![NPM version](https://img.shields.io/npm/v/array-tree-filter.svg?style=flat)](https://npmjs.org/package/array-tree-filter)
[![Build Status](https://img.shields.io/travis/afc163/array-tree-filter.svg?style=flat)](https://travis-ci.org/afc163/array-tree-filter)
[![David Status](https://img.shields.io/david/afc163/array-tree-filter.svg?style=flat)](https://david-dm.org/afc163/array-tree-filter)
[![NPM downloads](http://img.shields.io/npm/dm/array-tree-filter.svg?style=flat)](https://npmjs.org/package/array-tree-filter)

```js
import arrayTreeFilter from 'array-tree-filter';

const data = [{
  value: 'a',
  children: [{
    value: 'b',
    children: [{
      value: 'c'
    }, {
      value: 'd',
    }]
  }],
}];
const values = ['a', 'b', 'c'];
const result = arrayTreeFilter(
  data, (item, level) => item.value === values[level]
);

console.log(result);
// [
//   { value: 'a', children: [...] },
//   { value: 'b', children: [...] },
//   { value: 'c', children: [...] }
// ]
```
