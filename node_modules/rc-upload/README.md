# rc-upload

React Upload

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Test coverage][coveralls-image]][coveralls-url] [![Dependencies][david-image]][david-url] [![DevDependencies][david-dev-image]][david-dev-url] [![npm download][download-image]][download-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: https://img.shields.io/npm/v/rc-upload.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-upload
[github-actions-image]: https://github.com/react-component/upload/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/upload/actions
[circleci-image]: https://img.shields.io/circleci/react-component/upload/master?style=flat-square
[circleci-url]: https://circleci.com/gh/react-component/upload
[coveralls-image]: https://img.shields.io/coveralls/react-component/upload.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/react-component/upload?branch=master
[david-url]: https://david-dm.org/react-component/upload
[david-image]: https://david-dm.org/react-component/upload/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/upload?type=dev
[david-dev-image]: https://david-dm.org/react-component/upload/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-upload.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-upload
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-upload
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-upload

## Development

```
npm install
npm start
```

## Example

http://localhost:8000/

online example: https://upload.react-component.vercel.app/

## Feature

* support IE11+, Chrome, Firefox, Safari

## install

[![rc-upload](https://nodei.co/npm/rc-upload.png)](https://npmjs.org/package/rc-upload)

## Usage

```js
var Upload = require('rc-upload');
var React = require('react');
React.render(<Upload />, container);
```

## API

### props

|name|type|default| description|
|-----|---|--------|----|
|name | string | file| file param post to server |
|style | object | {}| root component inline style |
|className | string | - | root component className |
|disabled | boolean | false | whether disabled |
|component | "div"|"span" | "span"| wrap component name |
|action| string &#124; function(file): string &#124; Promise&lt;string&gt; | | form action url |
|method | string | post | request method |
|directory| boolean | false | support upload whole directory |
|data| object/function(file) | | other data object to post or a function which returns a data object(a promise object which resolve a data object) |
|headers| object | {} | http headers to post, available in modern browsers |
|accept | string | | input accept attribute |
|capture | string | | input capture attribute |
|multiple | boolean | false | only support ie10+|
|onStart | function| | start upload file |
|onError| function| | error callback |
|onSuccess | function | | success callback |
|onProgress | function || progress callback, only for modern browsers|
|beforeUpload| function |null| before upload check, return false or a rejected Promise will stop upload, only for modern browsers|
|customRequest | function | null | provide an override for the default xhr behavior for additional customization|
|withCredentials | boolean | false | ajax upload with cookie send |
|openFileDialogOnClick | boolean | true | useful for drag only upload as it does not trigger on enter key or click event |

#### onError arguments

1. `err`: request error message
2. `response`: request response, not support on iframeUpload
3. `file`: upload file

### onSuccess arguments

1. `result`: response body
2. `file`: upload file
3. `xhr`: xhr header, only for modern browsers which support AJAX upload. since
   2.4.0


### customRequest

Allows for advanced customization by overriding default behavior in AjaxUploader. Provide your own XMLHttpRequest calls to interface with custom backend processes or interact with AWS S3 service through the aws-sdk-js package.

customRequest callback is passed an object with:

* `onProgress: (event: { percent: number }): void`
* `onError: (event: Error, body?: Object): void`
* `onSuccess: (body: Object): void`
* `data: Object`
* `filename: String`
* `file: File`
* `withCredentials: Boolean`
* `action: String`
* `headers: Object`


### methods

abort(file?: File) => void: abort the uploading file

## License

rc-upload is released under the MIT license.
