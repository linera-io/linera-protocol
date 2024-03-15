# rc-mutate-observer

MutateObserver for React.

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/@rc-component/mutate-observer.svg?style=flat-square
[npm-url]: https://www.npmjs.com/package/@rc-component/mutate-observer
[github-actions-image]: https://github.com/react-component/mutate-observer/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/mutate-observer/actions
[codecov-image]: https://img.shields.io/codecov/c/github/@rc-component/mutate-observer/master.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/@rc-component/mutate-observer.svg?style=flat-square
[download-url]: https://www.npmjs.com/package/@rc-component/mutate-observer

## Development

```bash
npm install
npm run start
open http://localhost:8000
```

## Install

[![@rc-component/mutate-observer](https://nodei.co/npm/mutate-observer.png)](https://www.npmjs.com/package/@rc-component/mutate-observer)

## Usage

```tsx
import React from 'react';
import MutateObserver from './src';

const onMutate = (mutations: MutationRecord[], observer: MutationObserver) => {
  console.log(mutation);
  console.log(observer);
};

const Demo: React.FC = () => {
  return (
    <MutateObserver onMutate={onMutate}>
      <div>test</div>
    </MutateObserver>
  );
};

export default Demo;
```

## 🔥 API

We use typescript to create the Type definition. You can view directly in IDE. But you can still check the type definition [here](https://github.com/react-component/mutate-observer/blob/master/src/interface.ts).

### mutate-observer

| Prop     | Description                                                                                                      | Type                 | Default |
| -------- | ---------------------------------------------------------------------------------------------------------------- | -------------------- | ------- |
| onMutate | A function which will be called on each DOM change that qualifies given the observed node or subtree and options | MutationCallback     | -       |
| options  | An object providing options that describe which DOM mutations should be reported to mutationObserver's callback  | MutationObserverInit | -       |
