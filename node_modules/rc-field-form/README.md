# rc-field-form

React Performance First Form Component.

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/rc-field-form.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-field-form
[github-actions-image]: https://github.com/react-component/field-form/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/field-form/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/field-form/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/field-form/branch/master
[download-image]: https://img.shields.io/npm/dm/rc-field-form.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-field-form

## Development

```bash
npm install
npm start
open http://localhost:8000
```

## Feature

- Support react.js and even react-native
- Validate fields with [async-validator](https://github.com/yiminghe/async-validator/)

## Install

[![rc-field-form](https://nodei.co/npm/rc-field-form.png)](https://npmjs.org/package/rc-field-form)

## Usage

```js | pure
import Form, { Field } from 'rc-field-form';

const Input = ({ value = "", ...props }) => <input value={value} {...props} />;

const Demo = () => {
  return (
    <Form
      onFinish={(values) => {
        console.log("Finish:", values);
      }}
    >
      <Field name="username">
        <Input placeholder="Username" />
      </Field>
      <Field name="password">
        <Input placeholder="Password" />
      </Field>

      <button>Submit</button>
    </Form>
  );
};

export default Demo;
```

## 🔥 API

We use typescript to create the Type definition. You can view directly in IDE. But you can still check the type definition [here](https://github.com/react-component/field-form/blob/master/src/interface.ts).

### Form

| Prop             | Description                                        | Type                                         | Default          |
| ---------------- | -------------------------------------------------- | -------------------------------------------- | ---------------- |
| component        | Customize Form render component                    | string \| Component \| false                 | form             |
| fields           | Control Form fields status. Only use when in Redux | [FieldData](#fielddata)[]                    | -                |
| form             | Set form instance created by `useForm`             | [FormInstance](#useform)                     | `Form.useForm()` |
| initialValues    | Initial value of Form                              | Object                                       | -                |
| name             | Config name with [FormProvider](#formprovider)     | string                                       | -                |
| preserve         | Preserve value when field removed                  | boolean                                      | false            |
| validateMessages | Set validate message template                      | [ValidateMessages](#validatemessages)        | -                |
| onFieldsChange   | Trigger when any value of Field changed            | (changedFields, allFields) => void           | -                |
| onFinish         | Trigger when form submit and success               | (values) => void                             | -                |
| onFinishFailed   | Trigger when form submit and failed                | ({ values, errorFields, outOfDate }) => void | -                |
| onValuesChange   | Trigger when any value of Field changed            | (changedValues, values) => void              | -                |

### Field

| Prop              | Description                                                                   | Type                                        | Default  |
| ----------------- | ----------------------------------------------------------------------------- | ------------------------------------------- | -------- |
| dependencies      | Will re-render if dependencies changed                                        | [NamePath](#namepath)[]                     | -        |
| getValueFromEvent | Specify how to get value from event                                           | (..args: any[]) => any                      | -        |
| getValueProps     | Customize additional props with value. This prop will disable `valuePropName` | (value) => any                              | -        |
| initialValue      | Field initial value                                                           | any                                         | -        |
| name              | Field name path                                                               | [NamePath](#namepath)                       | -        |
| normalize         | Normalize value before update                                                 | (value, prevValue, prevValues) => any       | -        |
| preserve          | Preserve value when field removed                                             | boolean                                     | false    |
| rules             | Validate rules                                                                | [Rule](#rule)[]                             | -        |
| shouldUpdate      | Check if Field should update                                                  | boolean \| (prevValues, nextValues) => boolean | -        |
| trigger           | Collect value update by event trigger                                         | string                                      | onChange |
| validateTrigger   | Config trigger point with rule validate                                       | string \| string[]                          | onChange |
| valuePropName     | Config value mapping prop with element                                        | string                                      | value    |

### List

| Prop     | Description                     | Type                                                                                                    | Default |
| -------- | ------------------------------- | ------------------------------------------------------------------------------------------------------- | ------- |
| name     | List field name path            | [NamePath](#namepath)[]                                                                                 | -       |
| children | Render props for listing fields | (fields: { name: [NamePath](#namepath) }[], operations: [ListOperations](#listoperations)) => ReactNode | -       |

### useForm

Form component default create an form instance by `Form.useForm`. But you can create it and pass to Form also. This allow you to call some function on the form instance.

```jsx | pure
const Demo = () => {
  const [form] = Form.useForm();
  return <Form form={form} />;
};
```

For class component user, you can use `ref` to get form instance:

```jsx | pure
class Demo extends React.Component {
  setRef = form => {
    // Form instance here
  };

  render() {
    return <Form ref={this.setRef} />;
  }
}
```

| Prop              | Description                                | Type                                                                       |
| ----------------- | ------------------------------------------ | -------------------------------------------------------------------------- |
| getFieldValue     | Get field value by name path               | (name: [NamePath](#namepath)) => any                                       |
| getFieldsValue    | Get list of field values by name path list | (nameList?: ([NamePath](#namepath)[]) => any) \| true                      |
| getFieldError     | Get field errors by name path              | (name: [NamePath](#namepath)) => string[]                                  |
| getFieldsError    | Get list of field errors by name path list | (nameList?: [NamePath](#namepath)[]) => FieldError[]                       |
| isFieldsTouched   | Check if list of fields are touched        | (nameList?: [NamePath](#namepath)[], allTouched?: boolean) => boolean      |
| isFieldTouched    | Check if a field is touched                | (name: [NamePath](#namepath)) => boolean                                   |
| isFieldValidating | Check if a field is validating             | (name: [NamePath](#namepath)) => boolean                                   |
| resetFields       | Reset fields status                        | (fields?: [NamePath](#namepath)[]) => void                                 |
| setFields         | Set fields status                          | (fields: FieldData[]) => void                                              |
| setFieldsValue    | Set fields value                           | (values) => void                                                           |
| submit            | Trigger form submit                        | () => void                                                                 |
| validateFields    | Trigger fields to validate                 | (nameList?: [NamePath](#namepath)[], options?: ValidateOptions) => Promise |

### FormProvider

| Prop             | Description                               | Type                                     | Default |
| ---------------- | ----------------------------------------- | ---------------------------------------- | ------- |
| validateMessages | Config global `validateMessages` template | [ValidateMessages](#validatemessages)    | -       |
| onFormChange     | Trigger by named form fields change       | (name, { changedFields, forms }) => void | -       |
| onFormFinish     | Trigger by named form fields finish       | (name, { values, forms }) => void        | -       |

## 📋 Interface

### NamePath

| Type                                     |
| ---------------------------------------- |
| string \| number \| (string \| number)[] |

### FieldData

| Prop       | Type                                     |
| ---------- | ---------------------------------------- |
| touched    | boolean                                  |
| validating | boolean                                  |
| errors     | string[]                                 |
| name       | string \| number \| (string \| number)[] |
| value      | any                                      |

### Rule

| Prop            | Type                                                                                            |
| --------------- | ----------------------------------------------------------------------------------------------- |
| enum            | any[]                                                                                           |
| len             | number                                                                                          |
| max             | number                                                                                          |
| message         | string                                                                                          |
| min             | number                                                                                          |
| pattern         | RegExp                                                                                          |
| required        | boolean                                                                                         |
| transform       | (value) => any                                                                                  |
| type            | string                                                                                          |
| validator       | ([rule](#rule), value, callback: (error?: string) => void, [form](#useform)) => Promise \| void |
| whitespace      | boolean                                                                                         |
| validateTrigger | string \| string[]                                                                              |

#### validator

To keep sync with `rc-form` legacy usage of `validator`, we still provides `callback` to trigger validate finished. But in `rc-field-form`, we strongly recommend to return a Promise instead.

### ListOperations

| Prop   | Type                     |
| ------ | ------------------------ |
| add    | (initValue: any) => void |
| remove | (index: number) => void  |

### ValidateMessages

Validate Messages provides a list of error template. You can ref [here](https://github.com/react-component/field-form/blob/master/src/utils/messages.ts) for fully default templates.

| Prop    | Description         |
| ------- | ------------------- |
| enum    | Rule `enum` prop    |
| len     | Rule `len` prop     |
| max     | Rule `max` prop     |
| min     | Rule `min` prop     |
| name    | Field name          |
| pattern | Rule `pattern` prop |
| type    | Rule `type` prop    |

## Different with `rc-form`

`rc-field-form` is try to keep sync with `rc-form` in api level, but there still have something to change:

### 1. Field will not keep snyc with `initialValues` when un-touched

In `rc-form`, field value will get from `initialValues` if user not operate on it.
It's a bug but user use as a feature which makes fixing will be a breaking change and we have to keep it.
In Field Form, this bug will not exist anymore. If you want to change a field value, use `setFieldsValue` instead.

### 2. Remove Field will not clean up related value

We do lots of logic to clean up the value when Field removed before. But with user feedback, remove exist value increase the additional work to keep value back with conditional field.

### 3. Nest name use array instead of string

In `rc-form`, we support like `user.name` to be a name and convert value to `{ user: { name: 'Bamboo' } }`. This makes '.' always be the route of variable, this makes developer have to do additional work if name is real contains a point like `app.config.start` to be `app_config_start` and parse back to point when submit.

Field Form will only trade `['user', 'name']` to be `{ user: { name: 'Bamboo' } }`, and `user.name` to be `{ ['user.name']: 'Bamboo' }`.

### 4. Remove `validateFieldsAndScroll`

Since `findDomNode` is marked as warning in [StrictMode](https://reactjs.org/docs/strict-mode.html#warning-about-deprecated-finddomnode-usage). It seems over control of Form component.
We decide to remove `validateFieldsAndScroll` method and you should handle it with you own logic:

```jsx | pure
<Form>
  <Field name="username">
    <input ref={this.inputRef} />
  </Field>
</Form>
```

### 5. `getFieldsError` always return array

`rc-form` returns `null` when no error happen. This makes user have to do some additional code like:

```js | pure
(form.getFieldsError('fieldName') || []).forEach(() => {
  // Do something...
});
```

Now `getFieldsError` will return `[]` if no errors.

### 6. Remove `callback` with `validateFields`

Since ES8 is support `async/await`, that's no reason not to use it. Now you can easily handle your validate logic:

```js | pure
async function() {
  try {
    const values = await form.validateFields();
    console.log(values);
  } catch (errorList) {
    errorList.forEach(({ name, errors }) => {
      // Do something...
    });
  }
}
```

**Notice: Now if your validator return an `Error(message)`, not need to get error by `e => e.message`. FieldForm will handle this.**

### 7. `preserve` is default to false

In `rc-form` you should use `preserve` to keep a value cause Form will auto remove a value from Field removed. Field Form will always keep the value in the Form whatever Field removed. But you can still use `preserve=false` to disable value keeping since `1.5.0`.

### 8. `setFields` not trigger `onFieldsChange` and `setFieldsValue` not trigger `onValuesChange`

In `rc-form`, we hope to help user auto trigger change event by setting to make redux dispatch easier, but it's not good design since it makes code logic couping.

Additionally, user control update trigger `onFieldsChange` & `onValuesChange` event has potential dead loop risk.
