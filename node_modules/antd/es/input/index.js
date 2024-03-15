"use client";

import Group from './Group';
import InternalInput from './Input';
import Password from './Password';
import Search from './Search';
import TextArea from './TextArea';
const Input = InternalInput;
if (process.env.NODE_ENV !== 'production') {
  Input.displayName = 'Input';
}
Input.Group = Group;
Input.Search = Search;
Input.TextArea = TextArea;
Input.Password = Password;
export default Input;