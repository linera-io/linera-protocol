"use client";

import InternalCheckbox from './Checkbox';
import Group from './Group';
const Checkbox = InternalCheckbox;
Checkbox.Group = Group;
Checkbox.__ANT_CHECKBOX = true;
if (process.env.NODE_ENV !== 'production') {
  Checkbox.displayName = 'Checkbox';
}
export default Checkbox;