"use client";

import Group from './group';
import InternalRadio from './radio';
import Button from './radioButton';
export { Button, Group };
const Radio = InternalRadio;
Radio.Button = Button;
Radio.Group = Group;
Radio.__ANT_RADIO = true;
export default Radio;