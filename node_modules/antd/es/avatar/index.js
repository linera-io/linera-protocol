"use client";

import InternalAvatar from './avatar';
import Group from './group';
export { Group };
const Avatar = InternalAvatar;
Avatar.Group = Group;
export default Avatar;