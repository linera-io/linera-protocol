import type { ForwardRefExoticComponent, RefAttributes } from 'react';
import type { AvatarProps } from './avatar';
import Group from './group';
export type { AvatarProps } from './avatar';
export type { GroupProps } from './group';
export { Group };
type CompoundedComponent = ForwardRefExoticComponent<AvatarProps & RefAttributes<HTMLSpanElement>> & {
    Group: typeof Group;
};
declare const Avatar: CompoundedComponent;
export default Avatar;
