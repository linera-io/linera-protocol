import * as React from 'react';
import type { IconDefinition } from '@ant-design/icons-svg/lib/types';
import type { IconBaseProps } from './Icon';
import { getTwoToneColor, setTwoToneColor } from './twoTonePrimaryColor';
import type { TwoToneColor } from './twoTonePrimaryColor';
export interface AntdIconProps extends IconBaseProps {
    twoToneColor?: TwoToneColor;
}
export interface IconComponentProps extends AntdIconProps {
    icon: IconDefinition;
}
interface IconBaseComponent<Props> extends React.ForwardRefExoticComponent<Props & React.RefAttributes<HTMLSpanElement>> {
    getTwoToneColor: typeof getTwoToneColor;
    setTwoToneColor: typeof setTwoToneColor;
}
declare const Icon: IconBaseComponent<IconComponentProps>;
export default Icon;
