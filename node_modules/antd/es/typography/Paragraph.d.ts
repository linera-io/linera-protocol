import * as React from 'react';
import type { BlockProps } from './Base';
export interface ParagraphProps extends BlockProps<'div'>, Omit<React.HTMLAttributes<HTMLDivElement>, 'type' | keyof BlockProps<'div'>> {
}
declare const Paragraph: React.ForwardRefExoticComponent<ParagraphProps & React.RefAttributes<HTMLElement>>;
export default Paragraph;
