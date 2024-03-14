/// <reference types="react" />
import Collapse from './Collapse';
export type { CollapsePanelProps, CollapseProps } from './interface';
export default Collapse;
/**
 * @deprecated use `items` instead, will be removed in `v4.0.0`
 */
export declare const Panel: import("react").ForwardRefExoticComponent<import("./interface").CollapsePanelProps & import("react").RefAttributes<HTMLDivElement>>;
