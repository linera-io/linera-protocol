import InternalAnchor from './Anchor';
import AnchorLink from './AnchorLink';
export type { AnchorProps } from './Anchor';
export type { AnchorLinkProps } from './AnchorLink';
type InternalAnchorType = typeof InternalAnchor;
type CompoundedComponent = InternalAnchorType & {
    Link: typeof AnchorLink;
};
declare const Anchor: CompoundedComponent;
export default Anchor;
