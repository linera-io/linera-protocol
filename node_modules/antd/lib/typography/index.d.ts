import Link from './Link';
import Paragraph from './Paragraph';
import Text from './Text';
import Title from './Title';
import OriginTypography from './Typography';
export type TypographyProps = typeof OriginTypography & {
    Text: typeof Text;
    Link: typeof Link;
    Title: typeof Title;
    Paragraph: typeof Paragraph;
};
declare const Typography: TypographyProps;
export default Typography;
