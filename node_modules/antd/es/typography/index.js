"use client";

import Link from './Link';
import Paragraph from './Paragraph';
import Text from './Text';
import Title from './Title';
import OriginTypography from './Typography';
const Typography = OriginTypography;
Typography.Text = Text;
Typography.Link = Link;
Typography.Title = Title;
Typography.Paragraph = Paragraph;
export default Typography;