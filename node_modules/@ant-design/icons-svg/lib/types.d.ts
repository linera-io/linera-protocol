export interface AbstractNode {
    tag: string;
    attrs: {
        [key: string]: string;
    };
    children?: AbstractNode[];
}
export interface IconDefinition {
    name: string;
    theme: ThemeType;
    icon: ((primaryColor: string, secondaryColor: string) => AbstractNode) | AbstractNode;
}
export type ThemeType = 'filled' | 'outlined' | 'twotone';
export type ThemeTypeUpperCase = 'Filled' | 'Outlined' | 'TwoTone';
