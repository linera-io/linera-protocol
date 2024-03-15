import type React from 'react';
export type Direction = 'rtl' | 'ltr' | 'ttb' | 'btt';
export type OnStartMove = (e: React.MouseEvent | React.TouchEvent, valueIndex: number, startValues?: number[]) => void;
export type AriaValueFormat = (value: number) => string;
export type SemanticName = 'tracks' | 'track' | 'rail' | 'handle';
export type SliderClassNames = Partial<Record<SemanticName, string>>;
export type SliderStyles = Partial<Record<SemanticName, React.CSSProperties>>;
