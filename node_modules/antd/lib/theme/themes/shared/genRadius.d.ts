import type { MapToken } from '../../interface';
declare const genRadius: (radiusBase: number) => Pick<MapToken, 'borderRadiusXS' | 'borderRadiusSM' | 'borderRadiusLG' | 'borderRadius' | 'borderRadiusOuter'>;
export default genRadius;
