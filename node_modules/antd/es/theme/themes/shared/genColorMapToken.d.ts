import type { ColorMapToken, SeedToken } from '../../interface';
import type { GenerateColorMap, GenerateNeutralColorMap } from '../ColorMap';
interface PaletteGenerators {
    generateColorPalettes: GenerateColorMap;
    generateNeutralColorPalettes: GenerateNeutralColorMap;
}
export default function genColorMapToken(seed: SeedToken, { generateColorPalettes, generateNeutralColorPalettes }: PaletteGenerators): ColorMapToken;
export {};
