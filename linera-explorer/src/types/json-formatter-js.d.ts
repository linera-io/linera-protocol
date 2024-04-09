declare module 'json-formatter-js' {
    export default class JSONFormatter {
      constructor(json: any, open?: number, config?: { theme?: string });
      render(): HTMLElement;
    }
  }
  