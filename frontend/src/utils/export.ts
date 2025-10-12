import { saveAs } from 'file-saver';
import Papa from 'papaparse';

export class DataExporter {
  static toCSV(data: any[], filename: string = 'export.csv') {
    const csv = Papa.unparse(data);
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    saveAs(blob, filename);
  }

  static toJSON(data: any[], filename: string = 'export.json') {
    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    saveAs(blob, filename);
  }

  static toMarkdown(data: any[]): string {
    if (!data || data.length === 0) return '';

    const keys = Object.keys(data[0]);
    
    // Header
    let markdown = '| ' + keys.join(' | ') + ' |\n';
    markdown += '| ' + keys.map(() => '---').join(' | ') + ' |\n';
    
    // Rows
    data.forEach(row => {
      markdown += '| ' + keys.map(key => String(row[key] ?? '')).join(' | ') + ' |\n';
    });
    
    return markdown;
  }

  static copyToClipboard(text: string): Promise<void> {
    return navigator.clipboard.writeText(text);
  }

  static generateFilename(queryText: string, extension: string): string {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const sanitized = queryText
      .replace(/[^a-zA-Z0-9]/g, '_')
      .slice(0, 30);
    return `query_${sanitized}_${timestamp}.${extension}`;
  }
}

