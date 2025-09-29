export interface QueryResult {
  success: boolean;
  message?: string;
  original_query?: string;
  generated_sql?: string;
  row_count?: number;
  results?: any[];
  execution_time?: number;
  error?: string;
}

export interface QueryHistoryItem {
  id: string;
  query: string;
  result?: QueryResult;
  timestamp: Date;
}

export interface ExampleQuery {
  text: string;
  description: string;
}
