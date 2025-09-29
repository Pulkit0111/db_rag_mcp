export interface ApiResponse<T = any> {
  success: boolean;
  result?: T;
  error?: string;
}

export interface ConnectionStatus {
  connected: boolean;
  database_type?: string;
  host?: string;
  port?: number;
  database?: string;
  message?: string;
}