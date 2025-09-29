import axios, { type AxiosInstance } from 'axios';
import { type ApiResponse, type ConnectionStatus } from '../types/api';
import { type QueryResult } from '../types/query';

class ApiService {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: '/api',
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        console.error('API Error:', error);
        return Promise.reject(error);
      }
    );
  }

  // Database connection methods
  async connectDatabase(uri: string, databaseType: string = 'auto'): Promise<ApiResponse> {
    const response = await this.client.post<ApiResponse>('/database/connect', {
      uri,
      database_type: databaseType
    });
    return response.data;
  }

  async getConnectionStatus(): Promise<ApiResponse<ConnectionStatus>> {
    const response = await this.client.get<ApiResponse<ConnectionStatus>>('/database/status');
    return response.data;
  }

  // Query methods
  async queryData(naturalLanguageQuery: string): Promise<ApiResponse<QueryResult>> {
    const response = await this.client.post<ApiResponse<QueryResult>>('/query/execute', {
      natural_language_query: naturalLanguageQuery
    });
    return response.data;
  }
}

export const apiService = new ApiService();
