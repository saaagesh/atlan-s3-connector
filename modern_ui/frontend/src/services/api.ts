import axios from 'axios';
import { 
  Asset, 
  Column, 
  GenerateDescriptionsRequest, 
  GenerateDescriptionsResponse,
  SaveDescriptionsRequest 
} from '../types';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
});

export const apiService = {
  async getAssetsBySource(source: string): Promise<Asset[]> {
    const response = await api.post('/assets_by_source', { source });
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to fetch assets');
    }
    return response.data.assets;
  },

  async getColumns(assetQualifiedName: string, sourceType: string): Promise<Column[]> {
    const response = await api.post('/columns', {
      asset_qualified_name: assetQualifiedName,
      source_type: sourceType,
    });
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to fetch columns');
    }
    return response.data.columns;
  },

  async generateDescriptions(request: GenerateDescriptionsRequest): Promise<GenerateDescriptionsResponse[]> {
    const response = await api.post('/enhance_columns', request);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to generate descriptions');
    }
    return response.data.descriptions;
  },

  async saveDescriptions(request: SaveDescriptionsRequest): Promise<string> {
    const response = await api.post('/save_descriptions', request);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to save descriptions');
    }
    return response.data.message;
  },
};