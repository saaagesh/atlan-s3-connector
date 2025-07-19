// src/services/api.ts
import axios from 'axios';
import { Asset, Column } from '../types';

const api = axios.create({
  baseURL: '/api',
});

export const getAssetsBySource = async (source: string): Promise<Asset[]> => {
  const { data } = await api.post('/assets_by_source', { source });
  if (data.success) {
    return data.assets;
  }
  throw new Error(data.error || 'Failed to fetch assets');
};

export const getColumns = async (assetQualifiedName: string, sourceType: string): Promise<Column[]> => {
  const { data } = await api.post('/columns', { asset_qualified_name: assetQualifiedName, source_type: sourceType });
  if (data.success) {
    return data.columns;
  }
  throw new Error(data.error || 'Failed to fetch columns');
};

export const enhanceColumns = async (payload: { asset_qualified_name: string; asset_name: string; columns: { name: string; type: string }[] }) => {
  const { data } = await api.post('/enhance_columns', payload);
  if (data.success) {
    return data.descriptions;
  }
  throw new Error(data.error || 'Failed to enhance columns');
};

export const saveDescriptions = async (payload: { asset_qualified_name: string; columns: { name: string; description: string }[] }) => {
  const { data } = await api.post('/save_descriptions', payload);
  if (data.success) {
    return data;
  }
  throw new Error(data.error || 'Failed to save descriptions');
};

export const generateAssetDescription = async (asset: Asset) => {
    const { data } = await api.post('/generate_asset_description', {
        asset_qualified_name: asset.qualified_name,
        asset_name: asset.name,
        asset_type: asset.type,
    });
    if (data.success) {
        return data.description;
    }
    throw new Error(data.error || 'Failed to generate asset description');
};

export const saveAssetDescription = async (asset: Asset, description: string) => {
    const { data } = await api.post('/save_asset_description', {
        asset_qualified_name: asset.qualified_name,
        asset_type: asset.type,
        description: description,
    });
    if (data.success) {
        return data;
    }
    throw new Error(data.error || 'Failed to save asset description');
};
