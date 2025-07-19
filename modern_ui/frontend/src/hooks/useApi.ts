// src/hooks/useApi.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import * as api from '../services/api';
import { Asset, GenerateDescriptionsResponse } from '../types';

export const useAssetsBySource = (source: string) => {
  return useQuery({
    queryKey: ['assets', source],
    queryFn: () => api.getAssetsBySource(source),
    enabled: !!source,
  });
};

export const useColumns = (asset: Asset | null) => {
  return useQuery({
    queryKey: ['columns', asset?.qualified_name],
    queryFn: () => api.getColumns(asset!.qualified_name, asset!.type),
    enabled: !!asset,
  });
};

export const useEnhanceColumns = () => {
    return useMutation<GenerateDescriptionsResponse[], Error, { asset_qualified_name: string; asset_name: string; columns: { name: string; type: string }[] }>({
        mutationFn: (payload) => api.enhanceColumns(payload),
    });
};

export const useSaveDescriptions = () => {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (payload: { asset_qualified_name: string; columns: { name: string; description: string }[] }) => api.saveDescriptions(payload),
        onSuccess: (_data, variables) => {
            queryClient.invalidateQueries({ queryKey: ['columns', variables.asset_qualified_name] });
        },
    });
};

export const useGenerateAssetDescription = () => {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (asset: Asset) => api.generateAssetDescription(asset),
        onSuccess: (_data, variables) => {
            queryClient.invalidateQueries({ queryKey: ['assets', variables.type] });
        },
    });
};

export const useSaveAssetDescription = () => {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (params: { asset: Asset, description: string }) => api.saveAssetDescription(params.asset, params.description),
        onSuccess: (_data, variables) => {
            queryClient.invalidateQueries({ queryKey: ['assets', variables.asset.type] });
        },
    });
};