import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiService } from '../services/api';
import { GenerateDescriptionsRequest, SaveDescriptionsRequest } from '../types';

export const useAssetsBySource = (source: string) => {
  return useQuery({
    queryKey: ['assets', source],
    queryFn: () => apiService.getAssetsBySource(source),
    enabled: !!source,
  });
};

export const useColumns = (assetQualifiedName: string, sourceType: string) => {
  return useQuery({
    queryKey: ['columns', assetQualifiedName],
    queryFn: () => apiService.getColumns(assetQualifiedName, sourceType),
    enabled: !!assetQualifiedName && !!sourceType,
  });
};

export const useGenerateDescriptions = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (request: GenerateDescriptionsRequest) => 
      apiService.generateDescriptions(request),
    onSuccess: (_, variables) => {
      // Invalidate columns query to refresh data
      queryClient.invalidateQueries({
        queryKey: ['columns', variables.asset_qualified_name]
      });
    },
  });
};

export const useSaveDescriptions = () => {
  return useMutation({
    mutationFn: (request: SaveDescriptionsRequest) => 
      apiService.saveDescriptions(request),
  });
};