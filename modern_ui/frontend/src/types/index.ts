
export interface Asset {
  name: string;
  qualified_name: string;
  type: 'table' | 's3';
  description?: string;
  user_description?: string;
  has_description?: boolean;
  has_owners?: boolean;
  has_readme?: boolean;
  guid?: string;
  has_columns?: boolean;
}

export interface Column {
  name: string;
  type: string;
  description: string;
  has_description: boolean;
  qualified_name?: string;
  guid?: string;
}

export interface ColumnWithStatus extends Column {
  isGenerating?: boolean;
  hasChanges?: boolean;
}

export interface Source {
  id: string;
  name: string;
  type: 'postgres' | 's3' | 'snowflake';
  icon: string;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}

export interface GenerateDescriptionsRequest {
  asset_qualified_name: string;
  columns: Column[];
}

export interface GenerateDescriptionsResponse {
  name: string;
  description: string;
}

export interface SaveDescriptionsRequest {
  asset_qualified_name: string;
  source_type: string;
  columns: Column[];
}
