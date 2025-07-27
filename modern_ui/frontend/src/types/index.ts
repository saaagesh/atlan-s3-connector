
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

export interface GlossaryTerm {
  guid: string;
  name: string;
  qualified_name: string;
  description?: string;
  readme?: string;
  category?: string;
  type: 'term';
}

export interface GlossaryCategory {
  guid: string;
  name: string;
  qualified_name: string;
  description?: string;
  readme?: string;
  terms: GlossaryTerm[];
  type: 'category';
}

export interface BusinessGlossaryResponse {
  success: boolean;
  categories: GlossaryCategory[];
  terms: GlossaryTerm[];
  error?: string;
}

export interface GenerateGlossaryReadmeRequest {
  item_guid: string;
  item_name: string;
  item_type: 'term' | 'category';
  current_description?: string;
}

export interface SaveGlossaryReadmeRequest {
  item_guid: string;
  item_type: 'term' | 'category';
  readme: string;
}
