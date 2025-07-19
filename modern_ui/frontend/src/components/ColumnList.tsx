// src/components/ColumnList.tsx
import React, { useState } from 'react';
import { Column, Asset } from '../types';
import { ColumnListItem } from './ColumnListItem';
import { useSaveDescriptions, useEnhanceColumns } from '../hooks/useApi';
import toast from 'react-hot-toast';

interface ColumnListProps {
  columns: Column[];
  asset: Asset;
}

export const ColumnList: React.FC<ColumnListProps> = ({ columns: initialColumns, asset }) => {
  const [columns, setColumns] = useState(initialColumns);
  const saveMutation = useSaveDescriptions();
  const enhanceMutation = useEnhanceColumns();

  const handleDescriptionChange = (columnName: string, newDescription: string) => {
    setColumns(prev =>
      prev.map(c => (c.name === columnName ? { ...c, description: newDescription, hasChanges: true } : c))
    );
  };

  const handleSave = (columnName: string) => {
    const column = columns.find(c => c.name === columnName);
    if (column) {
      toast.promise(
        saveMutation.mutateAsync({
          asset_qualified_name: asset.qualified_name,
          columns: [{ name: column.name, description: column.description }],
        }),
        {
          loading: 'Saving description...',
          success: 'Description saved!',
          error: 'Failed to save description.',
        }
      );
    }
  };

  const handleEnhance = (columnName: string) => {
    const column = columns.find(c => c.name === columnName);
    if (column) {
      toast.promise(
        enhanceMutation.mutateAsync({
          asset_qualified_name: asset.qualified_name,
          asset_name: asset.name,
          columns: [{ name: column.name, type: column.type }],
        }),
        {
          loading: 'Generating description...',
          success: (descriptions) => {
            const newDescription = descriptions[0]?.description;
            if (newDescription) {
              handleDescriptionChange(columnName, newDescription);
              return 'Description generated!';
            }
            return 'Could not generate description.';
          },
          error: 'Failed to generate description.',
        }
      );
    }
  };

  return (
    <div className="mt-6">
      <h3 className="text-lg font-semibold">Columns</h3>
      <div className="mt-4 space-y-4">
        {columns.map(column => (
          <ColumnListItem
            key={column.name}
            column={column}
            onDescriptionChange={handleDescriptionChange}
            onSave={handleSave}
            onEnhance={handleEnhance}
            isSaving={saveMutation.isPending}
            isEnhancing={enhanceMutation.isPending}
          />
        ))}
      </div>
    </div>
  );
};