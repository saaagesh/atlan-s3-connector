// src/components/ColumnListItem.tsx
import React, { useState, useEffect } from 'react';
import { ColumnWithStatus } from '../types';
import { SparklesIcon, CheckIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { LoadingSpinner } from './LoadingSpinner';

interface ColumnListItemProps {
  column: ColumnWithStatus;
  onDescriptionChange: (columnName: string, newDescription: string) => void;
  onSave: (columnName:string) => void;
  onEnhance: (columnName: string) => void;
  isSaving: boolean;
  isEnhancing: boolean;
}

export const ColumnListItem: React.FC<ColumnListItemProps> = ({
  column,
  onDescriptionChange,
  onSave,
  onEnhance,
  isSaving,
  isEnhancing,
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(column.description);

  useEffect(() => {
    setEditValue(column.description);
  }, [column.description]);

  const handleSave = () => {
    onDescriptionChange(column.name, editValue);
    onSave(column.name);
    setIsEditing(false);
  };

  const handleCancel = () => {
    setEditValue(column.description);
    setIsEditing(false);
  };

  const handleEnhance = () => {
    onEnhance(column.name);
  };

  return (
    <div className="bg-white p-4 border border-gray-200 rounded-lg shadow-sm">
      <div className="flex justify-between items-start">
        <div>
          <h4 className="font-semibold text-gray-800">{column.name}</h4>
          <p className="text-sm text-gray-500">{column.type}</p>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={handleEnhance}
            disabled={isEnhancing || isSaving}
            className="p-1 text-blue-600 hover:text-blue-800 disabled:text-gray-400"
            title="Generate with AI"
          >
            {isEnhancing ? <LoadingSpinner size="sm" /> : <SparklesIcon className="w-5 h-5" />}
          </button>
        </div>
      </div>
      <div className="mt-2">
        {isEditing ? (
          <div>
            <textarea
              value={editValue}
              onChange={(e) => setEditValue(e.target.value)}
              className="w-full p-2 border rounded-md"
              rows={3}
            />
            <div className="flex justify-end space-x-2 mt-2">
              <button onClick={handleCancel} className="p-1 text-gray-600 hover:text-gray-800">
                <XMarkIcon className="w-5 h-5" />
              </button>
              <button onClick={handleSave} disabled={isSaving} className="p-1 text-green-600 hover:text-green-800 disabled:text-gray-400">
                {isSaving ? <LoadingSpinner size="sm" /> : <CheckIcon className="w-5 h-5" />}
              </button>
            </div>
          </div>
        ) : (
          <p className="text-gray-700" onClick={() => setIsEditing(true)}>
            {column.description || <span className="text-gray-400">No description. Click to add.</span>}
          </p>
        )}
      </div>
    </div>
  );
};