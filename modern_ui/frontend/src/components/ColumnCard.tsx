import { useState } from 'react';
import { 
  SparklesIcon, 
  PencilIcon,
  CheckIcon,
  XMarkIcon
} from '@heroicons/react/24/outline';
import { ColumnWithStatus } from '../types';
import { LoadingSpinner } from './LoadingSpinner';

interface ColumnCardProps {
  column: ColumnWithStatus;
  onGenerate: () => void;
  onDescriptionChange: (description: string) => void;
}

export const ColumnCard = ({ column, onGenerate, onDescriptionChange }: ColumnCardProps) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(column.description);

  const handleSaveEdit = () => {
    onDescriptionChange(editValue);
    setIsEditing(false);
  };

  const handleCancelEdit = () => {
    setEditValue(column.description);
    setIsEditing(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSaveEdit();
    } else if (e.key === 'Escape') {
      handleCancelEdit();
    }
  };

  return (
    <div className={`bg-white border rounded-lg p-4 transition-all duration-200 ${
      column.hasChanges ? 'border-blue-200 bg-blue-50' : 'border-gray-200 hover:border-gray-300'
    }`}>
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <div className="flex items-center space-x-2 mb-2">
            <h3 className="text-sm font-medium text-gray-900 truncate">
              {column.name}
            </h3>
            {column.hasChanges && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                Modified
              </span>
            )}
          </div>
          
          <div className="mt-2">
            {isEditing ? (
              <div className="space-y-2">
                <textarea
                  value={editValue}
                  onChange={(e) => setEditValue(e.target.value)}
                  onKeyDown={handleKeyDown}
                  className="w-full p-2 text-sm border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none"
                  rows={3}
                  placeholder="Enter column description..."
                  autoFocus
                />
                <div className="flex space-x-2">
                  <button
                    onClick={handleSaveEdit}
                    className="inline-flex items-center px-2 py-1 text-xs font-medium text-green-700 bg-green-100 rounded hover:bg-green-200"
                  >
                    <CheckIcon className="w-3 h-3 mr-1" />
                    Save
                  </button>
                  <button
                    onClick={handleCancelEdit}
                    className="inline-flex items-center px-2 py-1 text-xs font-medium text-gray-700 bg-gray-100 rounded hover:bg-gray-200"
                  >
                    <XMarkIcon className="w-3 h-3 mr-1" />
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
              <div className="group">
                <p className={`text-sm ${
                  column.description 
                    ? 'text-gray-700' 
                    : 'text-gray-400 italic'
                }`}>
                  {column.description || 'No description available'}
                </p>
                <button
                  onClick={() => setIsEditing(true)}
                  className="mt-1 opacity-0 group-hover:opacity-100 transition-opacity inline-flex items-center text-xs text-blue-600 hover:text-blue-800"
                >
                  <PencilIcon className="w-3 h-3 mr-1" />
                  Edit
                </button>
              </div>
            )}
          </div>
        </div>

        <div className="ml-4 flex-shrink-0">
          <button
            onClick={onGenerate}
            disabled={column.isGenerating || isEditing}
            className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-md text-blue-700 bg-blue-100 hover:bg-blue-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {column.isGenerating ? (
              <>
                <LoadingSpinner size="sm" className="mr-1" />
                Generating...
              </>
            ) : (
              <>
                <SparklesIcon className="w-3 h-3 mr-1" />
                Generate
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
};