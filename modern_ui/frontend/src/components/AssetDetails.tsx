// src/components/AssetDetails.tsx
import React, { useState, useEffect } from 'react';
import { Asset } from '../types';
import { useGenerateAssetDescription, useSaveAssetDescription } from '../hooks/useApi';
import toast from 'react-hot-toast';
import { SparklesIcon, CheckIcon, XMarkIcon, PencilIcon } from '@heroicons/react/24/outline';
import { LoadingSpinner } from './LoadingSpinner';

interface AssetDetailsProps {
  asset: Asset;
}

export const AssetDetails: React.FC<AssetDetailsProps> = ({ asset }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [description, setDescription] = useState(asset.description || '');
  const generateMutation = useGenerateAssetDescription();
  const saveMutation = useSaveAssetDescription();

  useEffect(() => {
    setDescription(asset.description || '');
  }, [asset.description]);

  const handleGenerate = () => {
    toast.promise(
      generateMutation.mutateAsync(asset),
      {
        loading: 'Generating description...',
        success: (newDescription) => {
          setDescription(newDescription);
          return 'Description generated!';
        },
        error: 'Failed to generate description.',
      }
    );
  };

  const handleSave = () => {
    toast.promise(
      saveMutation.mutateAsync({ asset, description }),
      {
        loading: 'Saving description...',
        success: 'Description saved!',
        error: 'Failed to save description.',
      }
    );
    setIsEditing(false);
  };

  const handleCancel = () => {
    setDescription(asset.description || '');
    setIsEditing(false);
  };

  return (
    <div className="bg-white p-6 border border-gray-200 rounded-lg shadow-sm">
      <div className="flex justify-between items-center">
        <h2 className="text-xl font-bold text-gray-800">{asset.name}</h2>
        <div className="flex items-center space-x-2">
          <button
            onClick={handleGenerate}
            disabled={generateMutation.isPending || saveMutation.isPending}
            className="p-1 text-blue-600 hover:text-blue-800 disabled:text-gray-400"
            title="Generate with AI"
          >
            {generateMutation.isPending ? <LoadingSpinner size="sm" /> : <SparklesIcon className="w-5 h-5" />}
          </button>
          {!isEditing && (
            <button
              onClick={() => setIsEditing(true)}
              className="p-1 text-gray-600 hover:text-gray-800"
              title="Edit description"
            >
              <PencilIcon className="w-5 h-5" />
            </button>
          )}
        </div>
      </div>
      <div className="mt-4">
        {isEditing ? (
          <div>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              className="w-full p-2 border rounded-md"
              rows={4}
            />
            <div className="flex justify-end space-x-2 mt-2">
              <button onClick={handleCancel} className="p-1 text-gray-600 hover:text-gray-800">
                <XMarkIcon className="w-5 h-5" />
              </button>
              <button onClick={handleSave} disabled={saveMutation.isPending} className="p-1 text-green-600 hover:text-green-800 disabled:text-gray-400">
                {saveMutation.isPending ? <LoadingSpinner size="sm" /> : <CheckIcon className="w-5 h-5" />}
              </button>
            </div>
          </div>
        ) : (
          <p className="text-gray-700" onClick={() => setIsEditing(true)}>
            {description || <span className="text-gray-400">No description. Click to add or generate one.</span>}
          </p>
        )}
      </div>
    </div>
  );
};