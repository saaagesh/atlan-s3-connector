import { useState, useEffect } from 'react';
import { Asset } from '../types';
import { useGenerateAssetDescription, useSaveAssetDescription } from '../hooks/useApi';
import toast from 'react-hot-toast';
import { SparklesIcon, CheckIcon, XMarkIcon, PencilIcon } from '@heroicons/react/24/outline';
import { LoadingSpinner } from './LoadingSpinner';
import TextareaAutosize from 'react-textarea-autosize';

interface AssetDescriptionProps {
    asset: Asset;
}

export const AssetDescription = ({ asset }: AssetDescriptionProps) => {
    const [isEditing, setIsEditing] = useState(false);
    const [description, setDescription] = useState(asset.user_description || asset.description || '');

    const generateMutation = useGenerateAssetDescription();
    const saveMutation = useSaveAssetDescription();

    useEffect(() => {
        setDescription(asset.user_description || asset.description || '');
    }, [asset.user_description, asset.description]);

    const handleGenerate = () => {
        toast.promise(
            generateMutation.mutateAsync(asset),
            {
                loading: 'Generating asset description...',
                success: (newDescription) => {
                    setDescription(newDescription);
                    handleSave(newDescription);
                    return 'Asset description generated and saved!';
                },
                error: 'Failed to generate description.'
            }
        );
    };

    const handleSave = (descToSave: string) => {
        toast.promise(
            saveMutation.mutateAsync({ asset, description: descToSave }),
            {
                loading: 'Saving asset description...',
                success: 'Asset description saved!',
                error: 'Failed to save description.'
            }
        );
        setIsEditing(false);
    };
    
    const handleCancel = () => {
        setDescription(asset.user_description || asset.description || '');
        setIsEditing(false);
    };

    return (
        <div className="bg-white p-6 border border-gray-200 rounded-lg shadow-sm mb-6">
            <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-semibold text-atlan-dark">Asset Description</h3>
                <div className="flex items-center space-x-2">
                    <button
                        onClick={handleGenerate}
                        disabled={generateMutation.isPending || saveMutation.isPending}
                        className="p-2 rounded-full text-atlan-blue bg-blue-50 hover:bg-blue-100 focus:outline-none focus:ring-2 focus:ring-atlan-blue focus:ring-offset-2 disabled:opacity-50"
                        title="Generate with AI"
                    >
                        {generateMutation.isPending ? <LoadingSpinner size="sm" /> : <SparklesIcon className="w-5 h-5" />}
                    </button>
                    {!isEditing && (
                        <button
                            onClick={() => setIsEditing(true)}
                            className="p-2 rounded-full text-gray-600 bg-gray-50 hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
                            title="Edit description"
                        >
                            <PencilIcon className="w-5 h-5" />
                        </button>
                    )}
                </div>
            </div>
            <div>
                {isEditing ? (
                    <div>
                        <TextareaAutosize
                            minRows={3}
                            value={description}
                            onChange={(e) => setDescription(e.target.value)}
                            className="w-full p-2 text-sm text-gray-700 bg-gray-50 rounded-md border border-gray-300 focus:bg-white focus:ring-2 focus:ring-atlan-blue focus:border-transparent resize-none"
                        />
                        <div className="flex justify-end space-x-2 mt-2">
                            <button onClick={handleCancel} className="px-3 py-1 text-sm font-medium text-gray-700 rounded-md hover:bg-gray-100">
                                <XMarkIcon className="w-5 h-5" />
                            </button>
                            <button 
                                onClick={() => handleSave(description)} 
                                disabled={saveMutation.isPending} 
                                className="px-3 py-1 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-50"
                            >
                                {saveMutation.isPending ? <LoadingSpinner size="sm" /> : <CheckIcon className="w-5 h-5" />}
                            </button>
                        </div>
                    </div>
                ) : (
                    <p className="text-sm text-gray-700 cursor-pointer" onClick={() => setIsEditing(true)}>
                        {description || <span className="text-gray-400">No description available. Click to edit or generate one.</span>}
                    </p>
                )}
            </div>
        </div>
    );
};
