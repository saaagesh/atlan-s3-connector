import { useState, useEffect } from 'react';
import { ColumnWithStatus } from '../types';
import { SparklesIcon } from '@heroicons/react/24/outline';
import { LoadingSpinner } from './LoadingSpinner';
import TextareaAutosize from 'react-textarea-autosize';

interface ColumnCardProps {
    column: ColumnWithStatus;
    onGenerate: () => void;
    onDescriptionChange: (newDescription: string) => void;
}

export const ColumnCard = ({ column, onGenerate, onDescriptionChange }: ColumnCardProps) => {
    const [currentDescription, setCurrentDescription] = useState(column.description);

    useEffect(() => {
        setCurrentDescription(column.description);
    }, [column.description]);

    const handleBlur = () => {
        if (currentDescription !== column.description) {
            onDescriptionChange(currentDescription);
        }
    };

    return (
        <div className="bg-white border border-gray-200 rounded-lg shadow-sm transition-shadow hover:shadow-md">
            <div className="p-4">
                <div className="flex items-start justify-between">
                    <div className="flex-grow">
                        <h3 className="font-semibold text-atlan-dark">{column.name}</h3>
                        <p className="text-sm text-atlan-gray uppercase">{column.type}</p>
                    </div>
                    <button
                        onClick={onGenerate}
                        disabled={column.isGenerating}
                        className="flex items-center justify-center p-2 rounded-full text-atlan-blue bg-blue-50 hover:bg-blue-100 focus:outline-none focus:ring-2 focus:ring-atlan-blue focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Generate with AI"
                    >
                        {column.isGenerating ? (
                            <LoadingSpinner size="sm" />
                        ) : (
                            <SparklesIcon className="w-5 h-5" />
                        )}
                    </button>
                </div>
                <div className="mt-3">
                    <TextareaAutosize
                        minRows={2}
                        value={currentDescription}
                        onChange={(e) => setCurrentDescription(e.target.value)}
                        onBlur={handleBlur}
                        placeholder="No description. Click to edit or generate one with AI."
                        className="w-full p-2 text-sm text-gray-700 bg-gray-50 rounded-md border border-gray-200 focus:bg-white focus:ring-2 focus:ring-atlan-blue focus:border-transparent resize-none"
                    />
                </div>
            </div>
        </div>
    );
};
