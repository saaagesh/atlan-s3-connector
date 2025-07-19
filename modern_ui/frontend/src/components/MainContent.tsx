import { useState, useEffect } from 'react';
import {
    SparklesIcon,
    DocumentTextIcon,
    CheckCircleIcon,
    ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { useColumns, useEnhanceColumns, useSaveDescriptions } from '../hooks/useApi';
import { Asset, ColumnWithStatus } from '../types';
import { LoadingSpinner } from './LoadingSpinner';
import { ColumnCard } from './ColumnCard';
import { AssetDescription } from './AssetDescription';
import toast from 'react-hot-toast';

interface MainContentProps {
    selectedAsset: Asset | null;
}

export const MainContent = ({ selectedAsset }: MainContentProps) => {
    const [columns, setColumns] = useState<ColumnWithStatus[]>([]);
    const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);

    const {
        data: fetchedColumns,
        isLoading: columnsLoading,
        error: columnsError,
        refetch: refetchColumns,
    } = useColumns(selectedAsset);

    const generateMutation = useEnhanceColumns();
    const saveMutation = useSaveDescriptions();

    useEffect(() => {
        if (fetchedColumns) {
            setColumns(fetchedColumns.map(col => ({ ...col, hasChanges: false, isGenerating: false })));
            setHasUnsavedChanges(false);
        }
    }, [fetchedColumns]);
    
    useEffect(() => {
        if (selectedAsset) {
            refetchColumns();
        }
    }, [selectedAsset, refetchColumns]);


    const handleGenerateAll = async () => {
        if (!selectedAsset || columns.length === 0) return;

        const columnsToEnhance = columns.filter(c => !c.description);
        if (columnsToEnhance.length === 0) {
            toast.success('All columns already have descriptions.');
            return;
        }

        setColumns(prev => prev.map(col => ({ ...col, isGenerating: true })));

        toast.promise(
            generateMutation.mutateAsync({
                asset_qualified_name: selectedAsset.qualified_name,
                asset_name: selectedAsset.name,
                columns: columnsToEnhance.map(col => ({ name: col.name, type: col.type }))
            }),
            {
                loading: 'Generating descriptions for all columns...',
                success: (result) => {
                    setColumns(prev => prev.map(col => {
                        const generated = result.find(r => r.name === col.name);
                        return {
                            ...col,
                            description: generated?.description || col.description,
                            isGenerating: false,
                            hasChanges: !!generated?.description
                        };
                    }));
                    setHasUnsavedChanges(true);
                    return `Generated descriptions for ${result.length} columns`;
                },
                error: (err) => {
                    setColumns(prev => prev.map(col => ({ ...col, isGenerating: false })));
                    return `Failed to generate descriptions: ${err.message}`;
                }
            }
        );
    };

    const handleGenerateOne = async (columnName: string) => {
        if (!selectedAsset) return;

        const column = columns.find(col => col.name === columnName);
        if (!column) return;

        setColumns(prev => prev.map(col =>
            col.name === columnName ? { ...col, isGenerating: true } : col
        ));

        toast.promise(
             generateMutation.mutateAsync({
                asset_qualified_name: selectedAsset.qualified_name,
                asset_name: selectedAsset.name,
                columns: [{ name: column.name, type: column.type }]
            }),
            {
                loading: `Generating description for ${columnName}...`,
                success: (result) => {
                    const generated = result[0];
                     if (generated) {
                        setColumns(prev => prev.map(col =>
                            col.name === columnName
                                ? {
                                    ...col,
                                    description: generated.description,
                                    isGenerating: false,
                                    hasChanges: true
                                }
                                : col
                        ));
                        setHasUnsavedChanges(true);
                        return `Generated description for ${columnName}`;
                    }
                    return 'No description generated.';
                },
                error: (err) => {
                     setColumns(prev => prev.map(col =>
                        col.name === columnName ? { ...col, isGenerating: false } : col
                    ));
                    return `Failed to generate description: ${err.message}`;
                }
            }
        );
    };

    const handleDescriptionChange = (columnName: string, newDescription: string) => {
        setColumns(prev => prev.map(col =>
            col.name === columnName
                ? { ...col, description: newDescription, hasChanges: true }
                : col
        ));
        setHasUnsavedChanges(true);
    };

    const handleSaveAll = async () => {
        if (!selectedAsset || !hasUnsavedChanges) return;

        const columnsToSave = columns.filter(col => col.hasChanges && col.description.trim());

        if (columnsToSave.length === 0) {
            toast.error('No changes to save');
            return;
        }

        toast.promise(
            saveMutation.mutateAsync({
                asset_qualified_name: selectedAsset.qualified_name,
                columns: columnsToSave.map(col => ({ name: col.name, description: col.description }))
            }),
            {
                loading: 'Saving descriptions to Atlan...',
                success: () => {
                    setColumns(prev => prev.map(col => ({ ...col, hasChanges: false })));
                    setHasUnsavedChanges(false);
                    return `Saved descriptions for ${columnsToSave.length} columns to Atlan`;
                },
                error: (err) => `Failed to save descriptions: ${err.message}`
            }
        );
    };

    if (!selectedAsset) {
        return (
            <div className="flex-1 flex items-center justify-center bg-gray-50">
                <div className="text-center">
                    <DocumentTextIcon className="mx-auto h-12 w-12 text-gray-400" />
                    <h3 className="mt-2 text-sm font-medium text-gray-900">No asset selected</h3>
                    <p className="mt-1 text-sm text-gray-500">
                        Select a data source and asset from the sidebar to get started.
                    </p>
                </div>
            </div>
        );
    }

    if (columnsLoading) {
        return (
            <div className="flex-1 flex items-center justify-center">
                <div className="text-center">
                    <LoadingSpinner size="lg" />
                    <p className="mt-2 text-sm text-gray-500">Loading columns...</p>
                </div>
            </div>
        );
    }

    if (columnsError) {
        return (
            <div className="flex-1 flex items-center justify-center">
                <div className="text-center">
                    <ExclamationTriangleIcon className="mx-auto h-12 w-12 text-red-400" />
                    <h3 className="mt-2 text-sm font-medium text-gray-900">Error loading columns</h3>
                    <p className="mt-1 text-sm text-gray-500">
                        {columnsError.message || 'An unexpected error occurred.'}
                    </p>
                </div>
            </div>
        );
    }

    return (
        <div className="flex-1 flex flex-col bg-gray-50">
            {/* Header */}
            <div className="bg-white border-b border-gray-200 px-6 py-4">
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-lg font-semibold text-atlan-dark">
                            {selectedAsset.name}
                        </h2>
                        <p className="text-sm text-atlan-gray">
                            {columns.length} columns • {selectedAsset.type.toUpperCase()}
                        </p>
                    </div>
                    <div className="flex space-x-3">
                        <button
                            onClick={handleGenerateAll}
                            disabled={generateMutation.isPending || saveMutation.isPending}
                            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-atlan-blue hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-atlan-blue disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                            <SparklesIcon className="w-4 h-4 mr-2" />
                            {generateMutation.isPending ? 'Generating...' : 'Generate All'}
                        </button>
                        {hasUnsavedChanges && (
                            <button
                                onClick={handleSaveAll}
                                disabled={saveMutation.isPending}
                                className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                                <CheckCircleIcon className="w-4 h-4 mr-2" />
                                {saveMutation.isPending ? 'Saving...' : 'Save to Atlan'}
                            </button>
                        )}
                    </div>
                </div>
            </div>

            {/* Main Area */}
            <div className="flex-1 overflow-y-auto p-6">
                <AssetDescription asset={selectedAsset} />
                 {columns.length === 0 ? (
                     <div className="text-center text-gray-500">
                        <DocumentTextIcon className="mx-auto h-12 w-12 text-gray-400" />
                        <h3 className="mt-2 text-sm font-medium text-gray-900">No columns found</h3>
                        <p className="mt-1 text-sm text-gray-500">This asset may not have columns or they are still being loaded.</p>
                    </div>
                 ) : (
                    <div className="space-y-4">
                        {columns.map((column) => (
                            <ColumnCard
                                key={column.name}
                                column={column}
                                onGenerate={() => handleGenerateOne(column.name)}
                                onDescriptionChange={(description) =>
                                    handleDescriptionChange(column.name, description)
                                }
                            />
                        ))}
                    </div>
                 )}
            </div>
        </div>
    );
};