import { useState, useEffect } from 'react';
import {
    BookOpenIcon,
    TagIcon,
    SparklesIcon,
    CheckCircleIcon,
    ChevronDownIcon,
    ChevronRightIcon
} from '@heroicons/react/24/outline';
import { LoadingSpinner } from './LoadingSpinner';
import toast from 'react-hot-toast';

interface GlossaryTerm {
    guid: string;
    name: string;
    qualified_name: string;
    description?: string;
    readme?: string;
    category?: string;
    type: 'term' | 'category';
    glossary_name?: string;
}

interface GlossaryCategory {
    guid: string;
    name: string;
    qualified_name: string;
    description?: string;
    readme?: string;
    terms: GlossaryTerm[];
    type: 'category';
    glossary_name?: string;
}

export const BusinessGlossaryTab = () => {
    const [categories, setCategories] = useState<GlossaryCategory[]>([]);
    const [terms, setTerms] = useState<GlossaryTerm[]>([]);
    const [loading, setLoading] = useState(true);
    const [selectedItem, setSelectedItem] = useState<GlossaryTerm | GlossaryCategory | null>(null);
    const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
    const [generatedReadme, setGeneratedReadme] = useState<string>('');
    const [isGenerating, setIsGenerating] = useState(false);
    const [isSaving, setIsSaving] = useState(false);

    useEffect(() => {
        fetchGlossaryData();
    }, []);

    const fetchGlossaryData = async () => {
        try {
            setLoading(true);
            const response = await fetch('/api/business_glossary', {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await response.json();

            if (data.success) {
                setCategories(data.categories || []);
                setTerms(data.terms || []);
            } else {
                toast.error(data.error || 'Failed to fetch business glossary');
            }
        } catch (error) {
            toast.error('Error fetching business glossary data');
            console.error('Error:', error);
        } finally {
            setLoading(false);
        }
    };

    const toggleCategory = (categoryGuid: string) => {
        const newExpanded = new Set(expandedCategories);
        if (newExpanded.has(categoryGuid)) {
            newExpanded.delete(categoryGuid);
        } else {
            newExpanded.add(categoryGuid);
        }
        setExpandedCategories(newExpanded);
    };

    const handleItemSelect = (item: GlossaryTerm | GlossaryCategory) => {
        setSelectedItem(item);
        setGeneratedReadme(item.readme || '');
    };

    const generateReadme = async () => {
        if (!selectedItem) return;

        setIsGenerating(true);
        try {
            const response = await fetch('/api/generate_glossary_readme', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    item_guid: selectedItem.guid,
                    item_name: selectedItem.name,
                    item_type: selectedItem.type,
                    current_description: selectedItem.description || ''
                })
            });

            const data = await response.json();
            if (data.success) {
                setGeneratedReadme(data.readme);
                toast.success('README generated successfully');
            } else {
                toast.error(data.error || 'Failed to generate README');
            }
        } catch (error) {
            toast.error('Error generating README');
            console.error('Error:', error);
        } finally {
            setIsGenerating(false);
        }
    };

    const saveReadme = async () => {
        if (!selectedItem || !generatedReadme.trim()) return;

        setIsSaving(true);
        try {
            const response = await fetch('/api/save_glossary_readme', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    item_guid: selectedItem.guid,
                    item_type: selectedItem.type,
                    readme: generatedReadme
                })
            });

            const data = await response.json();
            if (data.success) {
                toast.success('README saved successfully');
                // Update the local state
                if (selectedItem.type === 'category') {
                    setCategories(prev => prev.map(cat => 
                        cat.guid === selectedItem.guid 
                            ? { ...cat, readme: generatedReadme }
                            : cat
                    ));
                } else {
                    setTerms(prev => prev.map(term => 
                        term.guid === selectedItem.guid 
                            ? { ...term, readme: generatedReadme }
                            : term
                    ));
                }
                setSelectedItem(prev => prev ? { ...prev, readme: generatedReadme } : null);
            } else {
                toast.error(data.error || 'Failed to save README');
            }
        } catch (error) {
            toast.error('Error saving README');
            console.error('Error:', error);
        } finally {
            setIsSaving(false);
        }
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-center">
                    <LoadingSpinner size="lg" />
                    <p className="mt-2 text-sm text-gray-500">Loading business glossary...</p>
                </div>
            </div>
        );
    }

    return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 h-full">
            {/* Left Panel - Glossary Tree */}
            <div className="bg-white rounded-lg shadow border border-gray-200">
                <div className="p-4 border-b border-gray-200">
                    <h3 className="text-lg font-medium text-gray-900 flex items-center">
                        <BookOpenIcon className="w-5 h-5 mr-2" />
                        Business Glossary
                    </h3>
                    <p className="text-sm text-gray-500 mt-1">
                        Select a category or term to manage its README
                    </p>
                </div>
                
                <div className="p-4 max-h-96 overflow-y-auto">
                    {categories.length === 0 && terms.length === 0 ? (
                        <div className="text-center text-gray-500 py-8">
                            <BookOpenIcon className="mx-auto h-12 w-12 text-gray-400" />
                            <h3 className="mt-2 text-sm font-medium text-gray-900">No glossary items found</h3>
                            <p className="mt-1 text-sm text-gray-500">
                                Create business glossary categories and terms in Atlan first.
                            </p>
                            <p className="mt-2 text-xs text-gray-400">
                                Once created, they will appear here for README management.
                            </p>
                        </div>
                    ) : (
                        <div className="space-y-2">
                            {/* Categories */}
                            {categories.map((category) => (
                                <div key={category.guid}>
                                    <div
                                        className={`flex items-center p-2 rounded-md cursor-pointer hover:bg-gray-50 ${
                                            selectedItem?.guid === category.guid ? 'bg-blue-50 border border-blue-200' : ''
                                        }`}
                                        onClick={() => handleItemSelect(category)}
                                    >
                                        <button
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                toggleCategory(category.guid);
                                            }}
                                            className="mr-2 p-1 hover:bg-gray-200 rounded"
                                        >
                                            {expandedCategories.has(category.guid) ? (
                                                <ChevronDownIcon className="w-4 h-4" />
                                            ) : (
                                                <ChevronRightIcon className="w-4 h-4" />
                                            )}
                                        </button>
                                        <TagIcon className="w-4 h-4 mr-2 text-purple-500" />
                                        <div className="flex-1">
                                            <div className="font-medium text-sm">{category.name}</div>
                                            {category.glossary_name && (
                                                <div className="text-xs text-purple-600 font-medium">
                                                    {category.glossary_name}
                                                </div>
                                            )}
                                            {category.description && (
                                                <div className="text-xs text-gray-500 truncate">
                                                    {category.description}
                                                </div>
                                            )}
                                        </div>
                                        {category.readme && (
                                            <div className="w-2 h-2 bg-green-400 rounded-full ml-2" title="Has README" />
                                        )}
                                    </div>
                                    
                                    {/* Category Terms */}
                                    {expandedCategories.has(category.guid) && category.terms && (
                                        <div className="ml-8 mt-2 space-y-1">
                                            {category.terms.map((term) => (
                                                <div
                                                    key={term.guid}
                                                    className={`flex items-center p-2 rounded-md cursor-pointer hover:bg-gray-50 ${
                                                        selectedItem?.guid === term.guid ? 'bg-blue-50 border border-blue-200' : ''
                                                    }`}
                                                    onClick={() => handleItemSelect(term)}
                                                >
                                                    <BookOpenIcon className="w-4 h-4 mr-2 text-blue-500" />
                                                    <div className="flex-1">
                                                        <div className="font-medium text-sm">{term.name}</div>
                                                        {term.description && (
                                                            <div className="text-xs text-gray-500 truncate">
                                                                {term.description}
                                                            </div>
                                                        )}
                                                    </div>
                                                    {term.readme && (
                                                        <div className="w-2 h-2 bg-green-400 rounded-full ml-2" title="Has README" />
                                                    )}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))}
                            
                            {/* Standalone Terms */}
                            {terms.filter(term => !term.category).map((term) => (
                                <div
                                    key={term.guid}
                                    className={`flex items-center p-2 rounded-md cursor-pointer hover:bg-gray-50 ${
                                        selectedItem?.guid === term.guid ? 'bg-blue-50 border border-blue-200' : ''
                                    }`}
                                    onClick={() => handleItemSelect(term)}
                                >
                                    <BookOpenIcon className="w-4 h-4 mr-2 text-blue-500" />
                                    <div className="flex-1">
                                        <div className="font-medium text-sm">{term.name}</div>
                                        {term.glossary_name && (
                                            <div className="text-xs text-blue-600 font-medium">
                                                {term.glossary_name}
                                            </div>
                                        )}
                                        {term.description && (
                                            <div className="text-xs text-gray-500 truncate">
                                                {term.description}
                                            </div>
                                        )}
                                    </div>
                                    {term.readme && (
                                        <div className="w-2 h-2 bg-green-400 rounded-full ml-2" title="Has README" />
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* Right Panel - README Editor */}
            <div className="bg-white rounded-lg shadow border border-gray-200">
                {selectedItem ? (
                    <>
                        <div className="p-4 border-b border-gray-200">
                            <div className="flex items-center justify-between">
                                <div>
                                    <h3 className="text-lg font-medium text-gray-900 flex items-center">
                                        {selectedItem.type === 'category' ? (
                                            <TagIcon className="w-5 h-5 mr-2 text-purple-500" />
                                        ) : (
                                            <BookOpenIcon className="w-5 h-5 mr-2 text-blue-500" />
                                        )}
                                        {selectedItem.name}
                                    </h3>
                                    <p className="text-sm text-gray-500 mt-1">
                                        {selectedItem.type === 'category' ? 'Category' : 'Term'} README
                                    </p>
                                </div>
                                <div className="flex space-x-2">
                                    <button
                                        onClick={generateReadme}
                                        disabled={isGenerating || isSaving}
                                        className="inline-flex items-center px-3 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-atlan-blue hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-atlan-blue disabled:opacity-50 disabled:cursor-not-allowed"
                                    >
                                        <SparklesIcon className="w-4 h-4 mr-1" />
                                        {isGenerating ? 'Generating...' : 'Generate'}
                                    </button>
                                    {generatedReadme && generatedReadme !== selectedItem.readme && (
                                        <button
                                            onClick={saveReadme}
                                            disabled={isSaving || isGenerating}
                                            className="inline-flex items-center px-3 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
                                        >
                                            <CheckCircleIcon className="w-4 h-4 mr-1" />
                                            {isSaving ? 'Saving...' : 'Save'}
                                        </button>
                                    )}
                                </div>
                            </div>
                        </div>
                        
                        <div className="p-4">
                            {selectedItem.description && (
                                <div className="mb-4 p-3 bg-gray-50 rounded-md">
                                    <h4 className="text-sm font-medium text-gray-700 mb-1">Description</h4>
                                    <p className="text-sm text-gray-600">{selectedItem.description}</p>
                                </div>
                            )}
                            
                            <div>
                                <label className="block text-sm font-medium text-gray-700 mb-2">
                                    README Content
                                </label>
                                <textarea
                                    value={generatedReadme}
                                    onChange={(e) => setGeneratedReadme(e.target.value)}
                                    placeholder="Generate or write README content for this glossary item..."
                                    className="w-full h-64 p-3 border border-gray-300 rounded-md focus:ring-atlan-blue focus:border-atlan-blue resize-none"
                                />
                            </div>
                        </div>
                    </>
                ) : (
                    <div className="flex items-center justify-center h-full">
                        <div className="text-center">
                            <BookOpenIcon className="mx-auto h-12 w-12 text-gray-400" />
                            <h3 className="mt-2 text-sm font-medium text-gray-900">No item selected</h3>
                            <p className="mt-1 text-sm text-gray-500">
                                Select a category or term from the left panel to manage its README.
                            </p>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
};