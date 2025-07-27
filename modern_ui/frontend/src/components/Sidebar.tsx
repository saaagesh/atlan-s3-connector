import { Fragment } from 'react';
import { Listbox, Transition } from '@headlessui/react';
import { 
  ChevronUpDownIcon, 
  CheckIcon,
  CircleStackIcon,
  CloudIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';
import { useAssetsBySource } from '../hooks/useApi';
import { Asset, Source } from '../types';
import { LoadingSpinner } from './LoadingSpinner';

const sources: Source[] = [
  { id: 'postgres-sk', name: 'PostgreSQL', type: 'postgres', icon: 'database' },
  { id: 'aws-s3-connection-sk', name: 'AWS S3', type: 's3', icon: 'cloud' },
  { id: 'snowflake-sk', name: 'Snowflake', type: 'snowflake', icon: 'database' },
];

const getSourceIcon = (type: string) => {
  switch (type) {
    case 's3':
      return <CloudIcon className="w-5 h-5 text-atlan-gray" />;
    case 'postgres':
    case 'snowflake':
      return <CircleStackIcon className="w-5 h-5 text-atlan-gray" />;
    default:
      return <TableCellsIcon className="w-5 h-5 text-atlan-gray" />;
  }
};

interface SidebarProps {
  activeTab: 'columns' | 'glossary';
  selectedSource: string;
  selectedAsset: Asset | null;
  onSourceChange: (source: string) => void;
  onAssetChange: (asset: Asset | null) => void;
}

export const Sidebar = ({ 
  activeTab,
  selectedSource, 
  selectedAsset, 
  onSourceChange, 
  onAssetChange 
}: SidebarProps) => {
  const { data: assets, isLoading: assetsLoading } = useAssetsBySource(selectedSource);

  const selectedSourceObj = sources.find(s => s.id === selectedSource);

  // Debug log to see what activeTab we're receiving
  console.log('Sidebar activeTab:', activeTab);

  if (activeTab === 'glossary') {
    return (
      <div className="w-80 bg-purple-50 border-r border-purple-200 flex flex-col">
        <div className="p-6 border-b border-purple-200 bg-purple-100">
          <h2 className="text-lg font-semibold text-purple-900 mb-4">🔖 Business Glossary</h2>
          <p className="text-sm text-purple-700">
            Select a category or term from the main panel to manage its README content.
          </p>
        </div>
        
        <div className="p-6">
          <div className="bg-purple-100 rounded-lg p-4 border border-purple-200">
            <h3 className="text-sm font-medium text-purple-900 mb-2">📚 How to use</h3>
            <ul className="text-xs text-purple-800 space-y-1">
              <li>• Browse categories and terms in the main panel</li>
              <li>• Click on any item to select it</li>
              <li>• Generate AI-powered README content</li>
              <li>• Edit and save to Atlan</li>
            </ul>
          </div>
          
          <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
            <p className="text-xs text-yellow-800">
              <strong>Note:</strong> This section is independent of data sources. 
              All business glossary items are managed here.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="w-80 bg-blue-50 border-r border-blue-200 flex flex-col">
      <div className="p-6 border-b border-blue-200 bg-blue-100">
        <h2 className="text-lg font-semibold text-blue-900 mb-4">🗃️ Data Sources</h2>
        <p className="text-sm text-blue-700 mb-4">
          Select a data source and asset to manage table and column descriptions.
        </p>
        
        {/* Source Selection */}
        <div className="mb-6">
          <label className="block text-sm font-medium text-blue-800 mb-2">
            1. Select Source
          </label>
          <Listbox value={selectedSource} onChange={onSourceChange}>
            <div className="relative">
              <Listbox.Button className="relative w-full cursor-pointer rounded-lg bg-white py-3 pl-3 pr-10 text-left border border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent shadow-sm">
                <span className="flex items-center">
                  {selectedSourceObj ? (
                    <>
                      <span className="text-blue-600 mr-3">
                        {getSourceIcon(selectedSourceObj.type)}
                      </span>
                      <span className="block truncate text-blue-900">{selectedSourceObj.name}</span>
                    </>
                  ) : (
                    <span className="block truncate text-blue-400">Choose a source...</span>
                  )}
                </span>
                <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
                  <ChevronUpDownIcon className="h-5 w-5 text-blue-400" />
                </span>
              </Listbox.Button>
              <Transition
                as={Fragment}
                leave="transition ease-in duration-100"
                leaveFrom="opacity-100"
                leaveTo="opacity-0"
              >
                <Listbox.Options className="absolute z-10 mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-blue-200 ring-opacity-5 focus:outline-none">
                  {sources.map((source) => (
                    <Listbox.Option
                      key={source.id}
                      className={({ active }) =>
                        `relative cursor-pointer select-none py-2 pl-3 pr-9 ${
                          active ? 'bg-blue-50 text-blue-700' : 'text-gray-900'
                        }`
                      }
                      value={source.id}
                    >
                      {({ selected }) => (
                        <>
                          <div className="flex items-center">
                            <span className="text-blue-500 mr-3">
                              {getSourceIcon(source.type)}
                            </span>
                            <span className={`block truncate ${selected ? 'font-medium' : 'font-normal'}`}>
                              {source.name}
                            </span>
                          </div>
                          {selected && (
                            <span className="absolute inset-y-0 right-0 flex items-center pr-4 text-blue-600">
                              <CheckIcon className="h-5 w-5" />
                            </span>
                          )}
                        </>
                      )}
                    </Listbox.Option>
                  ))}
                </Listbox.Options>
              </Transition>
            </div>
          </Listbox>
        </div>

        {/* Asset Selection */}
        {selectedSource && (
          <div>
            <label className="block text-sm font-medium text-blue-800 mb-2">
              2. Select Asset/Table
            </label>
            {assetsLoading ? (
              <div className="flex items-center justify-center py-4">
                <LoadingSpinner size="sm" />
              </div>
            ) : (
              <Listbox value={selectedAsset} onChange={onAssetChange}>
                <div className="relative">
                  <Listbox.Button className="relative w-full cursor-pointer rounded-lg bg-white py-3 pl-3 pr-10 text-left border border-blue-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent shadow-sm">
                    <span className="block truncate text-blue-900">
                      {selectedAsset ? selectedAsset.name : 'Select asset/table...'}
                    </span>
                    <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
                      <ChevronUpDownIcon className="h-5 w-5 text-blue-400" />
                    </span>
                  </Listbox.Button>
                  <Transition
                    as={Fragment}
                    leave="transition ease-in duration-100"
                    leaveFrom="opacity-100"
                    leaveTo="opacity-0"
                  >
                    <Listbox.Options className="absolute z-10 mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-blue-200 ring-opacity-5 focus:outline-none">
                      {assets?.map((asset, index) => (
                        <Listbox.Option
                          key={index}
                          className={({ active }) =>
                            `relative cursor-pointer select-none py-2 pl-3 pr-9 ${
                              active ? 'bg-blue-50 text-blue-700' : 'text-gray-900'
                            }`
                          }
                          value={asset}
                        >
                          {({ selected }) => (
                            <>
                              <span className={`block truncate ${selected ? 'font-medium' : 'font-normal'}`}>
                                {asset.name}
                              </span>
                              {selected && (
                                <span className="absolute inset-y-0 right-0 flex items-center pr-4 text-blue-600">
                                  <CheckIcon className="h-5 w-5" />
                                </span>
                              )}
                            </>
                          )}
                        </Listbox.Option>
                      ))}
                    </Listbox.Options>
                  </Transition>
                </div>
              </Listbox>
            )}
          </div>
        )}
      </div>

      {/* Selected Asset Info */}
      {selectedAsset && (
        <div className="p-6">
          <h3 className="text-sm font-medium text-blue-900 mb-2">📊 Selected Asset</h3>
          <div className="bg-blue-100 rounded-lg p-3 border border-blue-200">
            <p className="text-sm font-medium text-blue-900">{selectedAsset.name}</p>
            <p className="text-xs text-blue-700 mt-1">Type: {selectedAsset.type.toUpperCase()}</p>
          </div>
          
          <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-lg">
            <p className="text-xs text-green-800">
              <strong>✅ Ready:</strong> You can now generate and manage column descriptions for this asset.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};