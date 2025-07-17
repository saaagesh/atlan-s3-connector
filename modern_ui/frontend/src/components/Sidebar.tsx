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
      return <CloudIcon className="w-5 h-5" />;
    case 'postgres':
    case 'snowflake':
      return <CircleStackIcon className="w-5 h-5" />;
    default:
      return <TableCellsIcon className="w-5 h-5" />;
  }
};

interface SidebarProps {
  selectedSource: string;
  selectedAsset: Asset | null;
  onSourceChange: (source: string) => void;
  onAssetChange: (asset: Asset | null) => void;
}

export const Sidebar = ({ 
  selectedSource, 
  selectedAsset, 
  onSourceChange, 
  onAssetChange 
}: SidebarProps) => {
  const { data: assets, isLoading: assetsLoading } = useAssetsBySource(selectedSource);

  const selectedSourceObj = sources.find(s => s.id === selectedSource);

  return (
    <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
      <div className="p-6 border-b border-gray-200">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Data Sources</h2>
        
        {/* Source Selection */}
        <div className="mb-6">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            1. Select Source
          </label>
          <Listbox value={selectedSource} onChange={onSourceChange}>
            <div className="relative">
              <Listbox.Button className="relative w-full cursor-pointer rounded-lg bg-white py-3 pl-3 pr-10 text-left border border-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent">
                <span className="flex items-center">
                  {selectedSourceObj ? (
                    <>
                      <span className="text-gray-500 mr-3">
                        {getSourceIcon(selectedSourceObj.type)}
                      </span>
                      <span className="block truncate">{selectedSourceObj.name}</span>
                    </>
                  ) : (
                    <span className="block truncate text-gray-400">Choose a source...</span>
                  )}
                </span>
                <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
                  <ChevronUpDownIcon className="h-5 w-5 text-gray-400" />
                </span>
              </Listbox.Button>
              <Transition
                as={Fragment}
                leave="transition ease-in duration-100"
                leaveFrom="opacity-100"
                leaveTo="opacity-0"
              >
                <Listbox.Options className="absolute z-10 mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
                  {sources.map((source) => (
                    <Listbox.Option
                      key={source.id}
                      className={({ active }) =>
                        `relative cursor-pointer select-none py-2 pl-3 pr-9 ${
                          active ? 'bg-blue-50 text-blue-900' : 'text-gray-900'
                        }`
                      }
                      value={source.id}
                    >
                      {({ selected }) => (
                        <>
                          <div className="flex items-center">
                            <span className="text-gray-500 mr-3">
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
            <label className="block text-sm font-medium text-gray-700 mb-2">
              2. Select Asset/Table
            </label>
            {assetsLoading ? (
              <div className="flex items-center justify-center py-4">
                <LoadingSpinner size="sm" />
              </div>
            ) : (
              <Listbox value={selectedAsset} onChange={onAssetChange}>
                <div className="relative">
                  <Listbox.Button className="relative w-full cursor-pointer rounded-lg bg-white py-3 pl-3 pr-10 text-left border border-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent">
                    <span className="block truncate">
                      {selectedAsset ? selectedAsset.name : 'Select asset/table...'}
                    </span>
                    <span className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
                      <ChevronUpDownIcon className="h-5 w-5 text-gray-400" />
                    </span>
                  </Listbox.Button>
                  <Transition
                    as={Fragment}
                    leave="transition ease-in duration-100"
                    leaveFrom="opacity-100"
                    leaveTo="opacity-0"
                  >
                    <Listbox.Options className="absolute z-10 mt-1 max-h-60 w-full overflow-auto rounded-md bg-white py-1 text-base shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none">
                      {assets?.map((asset, index) => (
                        <Listbox.Option
                          key={index}
                          className={({ active }) =>
                            `relative cursor-pointer select-none py-2 pl-3 pr-9 ${
                              active ? 'bg-blue-50 text-blue-900' : 'text-gray-900'
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
          <h3 className="text-sm font-medium text-gray-900 mb-2">Selected Asset</h3>
          <div className="bg-gray-50 rounded-lg p-3">
            <p className="text-sm font-medium text-gray-900">{selectedAsset.name}</p>
            <p className="text-xs text-gray-500 mt-1">Type: {selectedAsset.type.toUpperCase()}</p>
          </div>
        </div>
      )}
    </div>
  );
};