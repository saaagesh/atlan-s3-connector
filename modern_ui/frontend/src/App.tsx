// src/App.tsx
import { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import { Sidebar } from './components/Sidebar';
import { MainContent } from './components/MainContent';
import { Header } from './components/Header';
import { Asset } from './types';

const queryClient = new QueryClient();

function App() {
  const [selectedSource, setSelectedSource] = useState<string>('');
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);

  const handleSourceChange = (source: string) => {
    setSelectedSource(source);
    setSelectedAsset(null); // Reset asset when source changes
  };

  const handleAssetChange = (asset: Asset | null) => {
    setSelectedAsset(asset);
  };

  return (
    <QueryClientProvider client={queryClient}>
      <Toaster position="top-right" />
      <div className="min-h-screen bg-gray-50">
        <Header />
        <div className="flex h-[calc(100vh-4rem)]">
          <Sidebar
            selectedSource={selectedSource}
            selectedAsset={selectedAsset}
            onSourceChange={handleSourceChange}
            onAssetChange={handleAssetChange}
          />
          <MainContent selectedAsset={selectedAsset} />
        </div>
      </div>
    </QueryClientProvider>
  );
}

export default App;
