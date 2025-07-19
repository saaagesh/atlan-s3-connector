// src/components/Header.tsx
import React from 'react';

export const Header: React.FC = () => {
  return (
    <header className="bg-white shadow-sm z-10">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center py-3">
          <div className="flex items-center space-x-4">
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/Atlan-logo-full.svg/2560px-Atlan-logo-full.svg.png" alt="Atlan" className="h-8 w-auto" />
            <h1 className="text-xl font-semibold text-atlan-dark">Metadata Manager - Powered by AI</h1>
          </div>
        </div>
      </div>
    </header>
  );
};
