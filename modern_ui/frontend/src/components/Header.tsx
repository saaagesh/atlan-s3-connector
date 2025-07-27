// src/components/Header.tsx
import React from 'react';

export const Header: React.FC = () => {
  return (
    <header className="bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-700 shadow-lg z-10">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center py-4">
          <div className="flex items-center space-x-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-2 border border-white/20">
              <img 
                src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/Atlan-logo-full.svg/2560px-Atlan-logo-full.svg.png" 
                alt="Atlan" 
                className="h-8 w-auto filter brightness-0 invert" 
              />
            </div>
            <div>
              <h1 className="text-xl font-bold text-white">Metadata Manager</h1>
              <p className="text-sm text-blue-100 font-medium">✨ Powered by AI</p>
            </div>
          </div>
          
          <div className="hidden md:flex items-center space-x-2">
            <div className="bg-white/10 backdrop-blur-sm rounded-full px-3 py-1 border border-white/20">
              <span className="text-xs font-medium text-white">🚀 Enhanced</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};
