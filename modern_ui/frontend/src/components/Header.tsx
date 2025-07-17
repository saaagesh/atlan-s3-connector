import { SparklesIcon } from '@heroicons/react/24/outline';

export const Header = () => {
  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4">
      <div className="flex items-center space-x-4">
        <div className="flex items-center space-x-3">
          <img 
            src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/Atlan-logo-full.svg/2560px-Atlan-logo-full.svg.png"
            alt="Atlan Logo"
            className="h-8 w-auto"
          />
          <div className="flex items-center justify-center w-8 h-8 bg-gradient-to-br from-blue-500 to-purple-600 rounded-lg">
            <SparklesIcon className="w-5 h-5 text-white" />
          </div>
        </div>
        <div>
          <h1 className="text-xl font-semibold text-gray-900">
            Metadata Manager
          </h1>
          <p className="text-sm text-gray-500">
            AI-powered metadata enhancement and management
          </p>
        </div>
      </div>
    </header>
  );
};