import { useState } from 'react';
import App from './App';
import PhrasesApp from './PhrasesApp';
import SavedTranscriptions from './SavedTranscriptions';
import { Mic2, MessageSquare, Database } from 'lucide-react';

function MainApp() {
  const [currentView, setCurrentView] = useState<'words' | 'phrases' | 'saved'>('words');

  return (
    <div>
      <div className="flex items-center space-x-1 bg-transparent border-none p-2">
        <button
          onClick={() => setCurrentView('words')}
          className={`flex items-center gap-2 px-5 py-2 rounded-full font-medium transition-all ${currentView === 'words'
              ? 'bg-blue-600 text-white shadow-lg'
              : 'text-blue-600 hover:bg-blue-50'
            }`}
        >
          <Mic2 className="h-5 w-5" />
          Word-Level
        </button>
        <button
          onClick={() => setCurrentView('phrases')}
          className={`flex items-center gap-2 px-5 py-2 rounded-full font-medium transition-all ${currentView === 'phrases'
              ? 'bg-purple-600 text-white shadow-lg'
              : 'text-purple-600 hover:bg-purple-50'
            }`}
        >
          <MessageSquare className="h-5 w-5" />
          Phrase-Level
        </button>
        <button
          onClick={() => setCurrentView('saved')}
          className={`flex items-center gap-2 px-5 py-2 rounded-full font-medium transition-all ${currentView === 'saved'
              ? 'bg-indigo-600 text-white shadow-lg'
              : 'text-indigo-600 hover:bg-indigo-50'
            }`}
        >
          <Database className="h-5 w-5" />
          Saved
        </button>
      </div>
      
      {/* Content */}
      {currentView === 'words' && <App />}
      {currentView === 'phrases' && <PhrasesApp />}
      {currentView === 'saved' && <SavedTranscriptions />}
    </div>
  );
}

export default MainApp;

