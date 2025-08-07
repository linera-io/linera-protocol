import React, { useState } from 'react';
import { Search } from 'lucide-react';

interface SearchBarProps {
  onSearch: (query: string) => void;
  placeholder?: string;
}

export const SearchBar: React.FC<SearchBarProps> = ({ 
  onSearch, 
  placeholder = "Search by block hash..." 
}) => {
  const [query, setQuery] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      onSearch(query.trim());
    }
  };

  return (
    <form onSubmit={handleSubmit} className="relative group">
      <div className="relative">
        <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-linera-gray-medium w-5 h-5 group-focus-within:text-linera-red transition-colors" />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={placeholder}
          className="input-field pl-12 pr-4 py-4 text-base"
        />
        <div className="absolute inset-0 rounded-lg bg-gradient-to-r from-linera-red/20 to-blue-500/20 opacity-0 group-focus-within:opacity-100 transition-opacity -z-10 blur"></div>
      </div>
    </form>
  );
};