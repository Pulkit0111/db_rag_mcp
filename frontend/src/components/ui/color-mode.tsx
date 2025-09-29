import { createContext, useContext, useState, useEffect } from 'react';

type ColorMode = 'light' | 'dark';

interface ColorModeContextType {
  colorMode: ColorMode;
  toggleColorMode: () => void;
  setColorMode: (mode: ColorMode) => void;
}

const ColorModeContext = createContext<ColorModeContextType | undefined>(undefined);

interface ColorModeProviderProps {
  children: React.ReactNode;
  defaultValue?: ColorMode;
}

export function ColorModeProvider({ children, defaultValue = 'dark' }: ColorModeProviderProps) {
  const [colorMode, setColorMode] = useState<ColorMode>(() => {
    // Check localStorage for saved preference
    const saved = localStorage.getItem('chakra-ui-color-mode');
    return (saved as ColorMode) || defaultValue;
  });

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', colorMode);
    localStorage.setItem('chakra-ui-color-mode', colorMode);
    
    // Apply theme to body for global styling
    if (colorMode === 'dark') {
      document.body.style.backgroundColor = '#1A202C';
      document.body.style.color = '#F7FAFC';
    } else {
      document.body.style.backgroundColor = '#FFFFFF';
      document.body.style.color = '#1A202C';
    }
  }, [colorMode]);

  const toggleColorMode = () => {
    setColorMode(prev => prev === 'light' ? 'dark' : 'light');
  };

  return (
    <ColorModeContext.Provider value={{ colorMode, toggleColorMode, setColorMode }}>
      {children}
    </ColorModeContext.Provider>
  );
}

export function useColorMode() {
  const context = useContext(ColorModeContext);
  if (!context) {
    throw new Error('useColorMode must be used within ColorModeProvider');
  }
  return context;
}

// Color mode button component
export function ColorModeButton() {
  const { colorMode, toggleColorMode } = useColorMode();
  
  return (
    <button
      onClick={toggleColorMode}
      style={{
        background: 'none',
        border: '1px solid',
        borderColor: colorMode === 'dark' ? '#4A5568' : '#E2E8F0',
        borderRadius: '6px',
        padding: '8px 12px',
        cursor: 'pointer',
        color: colorMode === 'dark' ? '#F7FAFC' : '#1A202C',
        fontSize: '14px',
        fontWeight: '500',
        transition: 'all 0.2s',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = colorMode === 'dark' ? '#2D3748' : '#F7FAFC';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      {colorMode === 'dark' ? '☀️ Light' : '🌙 Dark'}
    </button>
  );
}
