import { ChakraProvider, defaultSystem } from '@chakra-ui/react';
import { ColorModeProvider } from './color-mode';

interface ProviderProps {
  children: React.ReactNode;
}

export function Provider({ children }: ProviderProps) {
  return (
    <ChakraProvider value={defaultSystem}>
      <ColorModeProvider defaultValue="dark">
        {children}
      </ColorModeProvider>
    </ChakraProvider>
  );
}
