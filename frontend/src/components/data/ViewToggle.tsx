import { Group } from '@chakra-ui/react';
import { IconButton } from '@chakra-ui/react';
import { FiTable, FiBarChart2 } from 'react-icons/fi';

interface ViewToggleProps {
  view: 'table' | 'chart';
  onChange: (view: 'table' | 'chart') => void;
  colorMode?: 'light' | 'dark';
}

export const ViewToggle: React.FC<ViewToggleProps> = ({ view, onChange, colorMode = 'light' }) => {
  return (
    <Group attached>
      <IconButton
        aria-label="Table view"
        onClick={() => onChange('table')}
        colorPalette={view === 'table' ? 'blue' : 'gray'}
        variant={view === 'table' ? 'solid' : 'outline'}
        size="sm"
        color={view === 'table' ? 'white' : (colorMode === 'dark' ? 'white' : 'gray.800')}
        borderColor={colorMode === 'dark' ? 'gray.600' : 'gray.300'}
        _hover={{
          borderColor: colorMode === 'dark' ? 'gray.500' : 'gray.400'
        }}
      >
        <FiTable />
      </IconButton>
      <IconButton
        aria-label="Chart view"
        onClick={() => onChange('chart')}
        colorPalette={view === 'chart' ? 'blue' : 'gray'}
        variant={view === 'chart' ? 'solid' : 'outline'}
        size="sm"
        color={view === 'chart' ? 'white' : (colorMode === 'dark' ? 'white' : 'gray.800')}
        borderColor={colorMode === 'dark' ? 'gray.600' : 'gray.300'}
        _hover={{
          borderColor: colorMode === 'dark' ? 'gray.500' : 'gray.400'
        }}
      >
        <FiBarChart2 />
      </IconButton>
    </Group>
  );
};

