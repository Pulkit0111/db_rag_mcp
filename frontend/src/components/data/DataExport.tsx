import { MenuRoot, MenuTrigger, MenuContent, MenuItem } from '@chakra-ui/react';
import { Button, Box } from '@chakra-ui/react';
import { FiDownload, FiCopy } from 'react-icons/fi';
import { DataExporter } from '../../utils/export';
import { useToast } from '../../hooks/useToast';

interface DataExportProps {
  data: any[];
  queryText?: string;
  colorMode?: 'light' | 'dark';
}

export const DataExport: React.FC<DataExportProps> = ({ data, queryText = '', colorMode = 'light' }) => {
  const toast = useToast();

  const handleExportCSV = () => {
    const filename = DataExporter.generateFilename(queryText, 'csv');
    DataExporter.toCSV(data, filename);
    toast.success({ title: 'Exported to CSV', description: filename });
  };

  const handleExportJSON = () => {
    const filename = DataExporter.generateFilename(queryText, 'json');
    DataExporter.toJSON(data, filename);
    toast.success({ title: 'Exported to JSON', description: filename });
  };

  const handleCopyMarkdown = async () => {
    const markdown = DataExporter.toMarkdown(data);
    await DataExporter.copyToClipboard(markdown);
    toast.success({ title: 'Copied as Markdown table' });
  };

  return (
    <Box position="relative" display="inline-block">
      <MenuRoot positioning={{ placement: "bottom-end", strategy: "absolute" }}>
        <MenuTrigger asChild>
          <Button 
            size="sm" 
            variant="outline" 
            width="100px" 
            flexShrink={0}
            bg={colorMode === 'dark' ? 'gray.800' : 'white'}
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            borderColor={colorMode === 'dark' ? 'gray.600' : 'gray.300'}
            _hover={{
              bg: colorMode === 'dark' ? 'gray.700' : 'gray.100',
              borderColor: colorMode === 'dark' ? 'gray.500' : 'gray.400'
            }}
            _active={{
              bg: colorMode === 'dark' ? 'gray.700' : 'gray.100',
            }}
          >
            <FiDownload style={{ marginRight: '4px' }} />
            Export
          </Button>
        </MenuTrigger>
        <MenuContent 
          minW="180px"
          bg={colorMode === 'dark' ? 'gray.800' : 'white'}
          borderColor={colorMode === 'dark' ? 'gray.700' : 'gray.200'}
          border="1px"
          shadow="lg"
          zIndex={1000}
        >
          <MenuItem 
            value="csv" 
            onClick={handleExportCSV}
            bg="transparent"
            _hover={{ bg: colorMode === 'dark' ? 'gray.700' : 'gray.100' }}
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            cursor="pointer"
          >
            <FiDownload style={{ marginRight: '8px' }} />
            Export as CSV
          </MenuItem>
          <MenuItem 
            value="json" 
            onClick={handleExportJSON}
            bg="transparent"
            _hover={{ bg: colorMode === 'dark' ? 'gray.700' : 'gray.100' }}
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            cursor="pointer"
          >
            <FiDownload style={{ marginRight: '8px' }} />
            Export as JSON
          </MenuItem>
          <MenuItem 
            value="markdown" 
            onClick={handleCopyMarkdown}
            bg="transparent"
            _hover={{ bg: colorMode === 'dark' ? 'gray.700' : 'gray.100' }}
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            cursor="pointer"
          >
            <FiCopy style={{ marginRight: '8px' }} />
            Copy as Markdown
          </MenuItem>
        </MenuContent>
      </MenuRoot>
    </Box>
  );
};

