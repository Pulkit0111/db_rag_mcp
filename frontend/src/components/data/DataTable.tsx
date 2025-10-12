import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import { useState, useMemo } from 'react';
import { Box, Table, Input, HStack, Button, Text, NativeSelectRoot, NativeSelectField } from '@chakra-ui/react';
import { FiChevronUp, FiChevronDown } from 'react-icons/fi';

interface DataTableProps {
  data: any[];
  maxHeight?: string;
  colorMode?: 'light' | 'dark';
}

export const DataTable: React.FC<DataTableProps> = ({ data, maxHeight = '500px', colorMode = 'light' }) => {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = useState('');
  const [pageSize, setPageSize] = useState(10);

  const theme = {
    bg: colorMode === 'dark' ? 'gray.900' : 'white',
    headerBg: colorMode === 'dark' ? 'gray.800' : 'gray.50',
    border: colorMode === 'dark' ? 'gray.700' : 'gray.200',
    hoverBg: colorMode === 'dark' ? 'gray.800' : 'gray.50',
    text: colorMode === 'dark' ? 'white' : 'gray.900',
    cellText: colorMode === 'dark' ? 'gray.100' : 'gray.800',
    inputBg: colorMode === 'dark' ? 'gray.800' : 'white',
    inputBorder: colorMode === 'dark' ? 'gray.600' : 'gray.300',
    inputText: colorMode === 'dark' ? 'white' : 'gray.900',
  };

  // Auto-generate columns from data
  const columns = useMemo<ColumnDef<any>[]>(() => {
    if (!data || data.length === 0) return [];
    
    return Object.keys(data[0]).map(key => ({
      accessorKey: key,
      header: key,
      cell: info => {
        const value = info.getValue();
        const stringValue = String(value ?? '');
        // Truncate very long values
        return stringValue.length > 100 ? stringValue.substring(0, 100) + '...' : stringValue;
      },
    }));
  }, [data]);

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      globalFilter,
    },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: {
      pagination: { pageSize },
    },
  });

  if (!data || data.length === 0) {
    return (
      <Box p={4} textAlign="center" color="gray.500">
        No data to display
      </Box>
    );
  }

  return (
    <Box>
      {/* Search */}
      <HStack mb={4} justify="space-between">
        <Input
          placeholder="Search all columns..."
          value={globalFilter}
          onChange={e => setGlobalFilter(e.target.value)}
          maxW="300px"
          size="sm"
          bg={theme.inputBg}
          borderColor={theme.inputBorder}
          color={theme.inputText}
          _placeholder={{ color: colorMode === 'dark' ? 'gray.500' : 'gray.400' }}
        />
        <HStack>
          <NativeSelectRoot size="sm" width="120px">
            <NativeSelectField
              value={pageSize}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                setPageSize(Number(e.target.value));
                table.setPageSize(Number(e.target.value));
              }}
              bg={theme.inputBg}
              borderColor={theme.inputBorder}
              color={theme.inputText}
            >
              <option value={10}>10 rows</option>
              <option value={25}>25 rows</option>
              <option value={50}>50 rows</option>
              <option value={100}>100 rows</option>
            </NativeSelectField>
          </NativeSelectRoot>
        </HStack>
      </HStack>

      {/* Table */}
      <Box overflowX="auto" overflowY="auto" maxH={maxHeight} border="1px" borderColor={theme.border} borderRadius="md" bg={theme.bg} w="100%">
        <Table.Root size="sm" bg={theme.bg} style={{ backgroundColor: colorMode === 'dark' ? '#171923' : '#FFFFFF', minWidth: '100%', width: 'max-content' }}>
          <Table.Header position="sticky" top={0} bg={theme.headerBg} zIndex={1} borderBottom="1px" borderColor={theme.border} style={{ backgroundColor: colorMode === 'dark' ? '#1A202C' : '#F7FAFC' }}>
            {table.getHeaderGroups().map(headerGroup => (
              <Table.Row key={headerGroup.id} style={{ backgroundColor: colorMode === 'dark' ? '#1A202C' : '#F7FAFC' }}>
                {headerGroup.headers.map(header => (
                  <Table.ColumnHeader
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    cursor="pointer"
                    userSelect="none"
                    color={theme.text}
                    fontWeight="semibold"
                    px={3}
                    py={2}
                    style={{ backgroundColor: colorMode === 'dark' ? '#1A202C' : '#F7FAFC' }}
                  >
                    <HStack gap={2}>
                      <span>{flexRender(header.column.columnDef.header, header.getContext())}</span>
                      {header.column.getIsSorted() && (
                        <Box color={theme.text}>
                          {header.column.getIsSorted() === 'asc' ? <FiChevronUp /> : <FiChevronDown />}
                        </Box>
                      )}
                    </HStack>
                  </Table.ColumnHeader>
                ))}
              </Table.Row>
            ))}
          </Table.Header>
          <Table.Body bg={theme.bg} style={{ backgroundColor: colorMode === 'dark' ? '#171923' : '#FFFFFF' }}>
            {table.getRowModel().rows.map(row => (
              <Table.Row 
                key={row.id} 
                _hover={{ bg: theme.hoverBg }}
                borderBottom="1px"
                borderColor={theme.border}
                style={{ backgroundColor: colorMode === 'dark' ? '#171923' : '#FFFFFF' }}
              >
                {row.getVisibleCells().map(cell => (
                  <Table.Cell 
                    key={cell.id}
                    color={theme.cellText}
                    px={3}
                    py={2}
                    whiteSpace="nowrap"
                    maxW="300px"
                    overflow="hidden"
                    textOverflow="ellipsis"
                    style={{ backgroundColor: colorMode === 'dark' ? '#171923' : '#FFFFFF' }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </Table.Cell>
                ))}
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      </Box>

      {/* Pagination */}
      <HStack mt={4} justify="space-between" flexWrap="wrap" gap={2}>
        <Text fontSize="sm" color={theme.text}>
          Showing {table.getRowModel().rows.length} of {data.length} rows
        </Text>
        <HStack>
          <Button
            size="sm"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
            variant="outline"
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            borderColor={colorMode === 'dark' ? 'gray.600' : 'gray.300'}
            _hover={{
              bg: colorMode === 'dark' ? 'gray.700' : 'gray.100',
              borderColor: colorMode === 'dark' ? 'gray.500' : 'gray.400'
            }}
          >
            Previous
          </Button>
          <Text fontSize="sm" color={theme.text}>
            Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()}
          </Text>
          <Button
            size="sm"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
            variant="outline"
            color={colorMode === 'dark' ? 'white' : 'gray.800'}
            borderColor={colorMode === 'dark' ? 'gray.600' : 'gray.300'}
            _hover={{
              bg: colorMode === 'dark' ? 'gray.700' : 'gray.100',
              borderColor: colorMode === 'dark' ? 'gray.500' : 'gray.400'
            }}
          >
            Next
          </Button>
        </HStack>
      </HStack>
    </Box>
  );
};
