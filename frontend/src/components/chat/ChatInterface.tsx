import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  VStack,
  HStack,
  Input,
  Button,
  Text,
  Flex,
  Badge,
  Textarea,
  Code,
} from '@chakra-ui/react';
import { FiSend, FiDatabase } from 'react-icons/fi';
import { apiService } from '../../services/api';
import { useColorMode, ColorModeButton } from '../ui/color-mode';
import { useToast } from '../../hooks/useToast';
import type { Message } from '../../types/query';
import { DataTable } from '../data/DataTable';
import { DataExport } from '../data/DataExport';
import { ViewToggle } from '../data/ViewToggle';
import { AnimatePresence } from 'framer-motion';
import { MessageBubble } from './MessageBubble';
import { TypingIndicator } from './TypingIndicator';

interface ConnectionStatus {
  connected: boolean;
  database_type?: string;
  host?: string;
  port?: number;
  database?: string;
  message?: string;
}

export const ChatInterface: React.FC = () => {
  const { colorMode } = useColorMode();
  const toast = useToast();
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>({ connected: false });
  const [dbUri, setDbUri] = useState('');
  const [isConnecting, setIsConnecting] = useState(false);
  const [showConnection, setShowConnection] = useState(false);
  const [view, setView] = useState<'table' | 'chart'>('table');
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Theme colors
  const theme = {
    bg: colorMode === 'dark' ? 'gray.900' : 'gray.50',
    cardBg: colorMode === 'dark' ? 'gray.800' : 'white',
    headerBg: colorMode === 'dark' ? 'gray.800' : 'white',
    sidebarBg: colorMode === 'dark' ? 'gray.800' : 'gray.100',
    inputBg: colorMode === 'dark' ? 'gray.700' : 'white',
    text: colorMode === 'dark' ? 'white' : 'gray.800',
    textSecondary: colorMode === 'dark' ? 'gray.300' : 'gray.600',
    textMuted: colorMode === 'dark' ? 'gray.500' : 'gray.400',
    border: colorMode === 'dark' ? 'gray.700' : 'gray.200',
    userBubble: colorMode === 'dark' ? 'blue.600' : 'blue.500',
    aiBubble: colorMode === 'dark' ? 'gray.700' : 'gray.100',
    systemBubble: colorMode === 'dark' ? 'gray.700' : 'blue.50',
    codeBg: colorMode === 'dark' ? 'gray.900' : 'gray.100',
    errorBg: colorMode === 'dark' ? 'red.900' : 'red.50',
    errorBorder: colorMode === 'dark' ? 'red.600' : 'red.200',
    errorText: colorMode === 'dark' ? 'red.200' : 'red.700',
  };

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    checkConnectionStatus();
    // Add welcome message
    setMessages([{
      id: '1',
      type: 'assistant',
      content: 'Welcome to DB Genie! Connect to your database and start asking questions in plain English.',
      timestamp: new Date()
    }]);
  }, []);

  const checkConnectionStatus = async () => {
    try {
      const result = await apiService.getConnectionStatus();
      if (result.success && result.result) {
        setConnectionStatus(result.result);
      }
    } catch (error) {
      console.error('Failed to check connection:', error);
    }
  };

  const handleConnect = async () => {
    if (!dbUri.trim()) {
      toast.warning({
        title: 'Invalid Input',
        description: 'Please enter a valid database connection URI'
      });
      return;
    }

    setIsConnecting(true);
    try {
      const result = await apiService.connectDatabase(dbUri);
      if (result.success) {
        await checkConnectionStatus();
        setShowConnection(false);
        setDbUri('');
        
        toast.success({
          title: 'Connected Successfully',
          description: 'You can now ask questions about your data!'
        });
        
        // Add connection success message
        const newMessage: Message = {
          id: Date.now().toString(),
          type: 'assistant',
          content: `Successfully connected to database. You can now ask questions about your data!`,
          timestamp: new Date()
        };
        setMessages(prev => [...prev, newMessage]);
      } else {
        toast.error({
          title: 'Connection Failed',
          description: result.error || 'Unknown error occurred'
        });
      }
    } catch (error) {
      toast.error({
        title: 'Connection Failed',
        description: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsConnecting(false);
    }
  };

  const handleDisconnect = async () => {
    try {
      const result = await apiService.disconnectDatabase();
      if (result.success) {
        await checkConnectionStatus();
        
        toast.success({
          title: 'Disconnected',
          description: 'Database connection closed successfully'
        });
        
        // Add disconnection message
        const newMessage: Message = {
          id: Date.now().toString(),
          type: 'assistant',
          content: `Database disconnected. Connect to a database to start asking questions.`,
          timestamp: new Date()
        };
        setMessages(prev => [...prev, newMessage]);
      } else {
        toast.error({
          title: 'Disconnection Failed',
          description: result.error || 'Unknown error occurred'
        });
      }
    } catch (error) {
      toast.error({
        title: 'Disconnection Failed',
        description: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    if (!connectionStatus.connected) {
      toast.warning({
        title: 'Not Connected',
        description: 'Please connect to a database first'
      });
      return;
    }

    const userMessage: Message = {
      id: Date.now().toString(),
      type: 'user',
      content: inputValue,
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const result = await apiService.queryData(inputValue);
      
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: result.success ? 
          `Found ${result.result?.results?.length || 0} results` : 
          'Query failed',
        timestamp: new Date(),
        data: result.success ? result.result : null,
        sql: result.result?.generated_sql,
        error: result.success ? undefined : result.error
      };

      setMessages(prev => [...prev, assistantMessage]);

      if (result.success) {
        toast.success({
          title: 'Query Executed',
          description: `Retrieved ${result.result?.results?.length || 0} rows`
        });
      }
    } catch (error) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: 'Sorry, I encountered an error processing your request.',
        timestamp: new Date(),
        error: error instanceof Error ? error.message : 'Unknown error'
      };
      setMessages(prev => [...prev, errorMessage]);
      
      toast.error({
        title: 'Query Failed',
        description: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsLoading(false);
    }
  };

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      toast.success({ title: 'Copied to clipboard' });
    } catch (error) {
      toast.error({ title: 'Failed to copy' });
    }
  };

  const renderMessage = (message: Message) => {
    const isUser = message.type === 'user';

    return (
      <Flex
        key={message.id}
        justify={isUser ? 'flex-end' : 'flex-start'}
        mb={4}
        align="flex-start"
      >
        {!isUser && (
          <Box
            w={10}
            h={10}
            display="flex"
            alignItems="center"
            justifyContent="center"
            mr={3}
            flexShrink={0}
          >
            <Text fontSize="2xl">
              🤖
            </Text>
          </Box>
        )}
        
        <Box maxW="80%" flex="1">
          <MessageBubble 
            message={message} 
            isUser={isUser}
            colorMode={colorMode}
          />
          
          {message.sql && (
            <Box mt={3} p={3} bg={theme.codeBg} borderRadius="md" border="1px" borderColor={theme.border}>
              <HStack justify="space-between" mb={2}>
                <Text fontSize="sm" color={theme.textMuted}>Generated SQL:</Text>
                <Button
                  size="xs"
                  variant="ghost"
                  onClick={() => copyToClipboard(message.sql!)}
                  color={theme.text}
                >
                  Copy
                </Button>
              </HStack>
              <Code display="block" whiteSpace="pre-wrap" fontSize="sm" bg="transparent" color={theme.text}>
                {message.sql}
              </Code>
            </Box>
          )}
          
          {message.data?.results && message.data.results.length > 0 && (
            <Box mt={3} w="100%" maxW="100%">
              <HStack justify="space-between" mb={2} flexWrap="wrap" gap={2}>
                <ViewToggle view={view} onChange={setView} colorMode={colorMode} />
                <DataExport data={message.data.results} queryText={message.content} colorMode={colorMode} />
              </HStack>
              {view === 'table' && (
                <Box overflowX="auto" w="100%">
                  <DataTable data={message.data.results} colorMode={colorMode} />
                </Box>
              )}
            </Box>
          )}
          
          {message.error && (
            <Box mt={3} p={3} bg={theme.errorBg} borderRadius="md" borderLeft="4px" borderColor={theme.errorBorder}>
              <Text color={theme.errorText} fontSize="sm" fontWeight="bold">Error:</Text>
              <Text color={theme.errorText} fontSize="sm">{message.error}</Text>
            </Box>
          )}
          
          <Text fontSize="xs" color={theme.textMuted} mt={1}>
            {message.timestamp.toLocaleTimeString()}
          </Text>
        </Box>
        
        {isUser && (
          <Box
            w={10}
            h={10}
            display="flex"
            alignItems="center"
            justifyContent="center"
            ml={3}
            flexShrink={0}
          >
            <Text fontSize="2xl">👤</Text>
          </Box>
        )}
      </Flex>
    );
  };

  return (
    <Box h="100vh" bg={theme.bg} color={theme.text}>
      {/* Header */}
      <Box bg={theme.headerBg} borderBottom="1px" borderColor={theme.border} px={6} py={4} shadow="sm">
        <Flex justify="space-between" align="center">
          <HStack gap={3}>
            <FiDatabase size={24} color={colorMode === 'dark' ? '#63B3ED' : '#3182CE'} />
            <Text fontSize="xl" fontWeight="bold" color={theme.text}>DB Genie</Text>
          </HStack>
          
          <HStack gap={3}>
            {connectionStatus.connected && (
              <>
                <Badge 
                  colorPalette="green"
                  variant="solid"
                >
                  Connected
                </Badge>
                <Text fontSize="sm" color={theme.textMuted}>
                  {connectionStatus.database_type} • {connectionStatus.database}
                </Text>
              </>
            )}
            <ColorModeButton />
            {connectionStatus.connected ? (
              <>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowConnection(!showConnection)}
                  borderColor={colorMode === 'dark' ? 'gray.500' : 'blue.500'}
                  color={colorMode === 'dark' ? 'gray.200' : 'blue.600'}
                  _hover={{ 
                    bg: colorMode === 'dark' ? 'gray.700' : 'blue.50',
                    borderColor: colorMode === 'dark' ? 'gray.400' : 'blue.600'
                  }}
                >
                  Reconnect
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  colorPalette="red"
                  onClick={handleDisconnect}
                  borderColor={colorMode === 'dark' ? 'red.500' : 'red.500'}
                  color={colorMode === 'dark' ? 'red.300' : 'red.600'}
                  _hover={{ 
                    bg: colorMode === 'dark' ? 'red.900' : 'red.50',
                    borderColor: colorMode === 'dark' ? 'red.400' : 'red.600'
                  }}
                >
                  Disconnect
                </Button>
              </>
            ) : (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setShowConnection(!showConnection)}
                borderColor={colorMode === 'dark' ? 'gray.500' : 'blue.500'}
                color={colorMode === 'dark' ? 'gray.200' : 'blue.600'}
                _hover={{ 
                  bg: colorMode === 'dark' ? 'gray.700' : 'blue.50',
                  borderColor: colorMode === 'dark' ? 'gray.400' : 'blue.600'
                }}
              >
                Connect DB
              </Button>
            )}
          </HStack>
        </Flex>
      </Box>

      <Flex h="calc(100vh - 80px)">
        {/* Chat area */}
        <Flex direction="column" flex="1" w="100%">
          {/* Connection Panel */}
          {showConnection && (
            <Box p={6} bg={theme.cardBg} borderBottom="1px" borderColor={theme.border}>
              <VStack gap={4} align="stretch">
                <Text fontSize="lg" fontWeight="bold" color={theme.text}>Connect to Database</Text>
                <Textarea
                  placeholder="postgresql://username:password@host:port/database"
                  value={dbUri}
                  onChange={(e) => setDbUri(e.target.value)}
                  bg={theme.inputBg}
                  border="1px"
                  borderColor={theme.border}
                  color={theme.text}
                  rows={3}
                />
                <Box bg={colorMode === 'dark' ? 'gray.700' : 'gray.50'} p={4} borderRadius="md">
                  <Text fontSize="sm" fontWeight="medium" mb={2} color={theme.text}>Supported Formats:</Text>
                  <VStack align="stretch" gap={1}>
                    <Text fontSize="xs" color="blue.400">PostgreSQL: postgresql://user:pass@host:5432/db</Text>
                    <Text fontSize="xs" color="green.400">MySQL: mysql://user:pass@host:3306/db</Text>
                    <Text fontSize="xs" color="yellow.400">SQLite: sqlite:///path/to/database.db</Text>
                  </VStack>
                </Box>
                <HStack justify="flex-end">
                  <Button 
                    variant="ghost" 
                    onClick={() => setShowConnection(false)} 
                    color={theme.text}
                  >
                    Cancel
                  </Button>
                  <Button
                    colorPalette="blue"
                    onClick={handleConnect}
                    loading={isConnecting}
                    disabled={!dbUri.trim()}
                  >
                    Connect
                  </Button>
                </HStack>
              </VStack>
            </Box>
          )}

          {/* Messages */}
          <Box flex="1" overflowY="auto" p={6} w="100%" maxW="100%">
            <Box maxW="1400px" mx="auto" w="100%">
              <AnimatePresence>
                {messages.map(renderMessage)}
              </AnimatePresence>
            {isLoading && (
              <Flex justify="flex-start" mb={4}>
                <Box
                  w={10}
                  h={10}
                  display="flex"
                  alignItems="center"
                  justifyContent="center"
                  mr={3}
                  flexShrink={0}
                >
                  <Text fontSize="2xl">
                    🤖
                  </Text>
                </Box>
                <Box bg={theme.aiBubble} p={4} borderRadius="lg" border="1px" borderColor={theme.border}>
                  <HStack>
                    <TypingIndicator />
                    <Text color={theme.text}>Processing your query...</Text>
                  </HStack>
                </Box>
              </Flex>
            )}
            <div ref={messagesEndRef} />
            </Box>
          </Box>

          {/* Input area */}
          <Box p={6} borderTop="1px" borderColor={theme.border} bg={theme.cardBg}>
            <Box maxW="1400px" mx="auto" w="100%">
              <HStack gap={3}>
              <Input
                placeholder="Ask a question about your data..."
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleSendMessage()}
                bg={theme.inputBg}
                border="1px"
                borderColor={theme.border}
                color={theme.text}
                disabled={!connectionStatus.connected || isLoading}
              />
              <Button
                colorPalette="blue"
                onClick={handleSendMessage}
                loading={isLoading}
                disabled={!inputValue.trim() || !connectionStatus.connected}
              >
                <FiSend />
              </Button>
              </HStack>
              {!connectionStatus.connected && (
                <Text fontSize="sm" color={theme.textMuted} mt={2}>
                  Connect to a database to start asking questions
                </Text>
              )}
            </Box>
          </Box>
        </Flex>
      </Flex>
    </Box>
  );
};

