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
  Spinner,
  Code,
} from '@chakra-ui/react';
import { FiSend, FiDatabase } from 'react-icons/fi';
import { apiService } from '../services/api';
import { useColorMode, ColorModeButton } from './ui/color-mode';

interface Message {
  id: string;
  type: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  data?: any;
  sql?: string;
  error?: string;
}

interface ConnectionStatus {
  connected: boolean;
  database_type?: string;
  host?: string;
  port?: number;
  database?: string;
  message?: string;
}

const SAMPLE_QUESTIONS = [
  "Show me all users",
  "What are the top 5 products by sales?",
  "How many orders were placed last month?",
  "List customers by total purchase amount",
  "Show me the revenue by product category",
  "Which products have low inventory?",
  "Get the average order value",
  "Show me recent transactions"
];

export const SimpleChatInterface: React.FC = () => {
  const { colorMode } = useColorMode();
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>({ connected: false });
  const [dbUri, setDbUri] = useState('');
  const [isConnecting, setIsConnecting] = useState(false);
  const [showConnection, setShowConnection] = useState(false);
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
      type: 'system',
      content: 'Welcome to Natural Language SQL! Connect to your database and start asking questions in plain English.',
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
      alert('Please enter a valid database connection URI');
      return;
    }

    setIsConnecting(true);
    try {
      const result = await apiService.connectDatabase(dbUri);
      if (result.success) {
        await checkConnectionStatus();
        setShowConnection(false);
        setDbUri('');
        
        // Add connection success message
        const newMessage: Message = {
          id: Date.now().toString(),
          type: 'system',
          content: `Successfully connected to database. You can now ask questions about your data!`,
          timestamp: new Date()
        };
        setMessages(prev => [...prev, newMessage]);
      } else {
        alert(`Connection failed: ${result.error}`);
      }
    } catch (error) {
      alert(`Connection failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setIsConnecting(false);
    }
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    if (!connectionStatus.connected) {
      alert('Please connect to a database first');
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
    } catch (error) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: 'Sorry, I encountered an error processing your request.',
        timestamp: new Date(),
        error: error instanceof Error ? error.message : 'Unknown error'
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSampleQuestion = (question: string) => {
    setInputValue(question);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    alert('Copied to clipboard');
  };

  const renderMessage = (message: Message) => {
    const isUser = message.type === 'user';
    const isSystem = message.type === 'system';

    return (
      <Flex
        key={message.id}
        justify={isUser ? 'flex-end' : 'flex-start'}
        mb={4}
        align="flex-start"
      >
        {!isUser && (
          <Box
            w={8}
            h={8}
            borderRadius="full"
            bg={isSystem ? 'blue.500' : 'green.500'}
            display="flex"
            alignItems="center"
            justifyContent="center"
            mr={3}
            flexShrink={0}
          >
            <Text color="white" fontSize="sm" fontWeight="bold">
              {isSystem ? 'S' : 'AI'}
            </Text>
          </Box>
        )}
        
        <Box maxW="80%" flex="1">
          <Box 
            bg={isUser ? theme.userBubble : isSystem ? theme.systemBubble : theme.aiBubble}
            color={isUser ? 'white' : colorMode === 'dark' ? 'gray.100' : 'gray.800'}
            p={4}
            borderRadius="lg"
            shadow="sm"
            border="1px"
            borderColor={isUser ? 'transparent' : theme.border}
          >
            <Text>{message.content}</Text>
            
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
              <Box mt={3} overflowX="auto" border="1px" borderColor={theme.border} borderRadius="md">
                <Box as="table" width="100%" fontSize="sm">
                  <Box as="thead" bg={colorMode === 'dark' ? 'gray.800' : 'gray.50'}>
                    <Box as="tr">
                      {Object.keys(message.data.results[0]).map((key) => (
                        <Box as="th" key={key} p={2} textAlign="left" color={theme.textSecondary} fontWeight="semibold">
                          {key}
                        </Box>
                      ))}
                    </Box>
                  </Box>
                  <Box as="tbody">
                    {message.data.results.slice(0, 10).map((row: any, idx: number) => (
                      <Box as="tr" key={idx} borderTop="1px" borderColor={theme.border}>
                        {Object.values(row).map((value: any, i) => (
                          <Box as="td" key={i} p={2} color={colorMode === 'dark' ? 'gray.200' : 'gray.700'}>
                            {String(value)}
                          </Box>
                        ))}
                      </Box>
                    ))}
                  </Box>
                </Box>
                {message.data.results.length > 10 && (
                  <Text fontSize="sm" color={theme.textMuted} mt={2} p={2}>
                    Showing 10 of {message.data.results.length} results
                  </Text>
                )}
              </Box>
            )}
            
            {message.error && (
              <Box mt={3} p={3} bg={theme.errorBg} borderRadius="md" borderLeft="4px" borderColor={theme.errorBorder}>
                <Text color={theme.errorText} fontSize="sm" fontWeight="bold">Error:</Text>
                <Text color={theme.errorText} fontSize="sm">{message.error}</Text>
              </Box>
            )}
          </Box>
          
          <Text fontSize="xs" color={theme.textMuted} mt={1}>
            {message.timestamp.toLocaleTimeString()}
          </Text>
        </Box>
        
        {isUser && (
          <Box
            w={8}
            h={8}
            borderRadius="full"
            bg="blue.500"
            display="flex"
            alignItems="center"
            justifyContent="center"
            ml={3}
            flexShrink={0}
          >
            <Text color="white" fontSize="sm" fontWeight="bold">U</Text>
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
            <Text fontSize="xl" fontWeight="bold" color={theme.text}>Natural Language SQL</Text>
          </HStack>
          
          <HStack gap={3}>
            <Badge 
              colorScheme={connectionStatus.connected ? 'green' : 'red'}
              variant="solid"
            >
              {connectionStatus.connected ? 'Connected' : 'Disconnected'}
            </Badge>
            {connectionStatus.connected && (
              <Text fontSize="sm" color={theme.textMuted}>
                {connectionStatus.database_type} • {connectionStatus.database}
              </Text>
            )}
            <ColorModeButton />
            <Button
              colorScheme="blue"
              variant="outline"
              size="sm"
              onClick={() => setShowConnection(!showConnection)}
              color={colorMode === 'dark' ? 'blue.200' : 'blue.600'}
              borderColor={colorMode === 'dark' ? 'blue.300' : 'blue.500'}
              _hover={{ 
                bg: colorMode === 'dark' ? 'blue.800' : 'blue.50',
                color: colorMode === 'dark' ? 'blue.100' : 'blue.700'
              }}
            >
              {connectionStatus.connected ? 'Reconnect' : 'Connect DB'}
            </Button>
          </HStack>
        </Flex>
      </Box>

      <Flex h="calc(100vh - 80px)">
        {/* Sidebar with sample questions */}
        <Box w="300px" bg={theme.sidebarBg} borderRight="1px" borderColor={theme.border} p={4}>
          <Text fontSize="lg" fontWeight="bold" mb={4} color={theme.text}>
            Sample Questions
          </Text>
          <VStack align="stretch" gap={2}>
            {SAMPLE_QUESTIONS.map((question, index) => (
              <Button
                key={index}
                variant="ghost"
                justifyContent="flex-start"
                h="auto"
                p={3}
                whiteSpace="normal"
                textAlign="left"
                onClick={() => handleSampleQuestion(question)}
                _hover={{ bg: colorMode === 'dark' ? 'gray.700' : 'gray.200' }}
                color={theme.text}
              >
                <Text fontSize="sm">{question}</Text>
              </Button>
            ))}
          </VStack>
        </Box>

        {/* Chat area */}
        <Flex direction="column" flex="1">
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
                  _placeholder={{ color: theme.textMuted }}
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
                    _hover={{ bg: colorMode === 'dark' ? 'gray.700' : 'gray.100' }}
                  >
                    Cancel
                  </Button>
                  <Button
                    colorScheme="blue"
                    onClick={handleConnect}
                    loading={isConnecting}
                    disabled={!dbUri.trim()}
                    color="white"
                    _hover={{ bg: 'blue.600' }}
                  >
                    Connect
                  </Button>
                </HStack>
              </VStack>
            </Box>
          )}

          {/* Messages */}
          <Box flex="1" overflowY="auto" p={6}>
            {messages.map(renderMessage)}
            {isLoading && (
              <Flex justify="flex-start" mb={4}>
                <Box
                  w={8}
                  h={8}
                  borderRadius="full"
                  bg="green.500"
                  display="flex"
                  alignItems="center"
                  justifyContent="center"
                  mr={3}
                >
                  <Spinner size="sm" color="white" />
                </Box>
                <Box bg={theme.aiBubble} p={4} borderRadius="lg" border="1px" borderColor={theme.border}>
                  <HStack>
                    <Spinner size="sm" color={theme.text} />
                    <Text color={theme.text}>Processing your query...</Text>
                  </HStack>
                </Box>
              </Flex>
            )}
            <div ref={messagesEndRef} />
          </Box>

          {/* Input area */}
          <Box p={6} borderTop="1px" borderColor={theme.border} bg={theme.cardBg}>
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
                _placeholder={{ color: theme.textMuted }}
                _focus={{ borderColor: 'blue.500', boxShadow: '0 0 0 1px blue.500' }}
                disabled={!connectionStatus.connected || isLoading}
              />
              <Button
                colorScheme="blue"
                onClick={handleSendMessage}
                loading={isLoading}
                disabled={!inputValue.trim() || !connectionStatus.connected}
                color="white"
                _hover={{ bg: 'blue.600' }}
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
        </Flex>
      </Flex>
    </Box>
  );
};
