import { motion, useReducedMotion } from 'framer-motion';
import { Box, Text, HStack, Spinner } from '@chakra-ui/react';
import type { Message } from '../../types/query';

interface MessageBubbleProps {
  message: Message;
  isUser: boolean;
  isTyping?: boolean;
  colorMode: 'light' | 'dark';
}

export const MessageBubble: React.FC<MessageBubbleProps> = ({ 
  message, 
  isUser,
  isTyping = false,
  colorMode 
}) => {
  const prefersReducedMotion = useReducedMotion();

  const theme = {
    userBubble: colorMode === 'dark' ? 'blue.600' : 'blue.500',
    aiBubble: colorMode === 'dark' ? 'gray.700' : 'gray.100',
    systemBubble: colorMode === 'dark' ? 'gray.700' : 'blue.50',
    text: colorMode === 'dark' ? 'gray.100' : 'gray.800',
    border: colorMode === 'dark' ? 'gray.700' : 'gray.200',
  };

  if (prefersReducedMotion) {
    return (
      <Box 
        bg={isUser ? theme.userBubble : message.type === 'system' ? theme.systemBubble : theme.aiBubble}
        color={isUser ? 'white' : theme.text}
        p={4}
        borderRadius="lg"
        shadow="sm"
        border="1px"
        borderColor={isUser ? 'transparent' : theme.border}
      >
        {isTyping ? (
          <HStack>
            <Spinner size="sm" />
            <Text>Processing your query...</Text>
          </HStack>
        ) : (
          <Text>{message.content}</Text>
        )}
      </Box>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20, scale: 0.95 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      transition={{ 
        duration: 0.3,
        ease: 'easeOut'
      }}
    >
      <Box 
        bg={isUser ? theme.userBubble : message.type === 'system' ? theme.systemBubble : theme.aiBubble}
        color={isUser ? 'white' : theme.text}
        p={4}
        borderRadius="lg"
        shadow="sm"
        border="1px"
        borderColor={isUser ? 'transparent' : theme.border}
      >
        {isTyping ? (
          <HStack>
            <Spinner size="sm" />
            <Text>Processing your query...</Text>
          </HStack>
        ) : (
          <Text>{message.content}</Text>
        )}
      </Box>
    </motion.div>
  );
};

