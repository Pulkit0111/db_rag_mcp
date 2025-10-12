import { motion, useReducedMotion } from 'framer-motion';
import { HStack, Box } from '@chakra-ui/react';

export const TypingIndicator: React.FC = () => {
  const prefersReducedMotion = useReducedMotion();

  const getDotAnimation = (delay: number) => prefersReducedMotion 
    ? {} 
    : {
        animate: { y: [0, -8, 0] },
        transition: { duration: 0.6, repeat: Infinity, delay }
      };

  return (
    <HStack gap={2}>
      <motion.div {...getDotAnimation(0)}>
        <Box w={2} h={2} borderRadius="full" bg="blue.500" />
      </motion.div>
      <motion.div {...getDotAnimation(0.2)}>
        <Box w={2} h={2} borderRadius="full" bg="blue.500" />
      </motion.div>
      <motion.div {...getDotAnimation(0.4)}>
        <Box w={2} h={2} borderRadius="full" bg="blue.500" />
      </motion.div>
    </HStack>
  );
};

