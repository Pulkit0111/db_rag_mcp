import { VStack, Button, Text } from '@chakra-ui/react';

interface SampleQuestionsProps {
  questions: string[];
  onQuestionClick: (question: string) => void;
  colorMode?: 'light' | 'dark';
}

export const SampleQuestions: React.FC<SampleQuestionsProps> = ({ 
  questions, 
  onQuestionClick,
  colorMode = 'light'
}) => {
  const theme = {
    text: colorMode === 'dark' ? 'white' : 'gray.800',
    hoverBg: colorMode === 'dark' ? 'gray.700' : 'gray.200',
  };

  return (
    <VStack align="stretch" gap={2}>
      {questions.map((question, index) => (
        <Button
          key={index}
          variant="ghost"
          justifyContent="flex-start"
          h="auto"
          p={3}
          whiteSpace="normal"
          textAlign="left"
          onClick={() => onQuestionClick(question)}
          _hover={{ bg: theme.hoverBg }}
          color={theme.text}
        >
          <Text fontSize="sm">{question}</Text>
        </Button>
      ))}
    </VStack>
  );
};

