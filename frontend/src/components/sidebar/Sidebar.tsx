import { Box, DrawerRoot, DrawerBackdrop, DrawerContent, DrawerCloseTrigger, IconButton } from '@chakra-ui/react';
import { FiMenu } from 'react-icons/fi';
import { useMediaQuery } from '../../hooks/useMediaQuery';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { SampleQuestions } from './SampleQuestions';

interface SidebarProps {
  questions: string[];
  onQuestionClick: (question: string) => void;
  colorMode?: 'light' | 'dark';
}

export const Sidebar: React.FC<SidebarProps> = ({ questions, onQuestionClick, colorMode = 'light' }) => {
  const isMobile = useMediaQuery('(max-width: 768px)');
  const [isOpen, setIsOpen] = useLocalStorage('sidebar-open', true);

  const theme = {
    sidebarBg: colorMode === 'dark' ? 'gray.800' : 'gray.100',
    border: colorMode === 'dark' ? 'gray.700' : 'gray.200',
    text: colorMode === 'dark' ? 'white' : 'gray.800',
  };

  const sidebarContent = (
    <Box p={4}>
      <SampleQuestions 
        questions={questions} 
        onQuestionClick={onQuestionClick}
        colorMode={colorMode}
      />
    </Box>
  );

  if (isMobile) {
    return (
      <>
        <IconButton
          aria-label="Open menu"
          onClick={() => setIsOpen(true)}
          position="fixed"
          top={4}
          left={4}
          zIndex={10}
        >
          <FiMenu />
        </IconButton>
        <DrawerRoot open={isOpen} placement="start" onOpenChange={(e) => setIsOpen(e.open)}>
          <DrawerBackdrop />
          <DrawerContent>
            <DrawerCloseTrigger />
            {sidebarContent}
          </DrawerContent>
        </DrawerRoot>
      </>
    );
  }

  return (
    <Box
      w="300px"
      bg={theme.sidebarBg}
      borderRight="1px"
      borderColor={theme.border}
      display={isOpen ? 'block' : 'none'}
    >
      {sidebarContent}
    </Box>
  );
};

