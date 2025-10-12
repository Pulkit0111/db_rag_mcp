import { toaster } from "../components/ui/toaster";

export type ToastType = 'success' | 'error' | 'warning' | 'info' | 'loading';

export interface ToastOptions {
  title: string;
  description?: string;
  duration?: number;
  isClosable?: boolean;
}

export const useToast = () => {
  const showToast = (type: ToastType, options: ToastOptions) => {
    return toaster.create({
      title: options.title,
      description: options.description,
      type: type,
      duration: options.duration ?? 3000, // Changed from 5000 to 3000
    });
  };

  return {
    success: (options: ToastOptions) => showToast('success', options),
    error: (options: ToastOptions) => showToast('error', options),
    warning: (options: ToastOptions) => showToast('warning', options),
    info: (options: ToastOptions) => showToast('info', options),
    loading: (options: ToastOptions) => showToast('loading', { ...options, duration: undefined }),
  };
};

