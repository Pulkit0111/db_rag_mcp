import { createToaster, Toaster } from "@chakra-ui/react";

export const toaster = createToaster({
  placement: "top",
  pauseOnPageIdle: true,
});

export function ToasterProvider() {
  return (
    <Toaster toaster={toaster}>
      {(toast) => {
        const getBackgroundColor = () => {
          switch (toast.type) {
            case 'success':
              return '#48BB78';
            case 'error':
              return '#E53E3E'; // Darker red for better visibility
            case 'warning':
              return '#ED8936';
            case 'info':
              return '#4299E1';
            default:
              return '#718096';
          }
        };

        return (
          <div
            style={{
              background: getBackgroundColor(),
              color: 'white',
              padding: '16px 20px',
              borderRadius: '12px',
              boxShadow: '0 10px 25px rgba(0, 0, 0, 0.2)',
              minWidth: '320px',
              maxWidth: '500px',
              position: 'relative',
              animation: 'slideInDown 0.3s ease-out, fadeOut 0.3s ease-in 2.7s', // Changed from 4.7s to 2.7s (3s - 0.3s)
              textAlign: 'center',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
            }}
          >
            {/* Close button */}
            <button
              onClick={() => toaster.dismiss(toast.id)}
              style={{
                position: 'absolute',
                top: '8px',
                right: '8px',
                background: 'rgba(255, 255, 255, 0.2)',
                border: 'none',
                borderRadius: '50%',
                width: '24px',
                height: '24px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                cursor: 'pointer',
                color: 'white',
                fontSize: '16px',
                fontWeight: 'bold',
                transition: 'background 0.2s',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'rgba(255, 255, 255, 0.3)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'rgba(255, 255, 255, 0.2)';
              }}
            >
              ×
            </button>

            {toast.title && (
              <div style={{ 
                fontWeight: 'bold', 
                marginBottom: toast.description ? '8px' : '0',
                fontSize: '16px',
                width: '100%',
                paddingRight: '20px', // Space for close button
              }}>
                {toast.title}
              </div>
            )}
            {toast.description && (
              <div style={{ 
                fontSize: '14px', 
                opacity: 0.95,
                width: '100%',
                paddingRight: '20px', // Space for close button
              }}>
                {toast.description}
              </div>
            )}

            <style>
              {`
                @keyframes slideInDown {
                  from {
                    transform: translateY(-100%);
                    opacity: 0;
                  }
                  to {
                    transform: translateY(0);
                    opacity: 1;
                  }
                }

                @keyframes fadeOut {
                  from {
                    opacity: 1;
                    transform: scale(1);
                  }
                  to {
                    opacity: 0;
                    transform: scale(0.95);
                  }
                }
              `}
            </style>
          </div>
        );
      }}
    </Toaster>
  );
}

