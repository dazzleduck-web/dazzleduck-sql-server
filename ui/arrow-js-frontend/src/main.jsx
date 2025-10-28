import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { LoggingProvider } from './context/LoggingContext.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <LoggingProvider>
      <App />
    </LoggingProvider>
  </StrictMode>
)
