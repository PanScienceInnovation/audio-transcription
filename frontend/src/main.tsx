import React from 'react'
import ReactDOM from 'react-dom/client'
import { GoogleOAuthProvider } from '@react-oauth/google'
import AppWithAuth from './AppWithAuth.tsx'
import './index.css'

// Google OAuth Client ID - Replace with your actual client ID
// You can get this from Google Cloud Console: https://console.cloud.google.com/
const GOOGLE_CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID || 'YOUR_GOOGLE_CLIENT_ID_HERE'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <GoogleOAuthProvider clientId={GOOGLE_CLIENT_ID}>
      <AppWithAuth />
    </GoogleOAuthProvider>
  </React.StrictMode>,
)

