import { useState, useEffect } from 'react';
import SignIn from './SignIn';
import MainApp from './MainApp';

function AppWithAuth() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check if user is already signed in
    const user = localStorage.getItem('user');
    if (user) {
      setIsAuthenticated(true);
    }
    setLoading(false);
  }, []);

  const handleSignIn = (userInfo: any) => {
    setIsAuthenticated(true);
  };

  const handleSignOut = () => {
    localStorage.removeItem('user');
    localStorage.removeItem('access_token');
    setIsAuthenticated(false);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-blue-50 to-purple-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <SignIn onSignIn={handleSignIn} />;
  }

  return <MainApp onSignOut={handleSignOut} />;
}

export default AppWithAuth;

