import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Users, FileText, CheckSquare, Flag, Loader2, ArrowLeft, Search, X } from 'lucide-react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || (import.meta.env.DEV ? 'http://localhost:5002' : '/api');

// Helper function to get user info from localStorage
const getUserInfo = (): { id: string | null; isAdmin: boolean } => {
  try {
    const userStr = localStorage.getItem('user');
    if (userStr) {
      const user = JSON.parse(userStr);
      return {
        id: user.sub || user.id || null,
        isAdmin: user.is_admin || false
      };
    }
  } catch (error) {
    console.error('Error getting user info:', error);
  }
  return { id: null, isAdmin: false };
};

// Helper function to get axios config with user headers
const getAxiosConfig = () => {
  const { id, isAdmin } = getUserInfo();
  const headers: Record<string, string> = {};
  if (id) {
    headers['X-User-ID'] = id;
  }
  headers['X-Is-Admin'] = isAdmin ? 'true' : 'false';
  return { headers };
};

interface User {
  _id: string;
  username: string;
  email: string;
  name: string;
  is_admin: boolean;
}

interface Transcription {
  _id: string;
  filename: string;
  created_at: string;
  language: string;
  assigned_user_id?: string;
  user_id?: string;
  status?: 'done' | 'pending' | 'flagged';
  is_flagged?: boolean;
}

interface UserStats {
  user: User;
  assignedFiles: number;
  annotatedFiles: number;
  flaggedFiles: number;
  pendingFiles: number;
}

function Teams() {
  const navigate = useNavigate();
  const [users, setUsers] = useState<User[]>([]);
  const [transcriptions, setTranscriptions] = useState<Transcription[]>([]);
  const [userStats, setUserStats] = useState<UserStats[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  const { isAdmin } = getUserInfo();

  useEffect(() => {
    if (!isAdmin) {
      setMessage({ type: 'error', text: 'Access denied. Admin privileges required.' });
      return;
    }
    loadData();
  }, [isAdmin]);

  const loadData = async () => {
    setLoading(true);
    try {
      const config = getAxiosConfig();

      // Load users (excluding admins)
      const usersResponse = await axios.get(`${API_BASE_URL}/api/admin/users`, config);
      if (usersResponse.data.success) {
        const allUsers = usersResponse.data.users.filter((u: User) => !u.is_admin);
        setUsers(allUsers);
      }

      // Load transcriptions
      const transcriptionsResponse = await axios.get(`${API_BASE_URL}/api/transcriptions?limit=50`, config);
      if (transcriptionsResponse.data.success) {
        setTranscriptions(transcriptionsResponse.data.data.transcriptions);
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to load data' });
    } finally {
      setLoading(false);
    }
  };

  // Calculate statistics for each user
  useEffect(() => {
    if (users.length === 0 || transcriptions.length === 0) {
      setUserStats([]);
      return;
    }

    const stats: UserStats[] = users.map(user => {
      const userTranscriptions = transcriptions.filter(t => t.assigned_user_id === user._id);

      const assignedFiles = userTranscriptions.length;
      const annotatedFiles = userTranscriptions.filter(t => t.status === 'done').length;
      const flaggedFiles = userTranscriptions.filter(t => t.is_flagged === true).length;
      const pendingFiles = userTranscriptions.filter(t => t.status === 'pending' || !t.status).length;

      return {
        user,
        assignedFiles,
        annotatedFiles,
        flaggedFiles,
        pendingFiles
      };
    });

    setUserStats(stats);
  }, [users, transcriptions]);

  // Filter users based on search term
  const filteredStats = userStats.filter(stat => {
    if (!searchTerm) return true;
    const searchLower = searchTerm.toLowerCase();
    return (
      stat.user.name?.toLowerCase().includes(searchLower) ||
      stat.user.username?.toLowerCase().includes(searchLower) ||
      stat.user.email?.toLowerCase().includes(searchLower)
    );
  });

  if (!isAdmin) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-red-600 mb-4">Access Denied</h1>
          <p className="text-gray-600">You need admin privileges to access this page.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-4">
              <button
                onClick={() => navigate('/admin')}
                className="flex items-center gap-2 px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                title="Go back to Admin Panel"
              >
                <ArrowLeft className="h-5 w-5" />
                <span className="hidden sm:inline">Back to Admin Panel</span>
              </button>
              <div>
                <h1 className="text-3xl font-bold text-gray-800 flex items-center gap-3">
                  <Users className="h-8 w-8 text-blue-600" />
                  Team Management
                </h1>
                <p className="text-gray-600 mt-2">View team member statistics and file assignments</p>
              </div>
            </div>
          </div>

          {message && (
            <div
              className={`mb-4 p-4 rounded-lg ${message.type === 'success' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'
                }`}
            >
              {message.text}
              <button
                onClick={() => setMessage(null)}
                className="float-right text-gray-500 hover:text-gray-700"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          )}

          {/* Search Bar */}
          <div className="mb-6">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-5 w-5" />
              <input
                type="text"
                placeholder="Search by name..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          </div>

          {/* Summary Statistics */}
          {!loading && userStats.length > 0 && (
            <div className="mb-8 grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-blue-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Team Members</p>
                    <p className="text-3xl font-bold text-gray-800">{userStats.length}</p>
                  </div>
                  <div className="bg-blue-100 rounded-full p-3">
                    <Users className="h-8 w-8 text-blue-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-green-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Assigned Files</p>
                    <p className="text-3xl font-bold text-gray-800">
                      {userStats.reduce((sum, stat) => sum + stat.assignedFiles, 0)}
                    </p>
                  </div>
                  <div className="bg-green-100 rounded-full p-3">
                    <FileText className="h-8 w-8 text-green-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-yellow-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Annotated Files</p>
                    <p className="text-3xl font-bold text-gray-800">
                      {userStats.reduce((sum, stat) => sum + stat.annotatedFiles, 0)}
                    </p>
                  </div>
                  <div className="bg-yellow-100 rounded-full p-3">
                    <CheckSquare className="h-8 w-8 text-yellow-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-red-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Flagged Files</p>
                    <p className="text-3xl font-bold text-gray-800">
                      {userStats.reduce((sum, stat) => sum + stat.flaggedFiles, 0)}
                    </p>
                  </div>
                  <div className="bg-red-100 rounded-full p-3">
                    <Flag className="h-8 w-8 text-red-600" />
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Team Members */}
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
            </div>
          ) : filteredStats.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              <Users className="h-16 w-16 mx-auto mb-4 text-gray-400" />
              <p className="text-lg">
                {searchTerm ? 'No users match your search' : 'No team members found'}
              </p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {filteredStats.map((stat) => (
                <div
                  key={stat.user._id}
                  className="bg-white rounded-lg shadow-lg border border-gray-200 p-6 hover:shadow-xl transition-shadow"
                >
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className="bg-blue-100 rounded-full p-3">
                        <Users className="h-6 w-6 text-blue-600" />
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-gray-800">
                          {stat.user.name || stat.user.username}
                        </h3>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-3">
                    {/* Assigned Files */}
                    <div className="flex items-center justify-between p-3 bg-blue-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <FileText className="h-5 w-5 text-blue-600" />
                        <span className="text-sm font-medium text-gray-700">Assigned Files</span>
                      </div>
                      <span className="text-xl font-bold text-blue-600">{stat.assignedFiles}</span>
                    </div>

                    {/* Annotated Files */}
                    <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <CheckSquare className="h-5 w-5 text-green-600" />
                        <span className="text-sm font-medium text-gray-700">Files Annotated</span>
                      </div>
                      <span className="text-xl font-bold text-green-600">{stat.annotatedFiles}</span>
                    </div>

                    {/* Pending Files */}
                    <div className="flex items-center justify-between p-3 bg-yellow-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <Loader2 className="h-5 w-5 text-yellow-600" />
                        <span className="text-sm font-medium text-gray-700">Pending Files</span>
                      </div>
                      <span className="text-xl font-bold text-yellow-600">{stat.pendingFiles}</span>
                    </div>

                    {/* Flagged Files */}
                    <div className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <Flag className="h-5 w-5 text-red-600" />
                        <span className="text-sm font-medium text-gray-700">Flagged Files</span>
                      </div>
                      <span className="text-xl font-bold text-red-600">{stat.flaggedFiles}</span>
                    </div>
                  </div>

                  {/* Progress Bar */}
                  {stat.assignedFiles > 0 && (
                    <div className="mt-4">
                      <div className="flex items-center justify-between text-xs text-gray-600 mb-1">
                        <span>Completion Rate</span>
                        <span>
                          {Math.round((stat.annotatedFiles / stat.assignedFiles) * 100)}%
                        </span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-2">
                        <div
                          className="bg-green-600 h-2 rounded-full transition-all"
                          style={{
                            width: `${(stat.annotatedFiles / stat.assignedFiles) * 100}%`
                          }}
                        />
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

        </div>
      </div>
    </div>
  );
}

export default Teams;

