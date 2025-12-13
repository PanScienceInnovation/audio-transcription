import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Users, FileText, CheckSquare, Flag, Loader2, ArrowLeft, Search, X, Eye, XCircle, Award, CheckCircle2 } from 'lucide-react';

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
  is_final_tester?: boolean;
}

interface UserStats {
  user: User;
  assignedFiles: number;
  annotatedFiles: number;
  completedFiles: number;
  flaggedFiles: number;
  pendingFiles: number;
  validatingFiles?: number;
  passedFiles?: number;
}

interface TeamStatsResponse {
  userStats: UserStats[];
  summary: {
    totalTeamMembers: number;
    totalAssignedFiles: number;
    totalAnnotatedFiles: number;
    totalCompletedFiles: number;
    totalFlaggedFiles: number;
    totalValidatingFiles?: number;
    totalPassedFiles?: number;
  };
}

interface Transcription {
  _id: string;
  filename: string;
  created_at: string;
  updated_at?: string;
  language: string;
  status?: string;
}

function Teams() {
  const navigate = useNavigate();
  const [userStats, setUserStats] = useState<UserStats[]>([]);
  const [summary, setSummary] = useState({
    totalTeamMembers: 0,
    totalAssignedFiles: 0,
    totalAnnotatedFiles: 0,
    totalCompletedFiles: 0,
    totalFlaggedFiles: 0
  });
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('');
  const [languageFilter, setLanguageFilter] = useState<string>('');
  const [dateFilter, setDateFilter] = useState<string>('');
  const [transcriptionTypeFilter, setTranscriptionTypeFilter] = useState<string>('');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [viewingCompletedFiles, setViewingCompletedFiles] = useState<string | null>(null);
  const [completedFilesList, setCompletedFilesList] = useState<Transcription[]>([]);
  const [loadingCompletedFiles, setLoadingCompletedFiles] = useState(false);

  const { isAdmin } = getUserInfo();

  useEffect(() => {
    if (!isAdmin) {
      setMessage({ type: 'error', text: 'Access denied. Admin privileges required.' });
      return;
    }
    loadData();
  }, [isAdmin, statusFilter, languageFilter, dateFilter, transcriptionTypeFilter]);

  const loadData = async () => {
    setLoading(true);
    try {
      const config = getAxiosConfig();

      // Build query parameters
      const params = new URLSearchParams();
      if (searchTerm) {
        params.append('search', searchTerm);
      }
      if (statusFilter) {
        params.append('status', statusFilter);
      }
      if (languageFilter) {
        params.append('language', languageFilter);
      }
      if (dateFilter) {
        params.append('date', dateFilter);
      }
      if (transcriptionTypeFilter) {
        params.append('transcription_type', transcriptionTypeFilter);
      }

      // Fetch team stats from backend with filters
      const response = await axios.get(
        `${API_BASE_URL}/api/admin/team-stats?${params.toString()}`,
        config
      );

      if (response.data.success) {
        const data: TeamStatsResponse = response.data.data;
        setUserStats(data.userStats);
        setSummary(data.summary);
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to load team statistics' });
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to load data' });
    } finally {
      setLoading(false);
    }
  };

  // Reload data when search term changes (with debounce)
  useEffect(() => {
    if (!isAdmin) return;
    
    const timer = setTimeout(() => {
      loadData();
    }, 300); // Debounce search by 300ms

    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchTerm]);

  const clearFilters = () => {
    setSearchTerm('');
    setStatusFilter('');
    setLanguageFilter('');
    setDateFilter('');
    setTranscriptionTypeFilter('');
  };

  const loadCompletedFiles = async (userId: string) => {
    setLoadingCompletedFiles(true);
    try {
      const config = getAxiosConfig();
      const params = new URLSearchParams();
      params.append('assigned_user', userId);
      params.append('status', 'completed');
      params.append('limit', '1000'); // Get all completed files

      const response = await axios.get(
        `${API_BASE_URL}/api/transcriptions?${params.toString()}`,
        config
      );

      if (response.data.success) {
        setCompletedFilesList(response.data.data.transcriptions || []);
        setViewingCompletedFiles(userId);
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to load completed files' });
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to load completed files' });
    } finally {
      setLoadingCompletedFiles(false);
    }
  };

  const handleLoadTranscription = (filename: string) => {
    const encodedFilename = encodeURIComponent(filename);
    navigate(`/word-level/transcription/${encodedFilename}?from=teams`);
  };

  const handleToggleFinalTester = async (userId: string, currentStatus: boolean) => {
    try {
      const config = getAxiosConfig();
      const response = await axios.put(
        `${API_BASE_URL}/api/admin/users/${userId}/final-tester`,
        { is_final_tester: !currentStatus },
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: `Final tester status ${!currentStatus ? 'enabled' : 'disabled'} successfully` });
        // Reload data to reflect changes
        loadData();
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to update final tester status' });
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to update final tester status' });
    }
  };

  const hasActiveFilters = searchTerm || statusFilter || languageFilter || dateFilter || transcriptionTypeFilter;

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

          {/* Filters Section */}
          <div className="mb-6 space-y-4">
            {/* Search Bar */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-5 w-5" />
              <input
                type="text"
                placeholder="Search by name, username, or email..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>

            {/* Filter Controls */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              {/* Status Filter */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Status
                </label>
                <select
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="">All Statuses</option>
                  <option value="done">Done</option>
                  <option value="pending">Pending</option>
                  <option value="flagged">Flagged</option>
                </select>
              </div>

              {/* Language Filter */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Language
                </label>
                <input
                  type="text"
                  placeholder="Filter by language..."
                  value={languageFilter}
                  onChange={(e) => setLanguageFilter(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>

              {/* Date Filter */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Date
                </label>
                <input
                  type="date"
                  value={dateFilter}
                  onChange={(e) => setDateFilter(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>

              {/* Transcription Type Filter */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Type
                </label>
                <select
                  value={transcriptionTypeFilter}
                  onChange={(e) => setTranscriptionTypeFilter(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="">All Types</option>
                  <option value="words">Words</option>
                  <option value="phrases">Phrases</option>
                </select>
              </div>
            </div>

            {/* Clear Filters Button */}
            {hasActiveFilters && (
              <div className="flex items-center gap-2">
                <button
                  onClick={clearFilters}
                  className="flex items-center gap-2 px-4 py-2 text-sm text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
                >
                  <X className="h-4 w-4" />
                  Clear Filters
                </button>
                <span className="text-sm text-gray-500">
                  {userStats.length} {userStats.length === 1 ? 'member' : 'members'} found
                </span>
              </div>
            )}
          </div>

          {/* Summary Statistics */}
          {!loading && (
            <div className="mb-8 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-blue-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Team Members</p>
                    <p className="text-3xl font-bold text-gray-800">{summary.totalTeamMembers}</p>
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
                    <p className="text-3xl font-bold text-gray-800">{summary.totalAssignedFiles}</p>
                  </div>
                  <div className="bg-green-100 rounded-full p-3">
                    <FileText className="h-8 w-8 text-green-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-yellow-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Done Files</p>
                    <p className="text-3xl font-bold text-gray-800">{summary.totalAnnotatedFiles}</p>
                  </div>
                  <div className="bg-yellow-100 rounded-full p-3">
                    <CheckSquare className="h-8 w-8 text-yellow-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-purple-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Completed Files</p>
                    <p className="text-3xl font-bold text-gray-800">{summary.totalCompletedFiles}</p>
                  </div>
                  <div className="bg-purple-100 rounded-full p-3">
                    <CheckSquare className="h-8 w-8 text-purple-600" />
                  </div>
                </div>
              </div>

              <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-red-500">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600 mb-1">Total Flagged Files</p>
                    <p className="text-3xl font-bold text-gray-800">{summary.totalFlaggedFiles}</p>
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
          ) : userStats.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              <Users className="h-16 w-16 mx-auto mb-4 text-gray-400" />
              <p className="text-lg">
                {hasActiveFilters ? 'No users match your filters' : 'No team members found'}
              </p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {userStats.map((stat) => (
                  <div
                  key={stat.user._id}
                  className="bg-white rounded-lg shadow-lg border border-gray-200 p-6 hover:shadow-xl transition-shadow flex flex-col h-full"
                >
                  <div className="flex items-center justify-between mb-4 flex-shrink-0">
                    <div className="flex items-center gap-3">
                      <div className="bg-blue-100 rounded-full p-3">
                        <Users className="h-6 w-6 text-blue-600" />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="text-lg font-semibold text-gray-800">
                            {stat.user.name || stat.user.username}
                          </h3>
                          {stat.user.is_final_tester && (
                            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800 border border-purple-300" title="Final Tester">
                              <Award className="h-3 w-3 mr-1" />
                              Final Tester
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                    <button
                      onClick={() => handleToggleFinalTester(stat.user._id, stat.user.is_final_tester || false)}
                      className={`p-2 rounded-lg transition-colors ${
                        stat.user.is_final_tester
                          ? 'bg-purple-100 text-purple-700 hover:bg-purple-200'
                          : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                      }`}
                      title={stat.user.is_final_tester ? 'Remove Final Tester' : 'Set as Final Tester'}
                    >
                      {stat.user.is_final_tester ? (
                        <CheckCircle2 className="h-5 w-5" />
                      ) : (
                        <Award className="h-5 w-5" />
                      )}
                    </button>
                  </div>

                  <div className="space-y-3 flex-1 overflow-y-auto" style={{ maxHeight: '400px' }}>
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
                        <span className="text-sm font-medium text-gray-700">Done Files</span>
                      </div>
                      <span className="text-xl font-bold text-green-600">{stat.annotatedFiles}</span>
                    </div>

                    {/* Completed Files */}
                    <div className="flex items-center justify-between p-3 bg-purple-50 rounded-lg">
                      <div className="flex items-center gap-2">
                        <CheckSquare className="h-5 w-5 text-purple-600" />
                        <span className="text-sm font-medium text-gray-700">Completed Files</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xl font-bold text-purple-600">{stat.completedFiles || 0}</span>
                        {/* {stat.completedFiles > 0 && (
                          <button
                            onClick={() => loadCompletedFiles(stat.user._id)}
                            className="p-1 text-purple-600 hover:text-purple-800 hover:bg-purple-100 rounded transition-colors"
                            title="View completed files"
                          >
                            <Eye className="h-4 w-4" />
                          </button>
                        )} */}
                      </div>
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

                    {/* Validating Files - only show if user is final tester */}
                    {stat.user.is_final_tester && (
                      <div className="flex items-center justify-between p-3 bg-indigo-50 rounded-lg">
                        <div className="flex items-center gap-2">
                          <Loader2 className="h-5 w-5 text-indigo-600" />
                          <span className="text-sm font-medium text-gray-700">Validating Files</span>
                        </div>
                        <span className="text-xl font-bold text-indigo-600">{stat.validatingFiles || 0}</span>
                      </div>
                    )}

                    {/* Passed Files - only show if user is final tester */}
                    {stat.user.is_final_tester && (
                      <div className="flex items-center justify-between p-3 bg-lime-50 rounded-lg">
                        <div className="flex items-center gap-2">
                          <CheckSquare className="h-5 w-5 text-lime-600" />
                          <span className="text-sm font-medium text-gray-700">Passed Files</span>
                        </div>
                        <span className="text-xl font-bold text-lime-600">{stat.passedFiles || 0}</span>
                      </div>
                    )}
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

      {/* Completed Files Modal */}
      {viewingCompletedFiles && (
        <>
          <div 
            className="fixed inset-0 bg-black bg-opacity-50 z-[100]" 
            onClick={() => setViewingCompletedFiles(null)}
          ></div>
          <div className="fixed top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 bg-white rounded-lg shadow-xl border border-gray-200 z-[101] w-full max-w-4xl max-h-[80vh] flex flex-col">
            <div className="flex justify-between items-center p-6 border-b border-gray-200">
              <div>
                <h3 className="text-lg font-semibold text-gray-800">
                  Completed Files
                </h3>
                <p className="text-sm text-gray-600 mt-1">
                  {(() => {
                    const user = userStats.find(s => s.user._id === viewingCompletedFiles);
                    return user ? (user.user.name || user.user.username) : 'User';
                  })()}
                </p>
              </div>
              <button 
                onClick={() => setViewingCompletedFiles(null)}
                className="text-gray-500 hover:text-gray-700"
              >
                <XCircle className="h-6 w-6" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-6">
              {loadingCompletedFiles ? (
                <div className="flex items-center justify-center py-12">
                  <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
                </div>
              ) : completedFilesList.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <FileText className="h-16 w-16 mx-auto mb-4 text-gray-400" />
                  <p className="text-lg">No completed files found</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {completedFilesList.map((file) => (
                    <div
                      key={file._id}
                      className="flex items-center justify-between p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors"
                    >
                      <div className="flex-1">
                        <button
                          onClick={() => handleLoadTranscription(file.filename)}
                          className="text-sm font-medium text-gray-900 hover:text-blue-600 hover:underline transition-colors text-left"
                        >
                          {file.filename}
                        </button>
                        <div className="flex items-center gap-4 mt-2 text-xs text-gray-500">
                          {file.language && (
                            <span>Language: {file.language}</span>
                          )}
                          {file.updated_at && (
                            <span>Updated: {new Date(file.updated_at).toLocaleDateString()}</span>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={() => handleLoadTranscription(file.filename)}
                        className="ml-4 p-2 text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded-lg transition-colors"
                        title="View transcription"
                      >
                        <Eye className="h-5 w-5" />
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className="p-6 border-t border-gray-200 flex justify-between items-center">
              <span className="text-sm text-gray-600">
                Total: {completedFilesList.length} file{completedFilesList.length !== 1 ? 's' : ''}
              </span>
              <button
                onClick={() => setViewingCompletedFiles(null)}
                className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default Teams;

