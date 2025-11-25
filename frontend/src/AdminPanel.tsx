import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Users, FileText, UserCheck, UserX, Loader2, Search, X, ChevronLeft, ChevronRight, ArrowLeft, CheckSquare, Square, Download, Trash2, Flag } from 'lucide-react';

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
  edited_words_count?: number;
  total_words?: number;
  transcription_type?: 'words' | 'phrases';
  is_flagged?: boolean;
  audio_duration?: number;
}

function AdminPanel() {
  const navigate = useNavigate();
  const [users, setUsers] = useState<User[]>([]);
  const [transcriptions, setTranscriptions] = useState<Transcription[]>([]);
  const [loading, setLoading] = useState(true);
  const [assigning, setAssigning] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedUserId, setSelectedUserId] = useState<string>('');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(20); // 20 items per page
  const [selectedTranscriptions, setSelectedTranscriptions] = useState<Set<string>>(new Set());
  const [bulkAssignUserId, setBulkAssignUserId] = useState<string>('');
  const [bulkAssigning, setBulkAssigning] = useState(false);
  const [languageFilter, setLanguageFilter] = useState<string>('');
  const [dateFilter, setDateFilter] = useState<string>('');
  const [downloading, setDownloading] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [flagging, setFlagging] = useState<string | null>(null);
  const [showFlagDropdown, setShowFlagDropdown] = useState<string | null>(null);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; right: number } | null>(null);

  const { isAdmin } = getUserInfo();

  useEffect(() => {
    if (!isAdmin) {
      setMessage({ type: 'error', text: 'Access denied. Admin privileges required.' });
      return;
    }
    loadData();
  }, [isAdmin]);

  // Reset to page 1 when search term or filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, languageFilter, dateFilter]);

  const loadData = async () => {
    setLoading(true);
    try {
      const config = getAxiosConfig();
      
      // Load users
      const usersResponse = await axios.get(`${API_BASE_URL}/api/admin/users`, config);
      if (usersResponse.data.success) {
        setUsers(usersResponse.data.users.filter((u: User) => !u.is_admin)); // Exclude admins from assignment list
      }

      // Load transcriptions
      const transcriptionsResponse = await axios.get(`${API_BASE_URL}/api/transcriptions?limit=1000`, config);
      if (transcriptionsResponse.data.success) {
        setTranscriptions(transcriptionsResponse.data.data.transcriptions);
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to load data' });
    } finally {
      setLoading(false);
    }
  };

  const handleAssign = async (transcriptionId: string, userId: string) => {
    setAssigning(transcriptionId);
    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/${transcriptionId}/assign`,
        { assigned_user_id: userId },
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'Transcription assigned successfully' });
        loadData(); // Reload data
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to assign transcription' });
    } finally {
      setAssigning(null);
    }
  };

  const handleUnassign = async (transcriptionId: string) => {
    setAssigning(transcriptionId);
    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/${transcriptionId}/unassign`,
        {},
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'Transcription unassigned successfully' });
        loadData(); // Reload data
      }
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to unassign transcription' });
    } finally {
      setAssigning(null);
    }
  };

  const handleBulkAssign = async () => {
    if (!bulkAssignUserId || selectedTranscriptions.size === 0) {
      setMessage({ type: 'error', text: 'Please select files and a user to assign' });
      return;
    }

    setBulkAssigning(true);
    const selectedIds = Array.from(selectedTranscriptions);
    console.log(`🔄 Bulk assigning ${selectedIds.length} transcriptions to user ${bulkAssignUserId}`);
    console.log('Selected IDs:', selectedIds);
    
    let successCount = 0;
    let errorCount = 0;
    const errors: string[] = [];

    try {
      const config = getAxiosConfig();
      
      // Assign all selected transcriptions in parallel
      const assignments = selectedIds.map((id, index) =>
        axios.post(
          `${API_BASE_URL}/api/admin/transcriptions/${id}/assign`,
          { assigned_user_id: bulkAssignUserId },
          config
        )
        .then(response => {
          // Verify the response indicates success
          if (response.data && response.data.success) {
            console.log(`✅ Assigned transcription ${index + 1}/${selectedIds.length}: ${id} to user ${bulkAssignUserId}`);
            console.log(`   Response:`, response.data);
            return { success: true, id, response };
          } else {
            const errorMsg = response.data?.error || 'Assignment failed - no success flag';
            console.error(`❌ Assignment failed for ${id}:`, errorMsg);
            errorCount++;
            errors.push(`ID ${id}: ${errorMsg}`);
            return { success: false, id, error: errorMsg };
          }
        })
        .catch(error => {
          errorCount++;
          const errorMsg = error.response?.data?.error || error.message || 'Unknown error';
          errors.push(`ID ${id}: ${errorMsg}`);
          console.error(`❌ Failed to assign transcription ${id}:`, errorMsg);
          if (error.response) {
            console.error(`   Response status: ${error.response.status}`);
            console.error(`   Response data:`, error.response.data);
          }
          return { success: false, id, error: errorMsg };
        })
      );

      const results = await Promise.all(assignments);
      successCount = results.filter(r => r.success).length;

      console.log(`📊 Assignment results: ${successCount} succeeded, ${errorCount} failed`);

      if (successCount > 0) {
        setMessage({ 
          type: 'success', 
          text: `Successfully assigned ${successCount} transcription${successCount > 1 ? 's' : ''}${errorCount > 0 ? `. ${errorCount} failed.` : ''}` 
        });
        setSelectedTranscriptions(new Set()); // Clear selection
        setBulkAssignUserId(''); // Reset dropdown
        loadData(); // Reload data
      } else {
        const errorDetails = errors.length > 0 ? ` Errors: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '...' : ''}` : '';
        setMessage({ type: 'error', text: `Failed to assign transcriptions.${errorDetails}` });
      }
    } catch (error: any) {
      console.error('❌ Bulk assignment error:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to assign transcriptions' });
    } finally {
      setBulkAssigning(false);
    }
  };

  const handleBulkUnassign = async () => {
    if (selectedTranscriptions.size === 0) {
      setMessage({ type: 'error', text: 'Please select files to unassign' });
      return;
    }

    setBulkAssigning(true);
    const selectedIds = Array.from(selectedTranscriptions);
    console.log(`🔄 Bulk unassigning ${selectedIds.length} transcriptions`);
    console.log('Selected IDs:', selectedIds);
    
    let successCount = 0;
    let errorCount = 0;
    const errors: string[] = [];

    try {
      const config = getAxiosConfig();
      
      // Unassign all selected transcriptions in parallel
      const unassignments = selectedIds.map((id, index) =>
        axios.post(
          `${API_BASE_URL}/api/admin/transcriptions/${id}/unassign`,
          {},
          config
        )
        .then(response => {
          // Verify the response indicates success
          if (response.data && response.data.success) {
            console.log(`✅ Unassigned transcription ${index + 1}/${selectedIds.length}: ${id}`);
            return { success: true, id, response };
          } else {
            const errorMsg = response.data?.error || 'Unassignment failed - no success flag';
            console.error(`❌ Unassignment failed for ${id}:`, errorMsg);
            errorCount++;
            errors.push(`ID ${id}: ${errorMsg}`);
            return { success: false, id, error: errorMsg };
          }
        })
        .catch(error => {
          errorCount++;
          const errorMsg = error.response?.data?.error || error.message || 'Unknown error';
          errors.push(`ID ${id}: ${errorMsg}`);
          console.error(`❌ Failed to unassign transcription ${id}:`, errorMsg);
          return { success: false, id, error: errorMsg };
        })
      );

      const results = await Promise.all(unassignments);
      successCount = results.filter(r => r.success).length;

      console.log(`📊 Unassignment results: ${successCount} succeeded, ${errorCount} failed`);

      if (successCount > 0) {
        setMessage({ 
          type: 'success', 
          text: `Successfully unassigned ${successCount} transcription${successCount > 1 ? 's' : ''}${errorCount > 0 ? `. ${errorCount} failed.` : ''}` 
        });
        setSelectedTranscriptions(new Set()); // Clear selection
        loadData(); // Reload data
      } else {
        const errorDetails = errors.length > 0 ? ` Errors: ${errors.slice(0, 3).join('; ')}${errors.length > 3 ? '...' : ''}` : '';
        setMessage({ type: 'error', text: `Failed to unassign transcriptions.${errorDetails}` });
      }
    } catch (error: any) {
      console.error('❌ Bulk unassignment error:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to unassign transcriptions' });
    } finally {
      setBulkAssigning(false);
    }
  };

  const handleDownloadDoneTranscriptions = async () => {
    setDownloading(true);
    try {
      const config = getAxiosConfig();
      
      // Make request to download endpoint with responseType: 'blob' for binary data
      const response = await axios.get(
        `${API_BASE_URL}/api/admin/transcriptions/download-done`,
        {
          ...config,
          responseType: 'blob'
        }
      );

      // Create a blob URL and trigger download
      const blob = new Blob([response.data], { type: 'application/zip' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Get filename from Content-Disposition header or use default
      const contentDisposition = response.headers['content-disposition'];
      let filename = 'done_transcriptions.zip';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/i);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      
      // Cleanup
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      setMessage({ type: 'success', text: 'Download completed successfully' });
    } catch (error: any) {
      console.error('❌ Error downloading transcriptions:', error);
      const errorMessage = error.response?.data?.error || error.message || 'Failed to download transcriptions';
      setMessage({ type: 'error', text: errorMessage });
    } finally {
      setDownloading(false);
    }
  };

  const handleDeleteTranscription = async (transcriptionId: string) => {
    const confirmed = window.confirm(
      'Are you sure you want to delete this transcription?\n\n' +
      'This action cannot be undone. The following will be permanently deleted:\n' +
      '• Transcription data from database\n' +
      '• Audio file from S3 storage\n\n' +
      'This action is irreversible.'
    );
    
    if (!confirmed) {
      return;
    }

    setDeleting(transcriptionId);
    try {
      const config = getAxiosConfig();
      const response = await axios.delete(
        `${API_BASE_URL}/api/transcriptions/${transcriptionId}`,
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'Transcription deleted successfully' });
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to delete transcription' });
      }
    } catch (error: any) {
      console.error('❌ Error deleting transcription:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to delete transcription' });
    } finally {
      setDeleting(null);
    }
  };

  const handleFlagTranscription = async (transcriptionId: string, currentFlagged: boolean, reason?: string) => {
    setFlagging(transcriptionId);
    setShowFlagDropdown(null);
    try {
      const config = getAxiosConfig();
      const newFlaggedState = !currentFlagged;
      
      const response = await axios.post(
        `${API_BASE_URL}/api/transcriptions/${transcriptionId}/flag`,
        { 
          is_flagged: newFlaggedState,
          flag_reason: newFlaggedState ? reason : null
        },
        config
      );

      if (response.data.success) {
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to flag transcription' });
      }
    } catch (error: any) {
      console.error('Error flagging transcription:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to flag transcription' });
    } finally {
      setFlagging(null);
    }
  };

  const getUserName = (userId: string | undefined) => {
    if (!userId) return 'Unassigned';
    const user = users.find(u => u._id === userId);
    return user ? user.name || user.username : userId;
  };

  // Helper function to format duration in hours, minutes, and seconds
  const formatDuration = (seconds: number): string => {
    if (!seconds || seconds === 0) return '0 hours';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    
    const parts: string[] = [];
    if (hours > 0) parts.push(`${hours} hour${hours !== 1 ? 's' : ''}`);
    if (minutes > 0) parts.push(`${minutes} minute${minutes !== 1 ? 's' : ''}`);
    if (secs > 0 && hours === 0) parts.push(`${secs} second${secs !== 1 ? 's' : ''}`);
    
    return parts.length > 0 ? parts.join(', ') : '0 hours';
  };

  // Calculate total duration of files with status "done"
  const totalDoneDuration = transcriptions
    .filter(t => t.status === 'done' && t.audio_duration)
    .reduce((sum, t) => sum + (t.audio_duration || 0), 0);

  // Get unique languages for filter dropdown
  const uniqueLanguages = Array.from(new Set(transcriptions.map(t => t.language).filter(Boolean))).sort();

  // Filter transcriptions based on search term, language, and date
  const filteredTranscriptions = transcriptions.filter(t => {
    // Search term filter
    const searchLower = searchTerm.toLowerCase();
    const matchesSearch = !searchTerm || (
      t.filename.toLowerCase().includes(searchLower) ||
      getUserName(t.assigned_user_id).toLowerCase().includes(searchLower) ||
      (t.status && t.status.toLowerCase().includes(searchLower)) ||
      (searchLower === 'done' && t.status === 'done') ||
      (searchLower === 'pending' && t.status === 'pending') ||
      (searchLower === 'flagged' && t.status === 'flagged')
    );

    // Language filter
    const matchesLanguage = !languageFilter || t.language === languageFilter;

    // Date filter
    let matchesDate = true;
    if (dateFilter) {
      const transcriptionDate = new Date(t.created_at);
      const filterDate = new Date(dateFilter);
      // Compare dates (ignore time)
      matchesDate = 
        transcriptionDate.getFullYear() === filterDate.getFullYear() &&
        transcriptionDate.getMonth() === filterDate.getMonth() &&
        transcriptionDate.getDate() === filterDate.getDate();
    }

    return matchesSearch && matchesLanguage && matchesDate;
  });

  // Calculate pagination
  const totalItems = filteredTranscriptions.length;
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedTranscriptions = filteredTranscriptions.slice(startIndex, endIndex);

  // Handle select/deselect all
  const handleSelectAll = (checked: boolean) => {
    if (checked) {
      const allIds = new Set(paginatedTranscriptions.map(t => t._id));
      setSelectedTranscriptions(prev => new Set([...prev, ...allIds]));
    } else {
      const currentPageIds = new Set(paginatedTranscriptions.map(t => t._id));
      setSelectedTranscriptions(prev => {
        const newSet = new Set(prev);
        currentPageIds.forEach(id => newSet.delete(id));
        return newSet;
      });
    }
  };

  // Handle individual selection
  const handleSelectTranscription = (id: string, checked: boolean) => {
    setSelectedTranscriptions(prev => {
      const newSet = new Set(prev);
      if (checked) {
        newSet.add(id);
      } else {
        newSet.delete(id);
      }
      return newSet;
    });
  };

  // Check if all current page items are selected
  const allCurrentPageSelected = paginatedTranscriptions.length > 0 && 
    paginatedTranscriptions.every(t => selectedTranscriptions.has(t._id));
  
  // Check if some (but not all) current page items are selected
  const someCurrentPageSelected = paginatedTranscriptions.some(t => selectedTranscriptions.has(t._id)) && 
    !allCurrentPageSelected;

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
                onClick={() => navigate(-1)}
                className="flex items-center gap-2 px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                title="Go back"
              >
                <ArrowLeft className="h-5 w-5" />
                <span className="hidden sm:inline">Back</span>
              </button>
              <div>
                <h1 className="text-3xl font-bold text-gray-800 flex items-center gap-3">
                  <Users className="h-8 w-8 text-blue-600" />
                  Admin Panel
                </h1>
                <p className="text-gray-600 mt-2">Manage transcription assignments</p>
              </div>
            </div>
            <button
              onClick={handleDownloadDoneTranscriptions}
              disabled={downloading}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              title="Download all done transcriptions as ZIP"
            >
              {downloading ? (
                <>
                  <Loader2 className="h-5 w-5 animate-spin" />
                  <span className="hidden sm:inline">Downloading...</span>
                </>
              ) : (
                <>
                  <Download className="h-5 w-5" />
                  <span className="hidden sm:inline">Download Done Files</span>
                </>
              )}
            </button>
          </div>

          {message && (
            <div
              className={`mb-4 p-4 rounded-lg ${
                message.type === 'success' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'
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

          {/* Total Duration Stats */}
          <div className="mb-6 ml-4 bg-transparent border-none">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-medium text-blue-900 mb-1">Total Duration of Done Files: {formatDuration(totalDoneDuration)}</h3>
              </div>
            </div>
          </div>

          <div className="mb-6 space-y-4">
            {/* Search and Filter Controls */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-5 w-5" />
                <input
                  type="text"
                  placeholder="Search by filename or user..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <select
                value={languageFilter}
                onChange={(e) => setLanguageFilter(e.target.value)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              >
                <option value="">All Languages</option>
                {uniqueLanguages.map((lang) => (
                  <option key={lang} value={lang}>
                    {lang}
                  </option>
                ))}
              </select>
              <input
                type="date"
                value={dateFilter}
                onChange={(e) => setDateFilter(e.target.value)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Filter by date"
              />
            </div>
            {/* Clear Filters Button */}
            {(languageFilter || dateFilter) && (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => {
                    setLanguageFilter('');
                    setDateFilter('');
                  }}
                  className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
                >
                  <X className="h-4 w-4" />
                  Clear Filters
                </button>
              </div>
            )}

            {/* Bulk Assignment Controls */}
            {selectedTranscriptions.size > 0 && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 flex items-center justify-between flex-wrap gap-4">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-blue-900">
                    {selectedTranscriptions.size} file{selectedTranscriptions.size > 1 ? 's' : ''} selected
                  </span>
                </div>
                <div className="flex items-center gap-2 flex-wrap">
                  <select
                    value={bulkAssignUserId}
                    onChange={(e) => setBulkAssignUserId(e.target.value)}
                    disabled={bulkAssigning}
                    className="text-sm border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:opacity-50"
                  >
                    <option value="">Select user to assign...</option>
                    {users.map((user) => (
                      <option key={user._id} value={user._id}>
                        {user.name || user.username}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={handleBulkAssign}
                    disabled={!bulkAssignUserId || bulkAssigning}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {bulkAssigning ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Assigning...
                      </>
                    ) : (
                      <>
                        <UserCheck className="h-4 w-4" />
                        Assign Selected
                      </>
                    )}
                  </button>
                  <button
                    onClick={handleBulkUnassign}
                    disabled={bulkAssigning}
                    className="flex items-center gap-2 px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {bulkAssigning ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Unassigning...
                      </>
                    ) : (
                      <>
                        <UserX className="h-4 w-4" />
                        Unassign Selected
                      </>
                    )}
                  </button>
                  <button
                    onClick={() => {
                      setSelectedTranscriptions(new Set());
                      setBulkAssignUserId('');
                    }}
                    className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    Clear Selection
                  </button>
                </div>
              </div>
            )}
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-100">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider w-12">
                      <button
                        onClick={() => handleSelectAll(!allCurrentPageSelected)}
                        className="flex items-center justify-center"
                        title={allCurrentPageSelected ? 'Deselect all' : 'Select all'}
                      >
                        {allCurrentPageSelected ? (
                          <CheckSquare className="h-5 w-5 text-blue-600" />
                        ) : someCurrentPageSelected ? (
                          <div className="relative">
                            <Square className="h-5 w-5 text-gray-400" />
                            <div className="absolute inset-0 flex items-center justify-center">
                              <div className="h-3 w-3 bg-blue-600 rounded-sm" />
                            </div>
                          </div>
                        ) : (
                          <Square className="h-5 w-5 text-gray-400" />
                        )}
                      </button>
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider w-16">
                      S.No.
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Filename
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Language
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Created
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Edited Words
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Assigned To
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {paginatedTranscriptions.length === 0 ? (
                    <tr>
                      <td colSpan={9} className="px-4 py-8 text-center text-gray-500">
                        {searchTerm ? 'No transcriptions match your search' : 'No transcriptions found'}
                      </td>
                    </tr>
                  ) : (
                    paginatedTranscriptions.map((transcription, index) => (
                      <tr key={transcription._id} className={`hover:bg-gray-50 ${selectedTranscriptions.has(transcription._id) ? 'bg-blue-50' : ''}`}>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <button
                            onClick={() => handleSelectTranscription(transcription._id, !selectedTranscriptions.has(transcription._id))}
                            className="flex items-center justify-center"
                          >
                            {selectedTranscriptions.has(transcription._id) ? (
                              <CheckSquare className="h-5 w-5 text-blue-600" />
                            ) : (
                              <Square className="h-5 w-5 text-gray-400" />
                            )}
                          </button>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {(currentPage - 1) * itemsPerPage + index + 1}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <div className="flex items-center">
                            <FileText className="h-5 w-5 text-gray-400 mr-2" />
                            <span className="text-sm font-medium text-gray-900">
                              {transcription.filename}
                            </span>
                          </div>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.language}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {new Date(transcription.created_at).toLocaleDateString()}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <span
                            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                              transcription.status === 'done'
                                ? 'bg-green-100 text-green-800'
                                : transcription.status === 'flagged'
                                ? 'bg-red-100 text-red-800'
                                : 'bg-yellow-100 text-yellow-800'
                            }`}
                          >
                            {transcription.status === 'done' ? 'Done' : transcription.status === 'flagged' ? 'Flagged' : 'Pending'}
                          </span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.transcription_type === 'words' && transcription.edited_words_count !== undefined
                            ? `${transcription.edited_words_count} / ${transcription.total_words || 0}`
                            : transcription.transcription_type === 'phrases'
                            ? 'N/A'
                            : '—'}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <span
                            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                              transcription.assigned_user_id
                                ? 'bg-green-100 text-green-800'
                                : 'bg-gray-100 text-gray-800'
                            }`}
                          >
                            {getUserName(transcription.assigned_user_id)}
                          </span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                          <div className="flex items-center gap-2">
                            <select
                              value={selectedUserId}
                              onChange={(e) => {
                                const userId = e.target.value;
                                setSelectedUserId(userId);
                                if (userId && !bulkAssigning) {
                                  // Only trigger individual assignment if not in bulk mode
                                  handleAssign(transcription._id, userId);
                                }
                              }}
                              disabled={assigning === transcription._id || bulkAssigning}
                              className="text-sm border border-gray-300 rounded px-2 py-1 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:opacity-50"
                            >
                              <option value="">Assign to...</option>
                              {users.map((user) => (
                                <option key={user._id} value={user._id}>
                                  {user.name || user.username}
                                </option>
                              ))}
                            </select>
                            {transcription.assigned_user_id && (
                              <button
                                onClick={() => {
                                  if (!bulkAssigning) {
                                    handleUnassign(transcription._id);
                                  }
                                }}
                                disabled={assigning === transcription._id || bulkAssigning}
                                className="text-red-600 hover:text-red-800 disabled:opacity-50"
                                title="Unassign"
                              >
                                {assigning === transcription._id ? (
                                  <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                  <UserX className="h-4 w-4" />
                                )}
                              </button>
                            )}
                            
                            <div className="relative">
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  if (transcription.is_flagged) {
                                    handleFlagTranscription(transcription._id, true);
                                  } else {
                                    if (showFlagDropdown === transcription._id) {
                                      setShowFlagDropdown(null);
                                    } else {
                                      const rect = e.currentTarget.getBoundingClientRect();
                                      setDropdownPosition({
                                        top: rect.bottom + 5,
                                        right: window.innerWidth - rect.right
                                      });
                                      setShowFlagDropdown(transcription._id);
                                    }
                                  }
                                }}
                                disabled={flagging === transcription._id}
                                className={`p-1 rounded-lg transition-colors flex items-center justify-center ${
                                  transcription.is_flagged
                                    ? 'text-red-600 hover:text-red-800 hover:bg-red-50'
                                    : 'text-gray-400 hover:text-gray-600 hover:bg-gray-100'
                                }`}
                                title={transcription.is_flagged ? "Unflag transcription" : "Flag transcription"}
                              >
                                {flagging === transcription._id ? (
                                  <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                  <Flag className={`h-4 w-4 ${transcription.is_flagged ? 'fill-current' : ''}`} />
                                )}
                              </button>
                            </div>

                            <button
                              onClick={() => handleDeleteTranscription(transcription._id)}
                              disabled={deleting === transcription._id}
                              className="text-red-600 hover:text-red-800 disabled:opacity-50 p-1"
                              title="Delete transcription"
                            >
                              {deleting === transcription._id ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <Trash2 className="h-4 w-4" />
                              )}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}

          {/* Pagination Controls */}
          {totalItems > itemsPerPage && (
            <div className="mt-6 flex items-center justify-between border-t border-gray-200 pt-4">
              <div className="flex items-center text-sm text-gray-700">
                Showing {startIndex + 1} to {Math.min(endIndex, totalItems)} of {totalItems} transcriptions
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                  disabled={currentPage === 1 || loading}
                  className="flex items-center gap-1 px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <ChevronLeft className="h-4 w-4" />
                  Previous
                </button>
                
                <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
                    let page: number;
                    if (totalPages <= 7) {
                      page = i + 1;
                    } else {
                      const totalPages = Math.ceil(totalItems / itemsPerPage);
                      if (currentPage <= 3) {
                        page = i + 1;
                      } else if (currentPage >= totalPages - 2) {
                        page = totalPages - 6 + i;
                      } else {
                        page = currentPage - 3 + i;
                      }
                    }
                    
                    return (
                      <button
                        key={page}
                        onClick={() => setCurrentPage(page)}
                        className={`px-3 py-2 text-sm font-medium rounded-lg ${
                          currentPage === page
                            ? 'bg-blue-600 text-white'
                            : 'text-gray-700 bg-white border border-gray-300 hover:bg-gray-50'
                        }`}
                      >
                        {page}
                      </button>
                    );
                  })}
                </div>

                <button
                  onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                  disabled={currentPage >= totalPages || loading}
                  className="flex items-center gap-1 px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Fixed Flag Dropdown */}
      {showFlagDropdown && dropdownPosition && (
        <>
          <div className="fixed inset-0 z-[100]" onClick={() => setShowFlagDropdown(null)}></div>
          <div
            className="fixed w-64 bg-white rounded-lg shadow-xl border border-gray-200 z-[101] overflow-hidden"
            style={{
              top: dropdownPosition.top,
              right: dropdownPosition.right,
            }}
          >
            <div className="p-2 border-b border-gray-100 bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wider">
              Select Reason
            </div>
            <div className="py-1">
              {[
                "Transcribed Word not seperated",
                "Transcribed words repeated",
                "Missing transcribed words"
              ].map((reason) => (
                <button
                  key={reason}
                  onClick={(e) => {
                    e.stopPropagation();
                    handleFlagTranscription(showFlagDropdown, false, reason);
                  }}
                  className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 hover:text-gray-900"
                >
                  {reason}
                </button>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default AdminPanel;

