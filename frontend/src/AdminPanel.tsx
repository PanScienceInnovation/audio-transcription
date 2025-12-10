import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { Users, FileText, UserCheck, UserX, Loader2, Search, X, ChevronLeft, ChevronRight, ArrowLeft, CheckSquare, Square, Download, Trash2, Flag, Eye, MessageSquare, RefreshCw } from 'lucide-react';

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
  updated_at?: string;
  language: string;
  assigned_user_id?: string;
  user_id?: string;
  status?: 'done' | 'pending' | 'flagged' | 'completed' | 'assigned_for_review';
  review_round?: number;
  review_history?: Array<{
    round: number;
    user_id: string | null;
    action: string;
    previous_status: string;
    new_status: string;
    timestamp: string;
    previous_assigned_user_id?: string;
    new_assigned_user_id?: string;
  }>;
  edited_words_count?: number;
  review_round_edited_words_count?: number;
  total_words?: number;
  transcription_type?: 'words' | 'phrases';
  is_flagged?: boolean;
  audio_duration?: number;
  remarks?: string;
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
  const [totalItems, setTotalItems] = useState(0); // Total items from backend
  const [statistics, setStatistics] = useState({ total: 0, done: 0, pending: 0, flagged: 0, completed: 0, double_flagged: 0, reprocessed: 0, total_done_duration: 0, total_completed_duration: 0 }); // Statistics from backend
  const [selectedTranscriptions, setSelectedTranscriptions] = useState<Set<string>>(new Set());
  const [bulkAssignUserId, setBulkAssignUserId] = useState<string>('');
  const [bulkAssigning, setBulkAssigning] = useState(false);
  const [bulkReassignUserId, setBulkReassignUserId] = useState<string>('');
  const [bulkReassigning, setBulkReassigning] = useState(false);
  const [languageFilter, setLanguageFilter] = useState<string>('');
  const [dateFilter, setDateFilter] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<string>('');
  const [assignedUserFilter, setAssignedUserFilter] = useState<string>('');
  const [originalAssigneeFilter, setOriginalAssigneeFilter] = useState<string>('');
  const [flaggedFilter, setFlaggedFilter] = useState<string>('');
  const [downloading, setDownloading] = useState(false);
  const [downloadingCompleted, setDownloadingCompleted] = useState(false);
  const [downloadingSelectedCompleted, setDownloadingSelectedCompleted] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [flagging, setFlagging] = useState<string | null>(null);
  const [showFlagDropdown, setShowFlagDropdown] = useState<string | null>(null);
  const [dropdownPosition, setDropdownPosition] = useState<{ top: number; right: number } | null>(null);
  const [updatingStatus, setUpdatingStatus] = useState<string | null>(null);
  const [remarksModal, setRemarksModal] = useState<{ isOpen: boolean; transcriptionId: string | null; remarks: string }>({
    isOpen: false,
    transcriptionId: null,
    remarks: ''
  });
  const [savingRemarks, setSavingRemarks] = useState(false);

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
  }, [searchTerm, languageFilter, dateFilter, statusFilter, assignedUserFilter, originalAssigneeFilter, flaggedFilter]);

  // Reload data when page changes or filters change
  useEffect(() => {
    if (isAdmin) {
      loadData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentPage, searchTerm, languageFilter, dateFilter, statusFilter, assignedUserFilter, originalAssigneeFilter, flaggedFilter, isAdmin]);

  const loadData = async () => {
    setLoading(true);
    try {
      const config = getAxiosConfig();

      // Load users
      const usersResponse = await axios.get(`${API_BASE_URL}/api/admin/users`, config);
      if (usersResponse.data.success) {
        setUsers(usersResponse.data.users.filter((u: User) => !u.is_admin)); // Exclude admins from assignment list
      }

      // Load statistics (overall counts, not filtered)
      const statsResponse = await axios.get(`${API_BASE_URL}/api/transcriptions/statistics`, config);
      if (statsResponse.data.success) {
        setStatistics(statsResponse.data.data);
      }

      // Build query parameters for transcriptions
      const params = new URLSearchParams();
      params.append('limit', itemsPerPage.toString());
      params.append('skip', ((currentPage - 1) * itemsPerPage).toString());
      
      if (searchTerm) params.append('search', searchTerm);
      if (languageFilter) params.append('language', languageFilter);
      if (dateFilter) params.append('date', dateFilter);
      if (statusFilter) params.append('status', statusFilter);
      if (assignedUserFilter) params.append('assigned_user', assignedUserFilter);
      if (originalAssigneeFilter) params.append('original_assignee', originalAssigneeFilter);
      if (flaggedFilter) params.append('flagged', flaggedFilter);

      // Load transcriptions with pagination and filters
      const transcriptionsResponse = await axios.get(`${API_BASE_URL}/api/transcriptions?${params.toString()}`, config);
      if (transcriptionsResponse.data.success) {
        setTranscriptions(transcriptionsResponse.data.data.transcriptions);
        // Store total count for pagination
        setTotalItems(transcriptionsResponse.data.data.total || 0);
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

  const handleReassign = async (transcriptionId: string, newUserId: string) => {
    setAssigning(transcriptionId);
    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/files/${transcriptionId}/reassign`,
        { new_user_id: newUserId },
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'File reassigned successfully for second review' });
        loadData(); // Reload data
      }
    } catch (error: any) {
      const errorMsg = error.response?.data?.error || 'Failed to reassign file';
      setMessage({ type: 'error', text: errorMsg });
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

    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/bulk-assign`,
        {
          transcription_ids: selectedIds,
          assigned_user_id: bulkAssignUserId
        },
        config
      );

      if (response.data.success) {
        const summary = response.data.summary;
        const message = `Bulk assign completed: ${summary.total_successful} successful, ${summary.total_failed} failed`;
        setMessage({ type: 'success', text: message });
        setSelectedTranscriptions(new Set()); // Clear selection
        setBulkAssignUserId(''); // Clear user selection
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to assign transcriptions' });
      }
    } catch (error: any) {
      console.error('❌ Error in bulk assignment:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to assign transcriptions' });
    } finally {
      setBulkAssigning(false);
    }
  };

  const handleBulkReassign = async () => {
    if (!bulkReassignUserId || selectedTranscriptions.size === 0) {
      setMessage({ type: 'error', text: 'Please select files and a user to reassign' });
      return;
    }

    setBulkReassigning(true);
    const selectedIds = Array.from(selectedTranscriptions);
    console.log(`🔄 Bulk reassigning ${selectedIds.length} transcriptions to user ${bulkReassignUserId}`);

    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/bulk-reassign`,
        {
          transcription_ids: selectedIds,
          new_user_id: bulkReassignUserId
        },
        config
      );

      if (response.data.success) {
        const summary = response.data.summary;
        const message = `Bulk reassign completed: ${summary.total_successful} successful, ${summary.total_failed} failed`;
        setMessage({ type: 'success', text: message });
        setSelectedTranscriptions(new Set()); // Clear selection
        setBulkReassignUserId(''); // Clear user selection
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to reassign transcriptions' });
      }
    } catch (error: any) {
      console.error('❌ Error in bulk reassignment:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to reassign transcriptions' });
    } finally {
      setBulkReassigning(false);
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

  const handleDownloadCompletedTranscriptions = async () => {
    setDownloadingCompleted(true);
    try {
      const config = getAxiosConfig();

      // Make request to download endpoint with responseType: 'blob' for binary data
      const response = await axios.get(
        `${API_BASE_URL}/api/admin/transcriptions/download-completed`,
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
      let filename = 'completed_transcriptions.zip';
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
      console.error('❌ Error downloading completed transcriptions:', error);
      const errorMessage = error.response?.data?.error || error.message || 'Failed to download completed transcriptions';
      setMessage({ type: 'error', text: errorMessage });
    } finally {
      setDownloadingCompleted(false);
    }
  };

  const handleDownloadSelectedCompleted = async () => {
    if (selectedTranscriptions.size === 0) {
      setMessage({ type: 'error', text: 'Please select at least one file to download' });
      return;
    }

    setDownloadingSelectedCompleted(true);
    try {
      const config = getAxiosConfig();
      const selectedIds = Array.from(selectedTranscriptions);

      // Make request to download endpoint with responseType: 'blob' for binary data
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/download-selected-completed`,
        { transcription_ids: selectedIds },
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
      let filename = 'selected_completed_transcriptions.zip';
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

      setMessage({ type: 'success', text: `Downloaded ${selectedIds.length} selected completed file(s)` });
    } catch (error: any) {
      console.error('❌ Error downloading selected completed transcriptions:', error);
      const errorMessage = error.response?.data?.error || error.message || 'Failed to download selected completed transcriptions';
      setMessage({ type: 'error', text: errorMessage });
    } finally {
      setDownloadingSelectedCompleted(false);
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

  const handleBulkDelete = async () => {
    if (selectedTranscriptions.size === 0) {
      setMessage({ type: 'error', text: 'Please select at least one transcription to delete' });
      return;
    }

    const selectedIds = Array.from(selectedTranscriptions);
    const confirmed = window.confirm(
      `Are you sure you want to delete ${selectedIds.length} transcription(s)?\n\n` +
      'This action cannot be undone. The following will be permanently deleted:\n' +
      '• Transcription data from database\n' +
      '• Audio files from S3 storage\n\n' +
      'This action is irreversible.'
    );

    if (!confirmed) {
      return;
    }

    setBulkDeleting(true);
    try {
      const config = getAxiosConfig();
      const response = await axios.post(
        `${API_BASE_URL}/api/admin/transcriptions/bulk-delete`,
        { transcription_ids: selectedIds },
        config
      );

      if (response.data.success) {
        const summary = response.data.summary;
        const message = `Bulk delete completed: ${summary.total_successful} successful, ${summary.total_failed} failed`;
        setMessage({ type: 'success', text: message });
        setSelectedTranscriptions(new Set()); // Clear selection
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to delete transcriptions' });
      }
    } catch (error: any) {
      console.error('❌ Error in bulk delete:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to delete transcriptions' });
    } finally {
      setBulkDeleting(false);
    }
  };

  const handleStatusChange = async (transcriptionId: string, newStatus: 'done' | 'pending' | 'flagged' | 'completed' | 'assigned_for_review') => {
    setUpdatingStatus(transcriptionId);
    try {
      const config = getAxiosConfig();

      const response = await axios.put(
        `${API_BASE_URL}/api/admin/transcriptions/${transcriptionId}/status`,
        { status: newStatus },
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'Status updated successfully' });
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to update status' });
      }
    } catch (error: any) {
      console.error('Error updating status:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to update status' });
    } finally {
      setUpdatingStatus(null);
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

  const handleSaveRemarks = async () => {
    if (!remarksModal.transcriptionId) return;
    
    setSavingRemarks(true);
    try {
      const config = getAxiosConfig();
      const response = await axios.put(
        `${API_BASE_URL}/api/admin/transcriptions/${remarksModal.transcriptionId}/remarks`,
        { remarks: remarksModal.remarks },
        config
      );

      if (response.data.success) {
        setMessage({ type: 'success', text: 'Remarks updated successfully' });
        setRemarksModal({ isOpen: false, transcriptionId: null, remarks: '' });
        loadData(); // Reload data
      } else {
        setMessage({ type: 'error', text: response.data.error || 'Failed to update remarks' });
      }
    } catch (error: any) {
      console.error('Error updating remarks:', error);
      setMessage({ type: 'error', text: error.response?.data?.error || 'Failed to update remarks' });
    } finally {
      setSavingRemarks(false);
    }
  };

  const getUserName = (userId: string | undefined) => {
    if (!userId) return 'Unassigned';
    const user = users.find(u => u._id === userId);
    return user ? user.name || user.username : userId;
  };

  const getOriginalAssignee = (transcription: Transcription): string | undefined => {
    // If there's no review_history, current assigned_user_id is the original
    if (!transcription.review_history || transcription.review_history.length === 0) {
      return transcription.assigned_user_id;
    }
    
    // Find the first "reassign" action in review_history to get the original assignee
    const reassignAction = transcription.review_history.find(
      (entry) => entry.action === 'reassign'
    );
    
    if (reassignAction && reassignAction.previous_assigned_user_id) {
      return reassignAction.previous_assigned_user_id;
    }
    
    // If no reassign action found, current assigned_user_id is the original
    return transcription.assigned_user_id;
  };

  const handleLoadTranscription = (filename: string) => {
    const encodedFilename = encodeURIComponent(filename);
    navigate(`/word-level/transcription/${encodedFilename}?from=admin`);
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

  // Total duration is now fetched from backend statistics
  const totalDoneDuration = statistics.total_done_duration || 0;
  const totalCompletedDuration = statistics.total_completed_duration || 0;

  // Get unique languages for filter dropdown (from all transcriptions, not just current page)
  // Note: This might need to be loaded separately or cached
  const uniqueLanguages = Array.from(new Set(transcriptions.map(t => t.language).filter(Boolean))).sort();

  // Transcriptions are already filtered, sorted, and paginated by the backend
  // When filtering by 'done', the backend ensures actual 'done' files appear before 'assigned_for_review' files
  const paginatedTranscriptions = transcriptions;
  
  // Calculate pagination info
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = Math.min(startIndex + paginatedTranscriptions.length, totalItems);

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
                onClick={() => navigate('/word-level')}
                className="flex items-center gap-2 px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
                title="Go back to Word-Level Transcription"
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
            <div className="flex items-center gap-3">
              <button
                onClick={() => navigate('/admin/teams')}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                title="View team management"
              >
                <Users className="h-5 w-5" />
                <span className="hidden sm:inline">Team Management</span>
              </button>
              <button
                onClick={handleDownloadDoneTranscriptions}
                disabled={downloading || downloadingCompleted || downloadingSelectedCompleted}
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
              <button
                onClick={handleDownloadCompletedTranscriptions}
                disabled={downloadingCompleted || downloading || downloadingSelectedCompleted}
                className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                title="Download all completed transcriptions as ZIP"
              >
                {downloadingCompleted ? (
                  <>
                    <Loader2 className="h-5 w-5 animate-spin" />
                    <span className="hidden sm:inline">Downloading...</span>
                  </>
                ) : (
                  <>
                    <Download className="h-5 w-5" />
                    <span className="hidden sm:inline">Download All Completed Files</span>
                  </>
                )}
              </button>
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

          {/* Total Duration Stats */}
          <div className="mb-6 ml-4 bg-transparent border-none">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-medium text-blue-900 mb-1">Total Duration of Done Files: {formatDuration(totalDoneDuration)}</h3>
                <h3 className="text-sm font-medium text-blue-900 mb-1">Total Duration of Completed Files: {formatDuration(totalCompletedDuration)}</h3>
              </div>
            </div>
          </div>

          {/* Insight Cards */}
          {/* Note: Stats shown are for the current filtered view, not all transcriptions */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4 mb-6">
            {/* Total Audio Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-blue-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Total Audio Files</p>
                  <p className="text-3xl font-bold text-gray-800">{statistics.total}</p>
                </div>
                <div className="bg-blue-100 rounded-full p-3">
                  <FileText className="h-8 w-8 text-blue-600" />
                </div>
              </div>
            </div>

            {/* Total Files Annotated Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-green-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Done Files</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.done}
                  </p>
                </div>
                <div className="bg-green-100 rounded-full p-3">
                  <CheckSquare className="h-8 w-8 text-green-600" />
                </div>
              </div>
            </div>

            {/* Completed Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-purple-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Completed Files</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.completed || 0}
                  </p>
                </div>
                <div className="bg-purple-100 rounded-full p-3">
                  <CheckSquare className="h-8 w-8 text-purple-600" />
                </div>
              </div>
            </div>

            {/* Pending Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-yellow-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Pending Files</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.pending}
                  </p>
                </div>
                <div className="bg-yellow-100 rounded-full p-3">
                  <Loader2 className="h-8 w-8 text-yellow-600" />
                </div>
              </div>
            </div>

            {/* Flagged Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-red-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Flagged Files</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.flagged}
                  </p>
                </div>
                <div className="bg-red-100 rounded-full p-3">
                  <Flag className="h-8 w-8 text-red-600" />
                </div>
              </div>
            </div>

            {/* Double Flagged Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-orange-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Double Flagged</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.double_flagged || 0}
                  </p>
                </div>
                <div className="bg-orange-100 rounded-full p-3">
                  <Flag className="h-8 w-8 text-orange-600 fill-current" />
                </div>
              </div>
            </div>

            {/* Reprocessed Files Card */}
            <div className="bg-white rounded-lg shadow-lg p-6 border-l-4 border-blue-500">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium text-gray-600 mb-1">Reprocessed</p>
                  <p className="text-3xl font-bold text-gray-800">
                    {statistics.reprocessed || 0}
                  </p>
                </div>
                <div className="bg-blue-100 rounded-full p-3">
                  <RefreshCw className="h-8 w-8 text-blue-600" />
                </div>
              </div>
            </div>
          </div>

          <div className="mb-6 space-y-4">
            {/* Search and Filter Controls */}
            <div className="bg-transparent border-none">
              <div className="mb-3">
                <h3 className="text-sm font-semibold text-gray-700 mb-3">Filters</h3>
              </div>

              {/* First Row: Search and Basic Filters */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4 mb-4">
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
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                >
                  <option value="">All Status</option>
                  <option value="done">Done</option>
                  <option value="completed">Completed</option>
                  <option value="pending">Pending</option>
                  {/* <option value="assigned_for_review">Assigned for Review</option> */}
                  <option value="flagged">Flagged</option>
                  <option value="double_flagged">Double Flagged</option>
                  <option value="reprocessed">Reprocessed</option>
                </select>
                <select
                  value={languageFilter}
                  onChange={(e) => setLanguageFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                >
                  <option value="">All Languages</option>
                  {uniqueLanguages.map((lang) => (
                    <option key={lang} value={lang}>
                      {lang}
                    </option>
                  ))}
                </select>
                <select
                  value={assignedUserFilter}
                  onChange={(e) => setAssignedUserFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                  title="Filter by current assignee"
                >
                  <option value="">Current Assignee: All</option>
                  <option value="unassigned">Current Assignee: Unassigned</option>
                  {users.map((user) => (
                    <option key={user._id} value={user._id}>
                      Current: {user.name || user.username}
                    </option>
                  ))}
                </select>
                <select
                  value={originalAssigneeFilter}
                  onChange={(e) => setOriginalAssigneeFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                  title="Filter by original assignee"
                >
                  <option value="">Original Assignee: All</option>
                  <option value="unassigned">Original Assignee: Unassigned</option>
                  {users.map((user) => (
                    <option key={user._id} value={user._id}>
                      Original: {user.name || user.username}
                    </option>
                  ))}
                </select>
                <div className="relative">
                  <input
                    type="date"
                    value={dateFilter}
                    onChange={(e) => setDateFilter(e.target.value)}
                    className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white w-full"
                    placeholder="Filter by date"
                    title={statusFilter === 'done' 
                      ? "Filter by updated date (when file was marked as done)" 
                      : "Filter by created date"}
                  />
                  {statusFilter === 'done' && dateFilter && (
                    <span className="absolute -bottom-5 left-0 text-xs text-gray-500 whitespace-nowrap">
                      Filtering by updated date
                    </span>
                  )}
                </div>
              </div>

              {/* Second Row: Clear Filters Button */}
              <div className={`grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 ${statusFilter === 'done' && dateFilter ? 'mb-6' : 'mb-4'}`}>
                {/* <select
                  value={assignedUserFilter}
                  onChange={(e) => setAssignedUserFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                >
                  <option value="">All Users</option>
                  <option value="unassigned">Unassigned</option>
                  {users.map((user) => (
                    <option key={user._id} value={user._id}>
                      {user.name || user.username}
                    </option>
                  ))}
                </select> */}
                {/* <select
                  value={flaggedFilter}
                  onChange={(e) => setFlaggedFilter(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
                >
                  <option value="">All Files</option>
                  <option value="flagged">Flagged Only</option>
                  <option value="not-flagged">Not Flagged</option>
                </select> */}
                {/* <div className="relative">
                  <input
                    type="date"
                    value={dateFilter}
                    onChange={(e) => setDateFilter(e.target.value)}
                    className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white w-full"
                    placeholder="Filter by date"
                    title={statusFilter === 'done' 
                      ? "Filter by updated date (when file was marked as done)" 
                      : "Filter by created date"}
                  />
                  {statusFilter === 'done' && dateFilter && (
                    <span className="absolute -bottom-5 left-0 text-xs text-gray-500 whitespace-nowrap">
                      Filtering by updated date
                    </span>
                  )}
                </div> */}
                {/* Clear Filters Button */}
                {(searchTerm || languageFilter || dateFilter || statusFilter || assignedUserFilter || originalAssigneeFilter) && (
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => {
                        setSearchTerm('');
                        setLanguageFilter('');
                        setDateFilter('');
                        setStatusFilter('');
                        setAssignedUserFilter('');
                        setOriginalAssigneeFilter('');
                        setFlaggedFilter('');
                      }}
                      className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1 px-3 py-1.5 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
                    >
                      <X className="h-4 w-4" />
                      Clear All Filters
                    </button>
                    <span className="text-xs text-gray-500">
                      {totalItems} file{totalItems !== 1 ? 's' : ''} total
                    </span>
                  </div>
                )}
              </div>
            </div>

            {/* Bulk Assignment Controls */}
            {selectedTranscriptions.size > 0 && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 flex items-center justify-between flex-wrap gap-4">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-blue-900">
                    {selectedTranscriptions.size} file{selectedTranscriptions.size > 1 ? 's' : ''} selected
                  </span>
                </div>
                <div className="flex items-center gap-2 flex-wrap">
                  <button
                    onClick={handleDownloadSelectedCompleted}
                    disabled={downloadingSelectedCompleted || downloading || downloadingCompleted || bulkAssigning || bulkReassigning || bulkDeleting}
                    className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    title="Download selected completed files as ZIP"
                  >
                    {downloadingSelectedCompleted ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Downloading...
                      </>
                    ) : (
                      <>
                        <Download className="h-4 w-4" />
                        Download Selected Completed
                      </>
                    )}
                  </button>
                  <select
                    value={bulkAssignUserId}
                    onChange={(e) => setBulkAssignUserId(e.target.value)}
                    disabled={bulkAssigning || bulkReassigning || bulkDeleting}
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
                    disabled={!bulkAssignUserId || bulkAssigning || bulkReassigning || bulkDeleting}
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
                  <select
                    value={bulkReassignUserId}
                    onChange={(e) => setBulkReassignUserId(e.target.value)}
                    disabled={bulkAssigning || bulkReassigning || bulkDeleting}
                    className="text-sm border border-gray-300 rounded px-3 py-2 focus:ring-2 focus:ring-purple-500 focus:border-purple-500 disabled:opacity-50"
                  >
                    <option value="">Select user to reassign...</option>
                    {users.map((user) => (
                      <option key={user._id} value={user._id}>
                        {user.name || user.username}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={handleBulkReassign}
                    disabled={!bulkReassignUserId || bulkAssigning || bulkReassigning || bulkDeleting}
                    className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    title="Reassign selected files for second review"
                  >
                    {bulkReassigning ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Reassigning...
                      </>
                    ) : (
                      <>
                        <UserCheck className="h-4 w-4" />
                        Reassign Selected
                      </>
                    )}
                  </button>
                  <button
                    onClick={handleBulkUnassign}
                    disabled={bulkAssigning || bulkReassigning || bulkDeleting}
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
                    onClick={handleBulkDelete}
                    disabled={bulkDeleting || bulkAssigning}
                    className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {bulkDeleting ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Deleting...
                      </>
                    ) : (
                      <>
                        <Trash2 className="h-4 w-4" />
                        Delete Selected
                      </>
                    )}
                  </button>
                  <button
                    onClick={() => {
                      setSelectedTranscriptions(new Set());
                      setBulkAssignUserId('');
                      setBulkReassignUserId('');
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
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider w-16 pointer-events-none">
                      S.No.
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Filename
                    </th>
                    {/* <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Language
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Created at
                    </th> */}
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Updated at
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Status
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Edited Words
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Edited Words in Review Round
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Current Assignee
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Original Assignee
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-700 uppercase tracking-wider pointer-events-none">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {paginatedTranscriptions.length === 0 ? (
                    <tr>
                      <td colSpan={12} className="px-4 py-8 text-center text-gray-500">
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
                          <div className="flex items-center gap-2">
                            {/* <FileText className="h-5 w-5 text-gray-400 mr-2" /> */}
                            <button
                              onClick={() => handleLoadTranscription(transcription.filename)}
                              className="text-sm font-medium text-gray-900 hover:text-blue-600 hover:underline transition-colors"
                              title="Load transcription"
                            >
                              {transcription.filename}
                            </button>
                            {(transcription as any).is_double_flagged && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-800 border border-orange-300">
                                ⚠️ Double Flagged
                              </span>
                            )}
                            {(transcription as any).has_been_reprocessed && !(transcription as any).is_double_flagged && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                                🔄 Reprocessed
                              </span>
                            )}
                          </div>
                        </td>
                        {/* <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.language}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {new Date(transcription.created_at).toLocaleDateString()}
                        </td> */}
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.updated_at 
                            ? new Date(transcription.updated_at).toLocaleDateString()
                            : '—'}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          {transcription.status === 'assigned_for_review' ? (
                            <span className="px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                              Assigned for Review
                            </span>
                          ) : (transcription as any).is_double_flagged ? (
                            <span className="px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-900 border border-orange-300">
                              Double Flagged
                            </span>
                          ) : (
                            <>
                              <select
                                value={transcription.status || 'pending'}
                                onChange={(e) => handleStatusChange(transcription._id, e.target.value as 'done' | 'pending' | 'flagged' | 'completed' | 'assigned_for_review')}
                                disabled={updatingStatus === transcription._id}
                                className={`px-2.5 py-0.5 rounded-full text-xs font-medium border-0 cursor-pointer focus:ring-2 focus:ring-blue-500 ${
                                  transcription.status === 'completed'
                                    ? 'bg-purple-100 text-purple-800'
                                    : transcription.status === 'done'
                                    ? 'bg-green-100 text-green-800'
                                    : transcription.status === 'flagged'
                                      ? ((transcription as any).has_been_reprocessed ? 'bg-orange-100 text-orange-800' : 'bg-red-100 text-red-800')
                                      : 'bg-yellow-100 text-yellow-800'
                                } ${updatingStatus === transcription._id ? 'opacity-50 cursor-not-allowed' : ''}`}
                              >
                                <option value="pending">Pending</option>
                                <option value="done">Done</option>
                                <option value="completed">Completed</option>
                                <option value="flagged">
                                  {(transcription as any).has_been_reprocessed ? 'Flagged (Reprocessed)' : 'Flagged'}
                                </option>
                              </select>
                              {updatingStatus === transcription._id && (
                                <Loader2 className="inline-block h-3 w-3 ml-2 animate-spin text-gray-500" />
                              )}
                            </>
                          )}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.transcription_type === 'words' && transcription.edited_words_count !== undefined
                            ? `${transcription.edited_words_count} / ${transcription.total_words || 0}`
                            : transcription.transcription_type === 'phrases'
                              ? 'N/A'
                              : '—'}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                          {transcription.transcription_type === 'words' && transcription.review_round_edited_words_count !== undefined
                            ? `${transcription.review_round_edited_words_count} / ${transcription.total_words || 0}`
                            : transcription.transcription_type === 'phrases'
                              ? 'N/A'
                              : '—'}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <span
                            className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${transcription.assigned_user_id
                                ? 'bg-green-100 text-green-800'
                                : 'bg-gray-100 text-gray-800'
                              }`}
                          >
                            {getUserName(transcription.assigned_user_id)}
                          </span>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          {(() => {
                            const originalAssigneeId = getOriginalAssignee(transcription);
                            const currentAssigneeId = transcription.assigned_user_id;
                            const wasReassigned = originalAssigneeId && currentAssigneeId && originalAssigneeId !== currentAssigneeId;
                            
                            return (
                              <span
                                className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                                  originalAssigneeId
                                    ? wasReassigned 
                                      ? 'bg-blue-100 text-blue-800'
                                      : 'bg-green-100 text-green-800'
                                    : 'bg-gray-100 text-gray-800'
                                }`}
                                title={wasReassigned ? 'Original assignee (file was reassigned)' : 'Original assignee'}
                              >
                                {getUserName(originalAssigneeId)}
                              </span>
                            );
                          })()}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => handleLoadTranscription(transcription.filename)}
                              className="text-blue-600 hover:text-blue-800 disabled:opacity-50 p-1"
                              title="Load transcription in detailed view"
                            >
                              <Eye className="h-4 w-4" />
                            </button>
                            <button
                              onClick={() => setRemarksModal({
                                isOpen: true,
                                transcriptionId: transcription._id,
                                remarks: transcription.remarks || ''
                              })}
                              className={`p-1 rounded-lg transition-colors ${transcription.remarks ? 'text-blue-600 hover:text-blue-800 bg-blue-50' : 'text-gray-400 hover:text-gray-600 hover:bg-gray-100'}`}
                              title={transcription.remarks ? "Edit remarks" : "Add remarks"}
                            >
                              <MessageSquare className="h-4 w-4" />
                            </button>
                            {/* Reassign button - show for files with status "done" that can be reassigned for second review */}
                            {transcription.status === 'done' && transcription.review_round !== 1 && (
                              <select
                                value=""
                                onChange={(e) => {
                                  const newUserId = e.target.value;
                                  if (newUserId && newUserId !== transcription.assigned_user_id) {
                                    handleReassign(transcription._id, newUserId);
                                  }
                                  e.target.value = ''; // Reset selection
                                }}
                                disabled={assigning === transcription._id || bulkAssigning}
                                className="text-xs border border-blue-300 rounded px-2 py-1 bg-blue-50 text-blue-700 hover:bg-blue-100 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                                title="Reassign for second review"
                              >
                                <option value="">Reassign...</option>
                                {users.filter(user => user._id !== transcription.assigned_user_id).map((user) => (
                                  <option key={user._id} value={user._id}>
                                    {user.name || user.username}
                                  </option>
                                ))}
                              </select>
                            )}
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
                                className={`p-1 rounded-lg transition-colors flex items-center justify-center ${transcription.is_flagged
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
                        className={`px-3 py-2 text-sm font-medium rounded-lg ${currentPage === page
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
                "Missing transcribed words",
                "Audio file not found",
                "Transcription not found"
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

      {/* Remarks Modal */}
      {remarksModal.isOpen && (
        <>
          <div className="fixed inset-0 bg-black bg-opacity-50 z-[100]" onClick={() => !savingRemarks && setRemarksModal({ ...remarksModal, isOpen: false })}></div>
          <div className="fixed top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 bg-white rounded-lg shadow-xl border border-gray-200 z-[101] w-96 p-6">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold text-gray-800">Transcription Remarks</h3>
              <button 
                onClick={() => !savingRemarks && setRemarksModal({ ...remarksModal, isOpen: false })}
                className="text-gray-500 hover:text-gray-700"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <textarea
              value={remarksModal.remarks}
              onChange={(e) => setRemarksModal({ ...remarksModal, remarks: e.target.value })}
              className="w-full h-32 p-2 border border-gray-300 rounded-lg mb-4 focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none"
              placeholder="Enter remarks here..."
              disabled={savingRemarks}
            />
            <div className="flex justify-end gap-2">
              <button
                onClick={() => setRemarksModal({ ...remarksModal, isOpen: false })}
                className="px-4 py-2 text-gray-600 hover:bg-gray-100 rounded-lg"
                disabled={savingRemarks}
              >
                Cancel
              </button>
              <button
                onClick={handleSaveRemarks}
                disabled={savingRemarks}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
              >
                {savingRemarks ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  'Save Remarks'
                )}
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default AdminPanel;
