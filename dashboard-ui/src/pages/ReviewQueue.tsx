import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import api from '../api';
import { Eye, Clock, Check, X, AlertTriangle } from 'lucide-react';

const ReviewQueue = () => {
  const [jobs, setJobs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [statusFilter, setStatusFilter] = useState('pending');
  const navigate = useNavigate();

  const fetchJobs = () => {
    setLoading(true);
    api.get('/jobs', {
      params: { page, limit: 15, status: statusFilter === 'all' ? undefined : statusFilter }
    }).then(res => {
      setJobs(res.data.data);
      setTotal(res.data.pagination.total);
    }).finally(() => {
      setLoading(false);
    });
  };

  useEffect(() => {
    fetchJobs();
  }, [page, statusFilter]);

  const renderStatus = (status: string) => {
    if (!status) return <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-gray-100 text-gray-800"><Clock className="w-3 h-3 mr-1" /> Pending</span>;
    if (status === 'relevant') return <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800"><Check className="w-3 h-3 mr-1" /> Relevant</span>;
    if (status === 'irrelevant') return <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-red-100 text-red-800"><X className="w-3 h-3 mr-1" /> Irrelevant</span>;
    return <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-yellow-100 text-yellow-800"><AlertTriangle className="w-3 h-3 mr-1" /> Needs Review</span>;
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h2 className="text-xl font-bold text-gray-900">Review Queue</h2>
        <div className="flex space-x-2">
          <select 
            className="border-gray-300 rounded-md shadow-sm text-sm focus:ring-blue-500 focus:border-blue-500 p-2 border"
            value={statusFilter}
            onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
          >
            <option value="pending">Pending Review</option>
            <option value="relevant">Relevant</option>
            <option value="irrelevant">Irrelevant</option>
            <option value="needs_review">Needs Review</option>
            <option value="all">All Statuses</option>
          </select>
        </div>
      </div>

      <div className="bg-white shadow overflow-hidden sm:rounded-md border border-gray-200">
        {loading ? (
          <div className="p-8 text-center text-gray-500">Loading jobs...</div>
        ) : jobs.length === 0 ? (
          <div className="p-8 text-center text-gray-500">No jobs found.</div>
        ) : (
          <ul className="divide-y divide-gray-200">
            {jobs.map((job) => (
              <li key={job.id}>
                <div className="px-4 py-4 sm:px-6 hover:bg-gray-50 flex items-center justify-between cursor-pointer" onClick={() => navigate(`/queue/${job.id}`)}>
                  <div className="flex-1 min-w-0 pr-4">
                    <div className="flex items-center justify-between">
                      <p className="text-sm font-medium text-blue-600 truncate">{job.title}</p>
                      <div className="ml-2 flex-shrink-0 flex">
                        {renderStatus(job.review_status)}
                      </div>
                    </div>
                    <div className="mt-2 sm:flex sm:justify-between">
                      <div className="sm:flex text-sm text-gray-500">
                        <p className="flex items-center truncate">
                          {job.company} &mdash; {job.location}
                        </p>
                      </div>
                      <div className="mt-2 flex items-center text-sm text-gray-500 sm:mt-0">
                        <p>AWL: {job.applywizz_id}</p>
                        <span className="mx-2">&bull;</span>
                        <p>
                          Scraped: {new Date(job.scraped_at).toLocaleDateString()}
                        </p>
                      </div>
                    </div>
                  </div>
                  <div>
                    <button className="text-gray-400 hover:text-blue-500">
                      <Eye className="w-5 h-5" />
                    </button>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
      
      {/* Pagination */}
      <div className="flex items-center justify-between mt-4">
        <div className="text-sm text-gray-700">
          Showing <span className="font-medium">{(page - 1) * 15 + 1}</span> to <span className="font-medium">{Math.min(page * 15, total)}</span> of <span className="font-medium">{total}</span> results
        </div>
        <div className="flex space-x-2">
          <button 
            disabled={page === 1}
            onClick={() => setPage(p => Math.max(1, p - 1))}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            Previous
          </button>
          <button 
            disabled={page * 15 >= total}
            onClick={() => setPage(p => p + 1)}
            className="px-3 py-1 border rounded text-sm disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
};

export default ReviewQueue;
