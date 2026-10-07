import React, { useEffect, useState } from 'react';
import api from '../api';
import { Activity, CheckCircle, XCircle, AlertCircle } from 'lucide-react';

const Overview = () => {
  const [summary, setSummary] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/summary')
      .then(res => setSummary(res.data))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="text-gray-500">Loading summary...</div>;
  if (!summary) return <div className="text-red-500">Failed to load data.</div>;

  return (
    <div className="space-y-6 max-w-5xl">
      <div className="flex justify-between items-end">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Last 48 Hours</h2>
          <p className="text-sm text-gray-500 mt-1">Metrics based on jobs scraped in the past two days.</p>
        </div>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <StatCard title="Total Scraped" value={summary.total_jobs} icon={Activity} color="text-blue-600" bg="bg-blue-100" />
        <StatCard title="Pending Review" value={summary.pending} icon={AlertCircle} color="text-yellow-600" bg="bg-yellow-100" />
        <StatCard title="Relevant" value={summary.relevant} icon={CheckCircle} color="text-green-600" bg="bg-green-100" />
        <StatCard title="Irrelevant" value={summary.irrelevant} icon={XCircle} color="text-red-600" bg="bg-red-100" />
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold mb-4">Relevance Rate</h3>
        <div className="flex items-center">
          <div className="text-5xl font-bold text-gray-900">{summary.relevance_rate}%</div>
          <div className="ml-4 text-sm text-gray-500 max-w-xs">
            Percentage of reviewed jobs marked as relevant. Excludes pending jobs.
          </div>
        </div>
      </div>
    </div>
  );
};

const StatCard = ({ title, value, icon: Icon, color, bg }: any) => (
  <div className="bg-white rounded-lg p-5 shadow-sm border border-gray-200 flex items-center space-x-4">
    <div className={`p-3 rounded-full ${bg}`}>
      <Icon className={`w-6 h-6 ${color}`} />
    </div>
    <div>
      <p className="text-sm font-medium text-gray-500">{title}</p>
      <p className="text-2xl font-bold text-gray-900">{value.toLocaleString()}</p>
    </div>
  </div>
);

export default Overview;
