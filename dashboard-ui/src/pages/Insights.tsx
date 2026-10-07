import React, { useEffect, useState } from 'react';
import api from '../api';
import { PieChart, AlertCircle } from 'lucide-react';

const Insights = () => {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/analytics')
      .then(res => setData(res.data))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8">Loading analytics...</div>;
  if (!data) return <div className="p-8 text-red-500">Failed to load analytics.</div>;

  return (
    <div className="space-y-6 max-w-5xl">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Scraper Insights</h2>
        <p className="text-sm text-gray-500 mt-1">Identify patterns in false positives and irrelevant jobs.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center border-b pb-4 mb-4">
            <PieChart className="w-5 h-5 text-blue-600 mr-2" />
            <h3 className="text-lg font-semibold text-gray-800">Top Irrelevance Reasons</h3>
          </div>
          
          {data.top_irrelevance_reasons?.length === 0 ? (
            <p className="text-gray-500">No irrelevance data collected yet.</p>
          ) : (
            <div className="space-y-4">
              {data.top_irrelevance_reasons?.map((item: any) => (
                <div key={item.review_reason}>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="font-medium text-gray-700 capitalize">
                      {item.review_reason.replace(/_/g, ' ')}
                    </span>
                    <span className="text-gray-500">{item.count} jobs</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div 
                      className="bg-blue-600 h-2 rounded-full" 
                      style={{ width: `${Math.min(100, (item.count / data.top_irrelevance_reasons[0].count) * 100)}%` }}
                    ></div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <div className="flex items-center border-b pb-4 mb-4">
            <AlertCircle className="w-5 h-5 text-yellow-600 mr-2" />
            <h3 className="text-lg font-semibold text-gray-800">Filter Failure Analysis</h3>
          </div>
          <p className="text-sm text-gray-600 mb-4">
            This section helps identify which business rules in <code>filtering.py</code> need improvement.
            If "Wrong Job Role" is the top reason, consider tightening keyword matching logic.
          </p>
          <div className="bg-yellow-50 text-yellow-800 p-4 rounded-md text-sm border border-yellow-200">
            <strong>Recommendation:</strong> Use the reasons on the left to prioritize scraper updates.
          </div>
        </div>
      </div>
    </div>
  );
};

export default Insights;
