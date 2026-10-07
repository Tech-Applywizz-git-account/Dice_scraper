import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import api from '../api';
import { ArrowLeft, ExternalLink, CheckCircle2, XCircle, AlertCircle, Save } from 'lucide-react';
import clsx from 'clsx';

const JobDetail = () => {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  
  // Form state
  const [reviewStatus, setReviewStatus] = useState<string>('');
  const [reviewReason, setReviewReason] = useState<string>('');
  const [reviewComment, setReviewComment] = useState<string>('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.get(`/jobs/${jobId}`)
      .then(res => {
        setData(res.data);
        if (res.data.review) {
          setReviewStatus(res.data.review.status || '');
          setReviewReason(res.data.review.reason || '');
          setReviewComment(res.data.review.comment || '');
        }
      })
      .finally(() => setLoading(false));
  }, [jobId]);

  const handleSave = async () => {
    if (reviewStatus === 'irrelevant' && !reviewReason) {
      alert("Please select a reason for marking the job as irrelevant.");
      return;
    }
    setSaving(true);
    try {
      await api.post(`/jobs/${jobId}/review`, {
        status: reviewStatus,
        reason: reviewStatus === 'irrelevant' ? reviewReason : null,
        comment: reviewComment
      });
      alert('Review saved successfully!');
      navigate('/queue');
    } catch (e) {
      alert('Failed to save review');
    } finally {
      setSaving(false);
    }
  };

  if (loading) return <div className="p-8">Loading...</div>;
  if (!data) return <div className="p-8 text-red-500">Failed to load job details.</div>;

  const { job, client_requirement: client, match_analysis: match, scrape_trace: trace } = data;

  return (
    <div className="space-y-6 pb-20">
      <div className="flex items-center space-x-4">
        <button onClick={() => navigate(-1)} className="p-2 bg-white border rounded hover:bg-gray-50">
          <ArrowLeft className="w-5 h-5" />
        </button>
        <h2 className="text-2xl font-bold text-gray-900">Job Review</h2>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* LEFT COLUMN: Scraped Job */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 space-y-4">
          <div className="flex justify-between items-start">
            <div>
              <h3 className="text-xl font-bold text-blue-700">{job.title}</h3>
              <p className="text-lg text-gray-700">{job.company}</p>
            </div>
            <a 
              href={job.url} 
              target="_blank" 
              rel="noreferrer"
              className="flex items-center text-sm font-medium text-blue-600 hover:text-blue-800 bg-blue-50 px-3 py-1.5 rounded-md"
            >
              Open Original <ExternalLink className="w-4 h-4 ml-1" />
            </a>
          </div>

          <div className="grid grid-cols-2 gap-4 text-sm bg-gray-50 p-4 rounded-md border border-gray-100">
            <div><span className="font-semibold text-gray-600">Location:</span> {job.location || 'N/A'}</div>
            <div><span className="font-semibold text-gray-600">Job Type:</span> {job.job_type || 'N/A'}</div>
            <div><span className="font-semibold text-gray-600">Experience:</span> {job.scraped_experience || 'N/A'}</div>
            <div><span className="font-semibold text-gray-600">Scraped At:</span> {new Date(job.scraped_at).toLocaleString()}</div>
          </div>

          <div>
            <h4 className="font-semibold text-gray-900 mb-2">Full Description</h4>
            <div className="prose prose-sm max-w-none text-gray-700 bg-gray-50 p-4 rounded-md border border-gray-200 h-96 overflow-y-auto whitespace-pre-wrap">
              {job.description}
            </div>
          </div>
        </div>

        {/* RIGHT COLUMN: Client Requirement & Review */}
        <div className="space-y-6">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 space-y-4">
            <h3 className="text-lg font-bold border-b pb-2">Client Requirement (AWL: {job.applywizz_id})</h3>
            <div className="grid grid-cols-2 gap-y-3 text-sm">
              <div className="font-medium text-gray-500">Required Role:</div>
              <div className="font-semibold">{client.role}</div>
              
              <div className="font-medium text-gray-500">Alt Roles:</div>
              <div>{client.alternate_roles || 'None'}</div>
              
              <div className="font-medium text-gray-500">Experience Limit:</div>
              <div>{client.experience} years</div>
              
              <div className="font-medium text-gray-500">Locations:</div>
              <div>{(client.locations || []).join(', ') || 'N/A'}</div>
              
              <div className="font-medium text-gray-500">Work Pref:</div>
              <div className="capitalize">{client.work_preference}</div>
              
              <div className="font-medium text-gray-500">Sponsorship:</div>
              <div className="capitalize">{client.sponsorship}</div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6 space-y-4">
            <h3 className="text-lg font-bold border-b pb-2">Scrape Trace & Match Analysis</h3>
            
            <div className="bg-slate-50 p-3 rounded text-sm border border-slate-200 mb-4 space-y-1">
              <p><span className="font-semibold">Search Keyword Used:</span> "{trace.search_keyword}"</p>
              <p><span className="font-semibold">Search Location Used:</span> "{trace.search_location}"</p>
            </div>

            <div className="space-y-2">
              <MatchRow label="Role Match" match={match.role_match} score={match.score_breakdown?.role} />
              <MatchRow label="Experience Match" match={match.experience_match} score={match.score_breakdown?.experience} />
              <MatchRow label="Location Match" match={match.location_match} score={match.score_breakdown?.location} />
              <MatchRow label="Work Mode Match" match={match.work_mode_match} score={match.score_breakdown?.work_mode} />
              <MatchRow label="Sponsorship Match" match={match.sponsorship_match} score={match.score_breakdown?.sponsorship} />
              <MatchRow label="Company Check" match={match.company_match} passFail />
            </div>

            <div className="pt-4 border-t flex justify-between items-center">
              <span className="font-semibold">System Relevance Score</span>
              <span className={clsx("text-2xl font-bold", match.relevance_score >= 80 ? "text-green-600" : match.relevance_score >= 50 ? "text-yellow-600" : "text-red-600")}>
                {match.relevance_score} / 100
              </span>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-blue-200 p-6 space-y-4 shadow-blue-50">
            <h3 className="text-lg font-bold text-blue-900 border-b border-blue-100 pb-2">Human Review Decision</h3>
            
            <div className="flex space-x-3">
              <ReviewButton 
                active={reviewStatus === 'relevant'} 
                onClick={() => setReviewStatus('relevant')}
                label="Relevant" icon={CheckCircle2} color="green" 
              />
              <ReviewButton 
                active={reviewStatus === 'irrelevant'} 
                onClick={() => setReviewStatus('irrelevant')}
                label="Irrelevant" icon={XCircle} color="red" 
              />
              <ReviewButton 
                active={reviewStatus === 'needs_review'} 
                onClick={() => setReviewStatus('needs_review')}
                label="Needs Review" icon={AlertCircle} color="yellow" 
              />
            </div>

            {reviewStatus === 'irrelevant' && (
              <div className="mt-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">Why is this job irrelevant? *</label>
                <select 
                  className="w-full border-gray-300 rounded-md shadow-sm p-2 border focus:ring-blue-500 focus:border-blue-500"
                  value={reviewReason}
                  onChange={e => setReviewReason(e.target.value)}
                >
                  <option value="">Select a reason...</option>
                  <option value="wrong_role">Wrong Job Role</option>
                  <option value="wrong_experience">Wrong Experience</option>
                  <option value="wrong_location">Wrong Location</option>
                  <option value="wrong_work_mode">Wrong Work Mode</option>
                  <option value="sponsorship_issue">Sponsorship Issue</option>
                  <option value="company_issue">Company Issue</option>
                  <option value="duplicate">Duplicate / Same Job</option>
                  <option value="description_mismatch">Job Description Doesn't Match</option>
                  <option value="keyword_false_positive">Search Keyword False Positive</option>
                  <option value="other">Other</option>
                </select>
              </div>
            )}

            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-1">Additional Comment (Optional)</label>
              <textarea 
                className="w-full border-gray-300 rounded-md shadow-sm p-2 border focus:ring-blue-500 focus:border-blue-500"
                rows={3}
                value={reviewComment}
                onChange={e => setReviewComment(e.target.value)}
                placeholder="Add context to your decision..."
              />
            </div>

            <div className="pt-4 flex justify-end">
              <button 
                onClick={handleSave}
                disabled={saving || !reviewStatus}
                className="flex items-center px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                <Save className="w-4 h-4 mr-2" />
                {saving ? 'Saving...' : 'Save Review'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const MatchRow = ({ label, match, score, passFail = false }: any) => (
  <div className="flex justify-between items-center text-sm py-1 border-b border-gray-100 last:border-0">
    <span className="text-gray-600">{label}</span>
    <div className="flex items-center space-x-4">
      {match ? (
        <span className="flex items-center text-green-600 font-medium">
          <CheckCircle2 className="w-4 h-4 mr-1" /> {passFail ? 'Passed' : 'Match'}
        </span>
      ) : (
        <span className="flex items-center text-red-600 font-medium">
          <XCircle className="w-4 h-4 mr-1" /> {passFail ? 'Failed' : 'Mismatch'}
        </span>
      )}
      {!passFail && <span className="w-16 text-right text-gray-400">{score}</span>}
    </div>
  </div>
);

const ReviewButton = ({ active, onClick, label, icon: Icon, color }: any) => {
  const colors = {
    green: "hover:bg-green-50 hover:border-green-500 text-green-700",
    red: "hover:bg-red-50 hover:border-red-500 text-red-700",
    yellow: "hover:bg-yellow-50 hover:border-yellow-500 text-yellow-700",
  };
  const activeColors = {
    green: "bg-green-100 border-green-600 text-green-800 ring-2 ring-green-500 ring-opacity-50",
    red: "bg-red-100 border-red-600 text-red-800 ring-2 ring-red-500 ring-opacity-50",
    yellow: "bg-yellow-100 border-yellow-600 text-yellow-800 ring-2 ring-yellow-500 ring-opacity-50",
  };

  return (
    <button
      onClick={onClick}
      className={clsx(
        "flex-1 flex flex-col items-center justify-center p-3 rounded-lg border transition-all duration-200",
        active ? (activeColors as any)[color] : `bg-white border-gray-200 ${(colors as any)[color]}`
      )}
    >
      <Icon className="w-6 h-6 mb-1" />
      <span className="font-semibold text-sm">{label}</span>
    </button>
  );
}

export default JobDetail;
