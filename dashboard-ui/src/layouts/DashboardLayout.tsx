import React from 'react';
import { Outlet, NavLink } from 'react-router-dom';
import { LayoutDashboard, ListTodo, BarChart3 } from 'lucide-react';

const DashboardLayout = () => {
  const navItems = [
    { name: 'Overview', path: '/overview', icon: LayoutDashboard },
    { name: 'Review Queue', path: '/queue', icon: ListTodo },
    { name: 'Insights', path: '/insights', icon: BarChart3 },
  ];

  return (
    <div className="min-h-screen flex bg-gray-50">
      {/* Sidebar */}
      <aside className="w-64 bg-white border-r border-gray-200 flex-shrink-0">
        <div className="h-16 flex items-center px-6 border-b border-gray-200">
          <h1 className="text-xl font-bold text-blue-600 tracking-tight">Dice Scraper QA</h1>
        </div>
        <nav className="p-4 space-y-1">
          {navItems.map((item) => (
            <NavLink
              key={item.name}
              to={item.path}
              className={({ isActive }) =>
                `flex items-center px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                  isActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-700 hover:bg-gray-100'
                }`
              }
            >
              <item.icon className="w-5 h-5 mr-3 opacity-75" />
              {item.name}
            </NavLink>
          ))}
        </nav>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col h-screen overflow-hidden">
        <header className="h-16 bg-white border-b border-gray-200 flex items-center px-8 shadow-sm z-10 flex-shrink-0">
          <h2 className="text-lg font-semibold text-gray-800">Relevance Review Dashboard</h2>
        </header>
        <div className="flex-1 overflow-auto p-8">
          <Outlet />
        </div>
      </main>
    </div>
  );
};

export default DashboardLayout;
