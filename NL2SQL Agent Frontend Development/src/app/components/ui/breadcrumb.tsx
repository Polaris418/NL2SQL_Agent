import React from 'react';
import { ChevronRight, Home, ArrowLeft } from 'lucide-react';
import { Link, useNavigate } from 'react-router';

export interface BreadcrumbItem {
  label: string;
  path?: string;
  icon?: React.ReactNode;
}

interface BreadcrumbProps {
  items: BreadcrumbItem[];
  className?: string;
  showBackButton?: boolean;
}

export const Breadcrumb: React.FC<BreadcrumbProps> = ({ items, className = '', showBackButton = true }) => {
  const navigate = useNavigate();
  
  // 获取上一级路径（倒数第二个有 path 的项）
  const getBackPath = () => {
    for (let i = items.length - 2; i >= 0; i--) {
      if (items[i].path) {
        return items[i].path;
      }
    }
    return '/';
  };

  return (
    <div className={`flex items-center gap-4 ${className}`}>
      {/* 返回按钮 */}
      {showBackButton && items.length > 1 && (
        <button
          onClick={() => navigate(getBackPath())}
          className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
          title="返回上一级"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>返回</span>
        </button>
      )}
      
      {/* 面包屑导航 */}
      <nav className="flex items-center gap-2 text-sm" aria-label="面包屑导航">
        {items.map((item, index) => {
          const isLast = index === items.length - 1;
          
          return (
            <React.Fragment key={index}>
              {index > 0 && (
                <ChevronRight className="h-4 w-4 text-gray-400" />
              )}
              {item.path && !isLast ? (
                <Link
                  to={item.path}
                  className="flex items-center gap-1.5 text-gray-600 hover:text-gray-900 transition-colors"
                >
                  {item.icon}
                  <span>{item.label}</span>
                </Link>
              ) : (
                <span className={`flex items-center gap-1.5 ${isLast ? 'text-gray-900 font-medium' : 'text-gray-600'}`}>
                  {item.icon}
                  <span>{item.label}</span>
                </span>
              )}
            </React.Fragment>
          );
        })}
      </nav>
    </div>
  );
};

// 预定义的面包屑配置
export const breadcrumbConfigs = {
  settings: [
    { label: '首页', path: '/', icon: <Home className="h-3.5 w-3.5" /> },
    { label: '设置', path: '/settings' },
  ],
  settingsLlm: [
    { label: '首页', path: '/', icon: <Home className="h-3.5 w-3.5" /> },
    { label: '设置', path: '/settings' },
    { label: 'API 配置' },
  ],
  prompts: [
    { label: '首页', path: '/', icon: <Home className="h-3.5 w-3.5" /> },
    { label: '设置', path: '/settings' },
    { label: 'Prompt 配置' },
  ],
  assistantConfig: [
    { label: '首页', path: '/', icon: <Home className="h-3.5 w-3.5" /> },
    { label: '设置', path: '/settings' },
    { label: 'AI 助手配置' },
  ],
  knowledgeBase: [
    { label: '首页', path: '/', icon: <Home className="h-3.5 w-3.5" /> },
    { label: '设置', path: '/settings' },
    { label: '知识库管理' },
  ],
};
