import { Outlet, useNavigate, useLocation } from 'react-router';
import { MessageSquare, BarChart3, Database, History, Menu, ArrowUpRight, Sparkles, PlugZap, Cpu, Loader2, FileText, Book } from 'lucide-react';
import { Button } from './ui/button';
import { Separator } from './ui/separator';
import { Sheet, SheetContent, SheetTrigger } from './ui/sheet';
import { Badge } from './ui/badge';
import { useEffect, useState } from 'react';
import { useConnectionStore } from '../store/connectionStore';
import type { RAGIndexState } from '../types/rag';
import { cn } from './ui/utils';
import { AIAssistant } from './assistant/AIAssistant';

export function Layout() {
  const navigate = useNavigate();
  const location = useLocation();
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const { connections, activeConnectionId, ragIndexStates, fetchConnections, fetchRagStates } = useConnectionStore();
  const activeConnection = connections.find((connection) => connection.id === activeConnectionId) ?? null;
  const ragStatus = activeConnectionId ? (ragIndexStates[activeConnectionId] ?? null) : null;
  const ragStatusLoading = Boolean(activeConnectionId && !ragStatus);

  const isActive = (path: string) => {
    return location.pathname === path || (path === '/' && location.pathname === '/chat');
  };

  const isSettingsActive = () => {
    return location.pathname.startsWith('/settings') || 
           location.pathname === '/prompts' || 
           location.pathname === '/assistant-config' || 
           location.pathname === '/knowledge-base';
  };

  useEffect(() => {
    void fetchConnections();
    void fetchRagStates();
  }, [fetchConnections, fetchRagStates]);

  const getRagStatusLabel = (status?: RAGIndexState | null) => {
    if (!status) {
      return 'RAG 未加载';
    }
    switch (status.index_status) {
      case 'ready':
        return 'RAG 已预构建';
      case 'indexing':
        return 'RAG 构建中';
      case 'failed':
        return 'RAG 构建失败';
      case 'pending':
        return 'RAG 待构建';
      default:
        return 'RAG 状态未知';
    }
  };

  const getRagStatusClasses = (status?: RAGIndexState | null) => {
    if (!status) {
      return 'border-white/10 bg-white/8 text-white/75';
    }
    switch (status.index_status) {
      case 'ready':
        return 'border-emerald-300/30 bg-emerald-500/15 text-emerald-50';
      case 'indexing':
        return 'border-amber-300/30 bg-amber-500/15 text-amber-50';
      case 'failed':
        return 'border-rose-300/30 bg-rose-500/15 text-rose-50';
      case 'pending':
        return 'border-white/10 bg-white/8 text-white/75';
      default:
        return 'border-white/10 bg-white/8 text-white/75';
    }
  };

  const SidebarContent = ({ collapsed = false }: { collapsed?: boolean }) => (
    <div className="flex h-full min-h-0 flex-col overflow-y-auto bg-sidebar text-sidebar-foreground">
      <div className={cn("border-b border-white/10", collapsed ? "px-3 py-3" : "px-4 py-4")}>
        <div className={cn("rounded-[24px] border border-white/10 bg-white/5 shadow-[0_18px_40px_rgba(0,0,0,0.16)]", collapsed ? "p-3" : "p-3.5")}>
          <div className={cn("flex justify-between gap-3", collapsed ? "flex-col items-center" : "items-start")}>
            <div className={cn(collapsed && "flex flex-col items-center text-center")}>
              <div className={cn("inline-flex items-center justify-center rounded-2xl bg-white text-black", collapsed ? "mb-2 h-10 w-10" : "mb-3 h-10 w-10")}>
                <Database className="h-4.5 w-4.5" />
              </div>
              {!collapsed && (
                <>
                  <h1 className="text-[20px] leading-[1.15] text-white">NL2SQL Agent</h1>
                  <p className="mt-2 max-w-[200px] text-[13px] leading-5 text-white/70">
                    把数据库访问做成一次对话，兼顾查询速度、可解释性和结果可视化。
                  </p>
                </>
              )}
            </div>
            <div className="rounded-full border border-white/12 bg-white/8 px-3 py-1 text-xs text-white/70">
              v1
            </div>
          </div>

          {!collapsed && (
            <div className="mt-3 grid grid-cols-1 gap-2">
              <div className="rounded-2xl border border-white/10 bg-black/10 px-3.5 py-2.5">
                <div className="text-[11px] uppercase tracking-[0.24em] text-white/45">Mode</div>
                <div className="mt-1 font-semibold text-white">Conversational BI</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/10 px-3.5 py-2.5">
                <div className="text-[11px] uppercase tracking-[0.24em] text-white/45">Focus</div>
                <div className="mt-1 font-semibold text-white">Schema-aware SQL</div>
              </div>
            </div>
          )}
        </div>
      </div>

      <div className={cn("py-3", collapsed ? "px-2" : "px-4")}>
        {!collapsed && (
          <div className="mb-2 flex items-center gap-2 px-2 text-[11px] font-semibold uppercase tracking-[0.28em] text-white/45">
            <Sparkles className="h-3.5 w-3.5" />
            Workspace
          </div>
        )}
        <div className="space-y-2">
          <Button
            variant="ghost"
            title="对话查询"
            className={`h-11 w-full rounded-2xl border text-left ${
              isActive('/') || isActive('/chat')
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/chat');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <MessageSquare className="h-4 w-4" />
              {!collapsed && '对话查询'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
          <Button
            variant="ghost"
            title="连接管理"
            className={`h-11 w-full rounded-2xl border text-left ${
              isActive('/connections')
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/connections');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <PlugZap className="h-4 w-4" />
              {!collapsed && '连接管理'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
          <Button
            variant="ghost"
            title="RAG 增强"
            className={`h-11 w-full rounded-2xl border text-left ${
              isActive('/rag')
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/rag');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <Sparkles className="h-4 w-4" />
              {!collapsed && 'RAG 增强'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
          <Button
            variant="ghost"
            title="分析面板"
            className={`h-11 w-full rounded-2xl border text-left ${
              isActive('/analytics')
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/analytics');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <BarChart3 className="h-4 w-4" />
              {!collapsed && '分析面板'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
          <Button
            variant="ghost"
            title="设置"
            className={`h-11 w-full rounded-2xl border text-left ${
              isSettingsActive()
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/settings');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <Cpu className="h-4 w-4" />
              {!collapsed && '设置'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
          <Button
            variant="ghost"
            title="查询历史"
            className={`h-11 w-full rounded-2xl border text-left ${
              isActive('/history')
                ? 'border-white/25 bg-white/12 text-white hover:bg-white/16'
                : 'border-white/8 bg-white/5 text-white/75 hover:bg-white/9 hover:text-white'
            } ${collapsed ? 'justify-center px-0' : 'justify-between px-4'}`}
            onClick={() => {
              navigate('/history');
              setIsMobileMenuOpen(false);
            }}
          >
            <span className={cn("flex items-center", collapsed ? "gap-0" : "gap-3")}>
              <History className="h-4 w-4" />
              {!collapsed && '查询历史'}
            </span>
            {!collapsed && <ArrowUpRight className="h-4 w-4 opacity-60" />}
          </Button>
        </div>
      </div>

      <Separator className="bg-white/8" />

      <div className={cn("mt-auto py-3", collapsed ? "px-2" : "px-4")}>
        <div className={cn("rounded-[24px] border border-white/10 bg-white/6 shadow-none backdrop-blur", collapsed ? "p-3" : "p-3.5")}>
          {!collapsed && (
            <div className="mb-2 flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.24em] text-white/45">
              <Database className="h-3.5 w-3.5" />
              Current connection
            </div>
          )}
          {activeConnection ? (
            <div className="space-y-2.5">
              <div className={cn("rounded-2xl border border-white/10 bg-black/10", collapsed ? "p-3" : "p-3.5")}>
                <div className={cn("flex gap-3", collapsed ? "flex-col items-center text-center" : "items-center justify-between")}>
                  <div>
                    {!collapsed ? (
                      <>
                        <div className="font-semibold text-white">{activeConnection.name}</div>
                        <div className="mt-1 text-xs uppercase tracking-[0.2em] text-white/45">
                          {activeConnection.db_type}
                        </div>
                      </>
                    ) : (
                      <Database className="mx-auto h-4 w-4 text-white" />
                    )}
                  </div>
                  <div className={`h-2.5 w-2.5 rounded-full ${activeConnection.is_online ? 'bg-white' : 'bg-white/35'}`} />
                </div>
                {!collapsed && (
                  <div className="mt-2 text-xs leading-4.5 text-white/55">
                    {activeConnection.is_online ? '已就绪，可直接进入聊天页查询。' : '当前连接离线，请到连接管理页检查配置。'}
                  </div>
                )}
              </div>
              <div className={cn("flex items-center gap-2", collapsed && "justify-center")}>
                {ragStatusLoading ? (
                  <Badge variant="outline" className="border-white/10 bg-white/8 text-white/75">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    {collapsed ? '' : 'RAG'}
                  </Badge>
                ) : (
                  <Badge variant="outline" className={cn("border", getRagStatusClasses(ragStatus))}>
                    {getRagStatusLabel(ragStatus)}
                  </Badge>
                )}
                {!collapsed && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 rounded-full border border-white/10 bg-white/8 px-3 text-xs text-white/80 hover:bg-white/12 hover:text-white"
                    onClick={() => {
                      navigate('/rag');
                      setIsMobileMenuOpen(false);
                    }}
                  >
                    查看 RAG
                  </Button>
                )}
              </div>
              <Button
                title="打开连接管理"
                className={cn("h-10 w-full rounded-2xl bg-white text-black hover:bg-zinc-100", collapsed && "px-0")}
                onClick={() => {
                  navigate('/connections');
                  setIsMobileMenuOpen(false);
                }}
              >
                {collapsed ? <PlugZap className="h-4 w-4" /> : '打开连接管理'}
              </Button>
            </div>
          ) : (
            <div className="space-y-2.5">
              {!collapsed && (
                <div className="rounded-2xl border border-dashed border-white/14 bg-black/10 p-3.5 text-sm leading-5 text-white/55">
                  还没有选中的数据库连接。RAG 会在连接创建或同步后自动预构建。
                </div>
              )}
              <div className={cn("flex items-center gap-2", collapsed && "justify-center")}>
                <Badge variant="outline" className={cn("border", getRagStatusClasses(null))}>
                  RAG 查看
                </Badge>
                {!collapsed && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 rounded-full border border-white/10 bg-white/8 px-3 text-xs text-white/80 hover:bg-white/12 hover:text-white"
                    onClick={() => {
                      navigate('/rag');
                      setIsMobileMenuOpen(false);
                    }}
                  >
                    查看 RAG
                  </Button>
                )}
              </div>
              <Button
                title="前往连接管理"
                className={cn("h-10 w-full rounded-2xl bg-white text-black hover:bg-zinc-100", collapsed && "px-0")}
                onClick={() => {
                  navigate('/connections');
                  setIsMobileMenuOpen(false);
                }}
              >
                {collapsed ? <PlugZap className="h-4 w-4" /> : '前往连接管理'}
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  return (
    <div className="flex h-[100dvh] overflow-hidden bg-transparent">
      <aside className="hidden w-[304px] shrink-0 border-r border-white/10 lg:flex lg:flex-col">
        <SidebarContent />
      </aside>

      <div className="lg:hidden fixed top-4 left-4 z-50">
        <Sheet open={isMobileMenuOpen} onOpenChange={setIsMobileMenuOpen}>
          <SheetTrigger asChild>
            <Button variant="outline" size="icon" className="rounded-2xl border-white/40 bg-white/75 backdrop-blur">
              <Menu className="h-5 w-5" />
            </Button>
          </SheetTrigger>
          <SheetContent side="left" className="w-[92vw] max-w-[360px] border-r-0 bg-sidebar p-0">
            <SidebarContent />
          </SheetContent>
        </Sheet>
      </div>

      <main className="relative min-h-0 flex-1 overflow-hidden">
        <div className="relative h-full overflow-hidden min-h-0 lg:p-3">
          <div className="h-full overflow-hidden border border-zinc-200 bg-white shadow-[0_12px_30px_rgba(0,0,0,0.06)] min-h-0 lg:rounded-[32px]">
            <Outlet />
          </div>
        </div>
      </main>

      {/* AI 助手悬浮球 */}
      <AIAssistant />
    </div>
  );
}
