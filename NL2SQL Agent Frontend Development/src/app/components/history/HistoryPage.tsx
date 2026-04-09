import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';
import { History as HistoryIcon, RefreshCw, Trash2 } from 'lucide-react';
import { historyApi } from '../../api/client';
import type { QueryHistoryDetail, QueryHistoryItem } from '../../types/query';
import { HistoryPanel } from './HistoryPanel';
import { Button } from '../ui/button';
import { Card, CardContent } from '../ui/card';
import { toast } from 'sonner';
import { useConnectionStore } from '../../store/connectionStore';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../ui/alert-dialog';

export function HistoryPage() {
  const navigate = useNavigate();
  const [histories, setHistories] = useState<Array<QueryHistoryItem | QueryHistoryDetail>>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [showDeleteAllDialog, setShowDeleteAllDialog] = useState(false);
  const fetchConnections = useConnectionStore((state) => state.fetchConnections);

  const loadHistories = async () => {
    setIsLoading(true);
    try {
      const [items] = await Promise.all([
        historyApi.list({ limit: 50, offset: 0 }),
        fetchConnections(),
      ]);
      setHistories(items);
    } catch (error: any) {
      toast.error(`加载历史记录失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadHistories();
  }, []);

  const handleSelectHistory = async (
    history: QueryHistoryItem | QueryHistoryDetail,
    threadItems: Array<QueryHistoryItem | QueryHistoryDetail> = [history],
  ) => {
    try {
      const details = await Promise.all(
        threadItems.map((item) => historyApi.get(item.id)),
      );
      const orderedDetails = [...details].sort(
        (a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime(),
      );
      navigate('/chat', {
        state: {
          action: 'view-history',
          histories: orderedDetails,
        },
      });
    } catch (error: any) {
      toast.error(`加载历史详情失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleLoadDetail = async (queryId: string) => historyApi.get(queryId);

  const handleRetry = async (queryId: string) => {
    try {
      const detail = await historyApi.get(queryId);
      navigate('/chat', {
        state: {
          action: 'retry-history',
          history: detail,
        },
      });
    } catch (error: any) {
      toast.error(`加载历史详情失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleFollowUp = async (queryId: string, question: string) => {
    try {
      const context = await historyApi.getContext(queryId);
      navigate('/chat', {
        state: {
          action: 'follow-up-history',
          question,
          context,
        },
      });
    } catch (error: any) {
      toast.error(`加载追问上下文失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleDeleteHistory = async (queryId: string) => {
    try {
      await historyApi.delete(queryId);
      toast.success('历史记录已删除');
      await loadHistories();
    } catch (error: any) {
      toast.error(`删除失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleDeleteAll = async () => {
    try {
      const result = await historyApi.deleteAll();
      toast.success(`已删除 ${result.deleted_count} 条历史记录`);
      setHistories([]);
      setShowDeleteAllDialog(false);
    } catch (error: any) {
      toast.error(`删除失败: ${error?.message || '未知错误'}`);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center p-4">
        <Card className="w-full max-w-xl">
          <CardContent className="flex flex-col items-center justify-center py-16">
            <RefreshCw className="h-8 w-8 animate-spin text-primary mb-3" />
            <p className="text-muted-foreground">加载历史记录中...</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="h-full p-4 md:p-4">
      <div className="h-full flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-2">
              <HistoryIcon className="h-7 w-7" />
              对话查询历史
            </h1>
            <p className="text-sm text-muted-foreground mt-1">查看最近的对话查询，区分每条查询对应的数据库连接，并可基于历史结果继续追问。</p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={() => void loadHistories()}>
              <RefreshCw className="h-4 w-4 mr-2" />
              刷新
            </Button>
            {histories.length > 0 && (
              <Button 
                variant="outline" 
                onClick={() => setShowDeleteAllDialog(true)}
                className="text-destructive hover:text-destructive"
              >
                <Trash2 className="h-4 w-4 mr-2" />
                清空历史
              </Button>
            )}
          </div>
        </div>

        <div className="flex-1 min-h-0">
          <HistoryPanel
            histories={histories}
            onLoadDetail={handleLoadDetail}
            onSelectHistory={handleSelectHistory}
            onRetry={handleRetry}
            onFollowUp={handleFollowUp}
            onDelete={handleDeleteHistory}
          />
        </div>
      </div>

      <AlertDialog open={showDeleteAllDialog} onOpenChange={setShowDeleteAllDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认清空所有历史记录？</AlertDialogTitle>
            <AlertDialogDescription>
              此操作将永久删除所有查询历史记录，包括查询问题、SQL、结果等信息。此操作无法撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={() => void handleDeleteAll()} className="bg-destructive text-destructive-foreground hover:bg-destructive/90">
              确认删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
