import React, { useEffect, useRef, useState } from 'react';
import { Book, MessageCircle, Minimize2, Send, X } from 'lucide-react';

import { assistantApi } from '../../api/client';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  sources?: string[];
}

const DIALOG_WIDTH = 400;
const DIALOG_HEIGHT = 600;
const FLOATING_SIZE = 56;
const VIEWPORT_PADDING = 20;

const createMessageId = () => `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

export const AIAssistant: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<Message[]>([
    {
      id: createMessageId(),
      role: 'assistant',
      content:
        '你好！我是 NL2SQL Agent 的智能助手。\n\n我可以帮你：\n• 了解系统功能\n• 学习如何使用\n• 解答常见问题\n• 提供操作指引\n\n有什么我可以帮你的吗？',
    },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [position, setPosition] = useState({
    x: window.innerWidth - 100,
    y: window.innerHeight - 100,
  });
  const [dialogPosition, setDialogPosition] = useState({ x: 0, y: 0 });
  const [isDraggingBall, setIsDraggingBall] = useState(false);
  const [ballDragOffset, setBallDragOffset] = useState({ x: 0, y: 0 });
  const [ballDragStartTime, setBallDragStartTime] = useState(0);
  const [isDraggingDialog, setIsDraggingDialog] = useState(false);
  const [dialogDragOffset, setDialogDragOffset] = useState({ x: 0, y: 0 });

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const floatingButtonRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const clampBallPosition = (x: number, y: number) => ({
    x: Math.max(0, Math.min(window.innerWidth - FLOATING_SIZE, x)),
    y: Math.max(0, Math.min(window.innerHeight - FLOATING_SIZE, y)),
  });

  const clampDialogPosition = (x: number, y: number) => ({
    x: Math.max(
      VIEWPORT_PADDING,
      Math.min(window.innerWidth - DIALOG_WIDTH - VIEWPORT_PADDING, x),
    ),
    y: Math.max(
      VIEWPORT_PADDING,
      Math.min(window.innerHeight - DIALOG_HEIGHT - VIEWPORT_PADDING, y),
    ),
  });

  const computeDialogPosition = (buttonPosition: { x: number; y: number }) => {
    let x = buttonPosition.x - DIALOG_WIDTH - 20;
    let y = buttonPosition.y;

    if (x < VIEWPORT_PADDING) {
      x = buttonPosition.x + FLOATING_SIZE + 20;
    }

    return clampDialogPosition(x, y);
  };

  const openDialog = () => {
    setDialogPosition(computeDialogPosition(position));
    setIsOpen(true);
  };

  const updateMessage = (id: string, updater: (message: Message) => Message) => {
    setMessages((previous) => previous.map((message) => (message.id === id ? updater(message) : message)));
  };

  const handleBallMouseDown = (event: React.MouseEvent) => {
    if (isOpen) {
      return;
    }

    setBallDragStartTime(Date.now());
    setIsDraggingBall(true);

    const rect = floatingButtonRef.current?.getBoundingClientRect();
    if (rect) {
      setBallDragOffset({
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      });
    }
  };

  const handleDialogHeaderMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    setIsDraggingDialog(true);
    setDialogDragOffset({
      x: event.clientX - dialogPosition.x,
      y: event.clientY - dialogPosition.y,
    });
  };

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (isDraggingBall) {
        setPosition(
          clampBallPosition(event.clientX - ballDragOffset.x, event.clientY - ballDragOffset.y),
        );
      }

      if (isDraggingDialog) {
        setDialogPosition(
          clampDialogPosition(
            event.clientX - dialogDragOffset.x,
            event.clientY - dialogDragOffset.y,
          ),
        );
      }
    };

    const handleMouseUp = () => {
      setIsDraggingBall(false);
      setIsDraggingDialog(false);
    };

    if (isDraggingBall || isDraggingDialog) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [ballDragOffset, dialogDragOffset, isDraggingBall, isDraggingDialog]);

  const handleFloatingButtonClick = () => {
    if (Date.now() - ballDragStartTime < 200) {
      openDialog();
    }
  };

  const handleMinimize = () => {
    setIsOpen(false);
  };

  const handleClose = () => {
    setIsOpen(false);
  };

  const handleSend = async () => {
    if (!input.trim() || isLoading) {
      return;
    }

    const requestMessage = input.trim();
    const userMessage: Message = {
      id: createMessageId(),
      role: 'user',
      content: requestMessage,
    };
    const assistantMessageId = createMessageId();
    const assistantPlaceholder: Message = {
      id: assistantMessageId,
      role: 'assistant',
      content: '',
      sources: [],
    };

    const history = messages.map((message) => ({
      role: message.role,
      content: message.content,
    }));

    setMessages((previous) => [...previous, userMessage, assistantPlaceholder]);
    setInput('');
    setIsLoading(true);

    try {
      await assistantApi.streamChat(
        {
          message: requestMessage,
          history,
        },
        {
          onChunk: (delta) => {
            updateMessage(assistantMessageId, (message) => ({
              ...message,
              content: `${message.content}${delta}`,
            }));
          },
          onDone: ({ message, sources }) => {
            updateMessage(assistantMessageId, (current) => ({
              ...current,
              content: message || current.content,
              sources,
            }));
          },
          onError: (errorMessage) => {
            updateMessage(assistantMessageId, () => ({
              id: assistantMessageId,
              role: 'assistant',
              content: errorMessage || '抱歉，我暂时遇到了一些问题，请稍后再试。',
            }));
          },
        },
      );
    } catch (error) {
      console.error('Failed to send message:', error);
      updateMessage(assistantMessageId, () => ({
        id: assistantMessageId,
        role: 'assistant',
        content: '抱歉，我暂时遇到了一些问题，请稍后再试。',
      }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      void handleSend();
    }
  };

  const quickQuestions = [
    '如何查询数据？',
    '怎么查看历史记录？',
    '如何添加数据库连接？',
    'RAG 是什么？',
  ];

  return (
    <>
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/20 backdrop-blur-sm"
          style={{ zIndex: 9997 }}
          onClick={handleClose}
        />
      )}

      {!isOpen && (
        <div
          ref={floatingButtonRef}
          className="fixed"
          style={{
            left: `${position.x}px`,
            top: `${position.y}px`,
            zIndex: 9999,
          }}
        >
          <button
            onMouseDown={handleBallMouseDown}
            onClick={handleFloatingButtonClick}
            className="flex h-14 w-14 cursor-pointer items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-purple-600 text-white shadow-lg transition-all duration-300 hover:scale-110 hover:shadow-xl active:scale-95"
            title="AI 助手"
          >
            <MessageCircle className="h-6 w-6" />
          </button>
          <div className="pointer-events-none absolute inset-0 animate-ping rounded-full bg-blue-400 opacity-20" />
        </div>
      )}

      {isOpen && (
        <div
          className="fixed flex flex-col rounded-lg border border-gray-200 bg-white shadow-2xl"
          style={{
            left: `${dialogPosition.x}px`,
            top: `${dialogPosition.y}px`,
            width: `${DIALOG_WIDTH}px`,
            height: `${DIALOG_HEIGHT}px`,
            maxHeight: '80vh',
            zIndex: 9998,
          }}
        >
          <div
            className="flex cursor-move select-none items-center justify-between rounded-t-lg border-b bg-gradient-to-r from-blue-500 to-purple-600 p-4 text-white"
            onMouseDown={handleDialogHeaderMouseDown}
          >
            <div className="flex items-center gap-2">
              <MessageCircle className="h-5 w-5" />
              <h3 className="font-semibold">AI 助手</h3>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={handleMinimize}
                onMouseDown={(event) => event.stopPropagation()}
                className="rounded p-1 transition-colors hover:bg-white/20"
                title="最小化"
              >
                <Minimize2 className="h-4 w-4" />
              </button>
              <button
                onClick={handleClose}
                onMouseDown={(event) => event.stopPropagation()}
                className="rounded p-1 transition-colors hover:bg-white/20"
                title="关闭"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          <div className="flex-1 space-y-4 overflow-y-auto p-4">
            {messages.map((message) => (
              <div
                key={message.id}
                className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                <div
                  className={`max-w-[80%] rounded-lg p-3 ${
                    message.role === 'user'
                      ? 'bg-blue-500 text-white'
                      : 'bg-gray-100 text-gray-800'
                  }`}
                >
                  <div className="whitespace-pre-wrap text-sm">
                    {message.content || (isLoading && message.role === 'assistant' ? '正在思考…' : '')}
                  </div>
                  {message.sources && message.sources.length > 0 && (
                    <div className="mt-2 flex items-center gap-1 border-t border-gray-300 pt-2 text-xs text-gray-600">
                      <Book className="h-3 w-3" />
                      <span>参考：{message.sources.join('、')}</span>
                    </div>
                  )}
                </div>
              </div>
            ))}

            <div ref={messagesEndRef} />
          </div>

          {messages.length === 1 && (
            <div className="px-4 pb-2">
              <div className="mb-2 text-xs text-gray-500">快捷问题：</div>
              <div className="flex flex-wrap gap-2">
                {quickQuestions.map((question) => (
                  <button
                    key={question}
                    onClick={() => setInput(question)}
                    className="rounded-full bg-gray-100 px-3 py-1 text-xs transition-colors hover:bg-gray-200"
                  >
                    {question}
                  </button>
                ))}
              </div>
            </div>
          )}

          <div className="border-t p-4">
            <div className="flex gap-2">
              <input
                type="text"
                value={input}
                onChange={(event) => setInput(event.target.value)}
                onKeyDown={handleKeyPress}
                placeholder="输入你的问题..."
                className="flex-1 rounded-lg border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                disabled={isLoading}
              />
              <button
                onClick={() => void handleSend()}
                disabled={!input.trim() || isLoading}
                className="rounded-lg bg-blue-500 px-4 py-2 text-white transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:bg-gray-300"
              >
                <Send className="h-4 w-4" />
              </button>
            </div>
            <div className="mt-2 text-xs text-gray-400">按 Enter 发送，Shift + Enter 换行</div>
          </div>
        </div>
      )}
    </>
  );
};
