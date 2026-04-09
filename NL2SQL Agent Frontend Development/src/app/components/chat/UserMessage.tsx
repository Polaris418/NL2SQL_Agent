import { User } from 'lucide-react';
import { Card } from '../ui/card';

interface UserMessageProps {
  content: string;
}

export function UserMessage({ content }: UserMessageProps) {
  return (
    <div className="mb-6 flex justify-end">
      <div className="flex max-w-[86%] gap-3">
        <Card className="rounded-[26px] border-0 bg-black p-5 text-primary-foreground shadow-[0_12px_28px_rgba(0,0,0,0.12)]">
          <p className="text-sm whitespace-pre-wrap break-words">{content}</p>
        </Card>
        <div className="flex items-start pt-1">
          <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-2xl bg-black shadow-[0_10px_24px_rgba(0,0,0,0.12)]">
            <User className="h-4 w-4 text-primary-foreground" />
          </div>
        </div>
      </div>
    </div>
  );
}
