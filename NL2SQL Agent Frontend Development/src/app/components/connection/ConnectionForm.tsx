import { useState } from 'react';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { Label } from '../ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Alert, AlertDescription } from '../ui/alert';
import { AlertCircle, Plus, ShieldCheck } from 'lucide-react';
import type { ConnectionConfig, DBType } from '../../types/connection';
import { useConnectionStore } from '../../store/connectionStore';

export function ConnectionForm() {
  const { addConnection } = useConnectionStore();
  const [config, setConfig] = useState<ConnectionConfig>({
    name: '',
    db_type: 'postgresql',
    host: 'localhost',
    port: 5432,
    username: '',
    password: '',
    database: '',
    file_path: '',
  });
  const [isCreating, setIsCreating] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  const handleChange = (field: keyof ConnectionConfig, value: any) => {
    setConfig((prev) => ({ ...prev, [field]: value }));
    // 清除验证错误
    setValidationErrors([]);
  };

  const handleDbTypeChange = (dbType: DBType) => {
    const defaultPorts: Record<DBType, number> = {
      postgresql: 5432,
      mysql: 3306,
      sqlite: 0,
    };
    
    const newConfig: Partial<ConnectionConfig> = {
      db_type: dbType,
      port: defaultPorts[dbType],
    };
    
    // SQLite 时清空不需要的字段
    if (dbType === 'sqlite') {
      newConfig.host = '';
      newConfig.port = 0;
      newConfig.username = '';
      newConfig.password = '';
    } else {
      newConfig.host = 'localhost';
      newConfig.file_path = '';
    }
    
    setConfig((prev) => ({ ...prev, ...newConfig }));
    setValidationErrors([]);
  };

  // 表单验证
  const validateForm = (): boolean => {
    const errors: string[] = [];
    
    if (!config.name.trim()) {
      errors.push('连接名称不能为空');
    }
    
    if (!config.database.trim()) {
      errors.push(config.db_type === 'sqlite' ? '数据库文件路径不能为空' : '数据库名称不能为空');
    }
    
    if (config.db_type !== 'sqlite') {
      if (!config.host?.trim()) {
        errors.push('主机地址不能为空');
      }
      if (!config.port || config.port <= 0) {
        errors.push('端口号必须大于0');
      }
      if (!config.username?.trim()) {
        errors.push('用户名不能为空');
      }
      if (!config.password?.trim()) {
        errors.push('密码不能为空');
      }
    }
    
    setValidationErrors(errors);
    return errors.length === 0;
  };

  const handleCreate = async () => {
    if (!validateForm()) {
      return;
    }
    
    setIsCreating(true);
    try {
      const result = await addConnection(config);
      if (result) {
        // 重置表单
        setConfig({
          name: '',
          db_type: 'postgresql',
          host: 'localhost',
          port: 5432,
          username: '',
          password: '',
          database: '',
          file_path: '',
        });
        setValidationErrors([]);
      }
    } finally {
      setIsCreating(false);
    }
  };

  const isSQLite = config.db_type === 'sqlite';

  return (
    <Card className="rounded-[24px] border-zinc-200 bg-white text-black shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-black">
          <span className="flex h-7 w-7 items-center justify-center rounded-2xl bg-black text-white">
            <Plus className="h-4 w-4" />
          </span>
          新建数据库连接
        </CardTitle>
        <CardDescription className="text-sm text-zinc-500">配置并连接到您的数据库</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {validationErrors.length > 0 && (
          <Alert variant="destructive" className="border-zinc-300 bg-zinc-100 text-black">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              <ul className="list-disc list-inside space-y-1">
                {validationErrors.map((error, index) => (
                  <li key={index}>{error}</li>
                ))}
              </ul>
            </AlertDescription>
          </Alert>
        )}

        <div className="text-xs uppercase tracking-[0.22em] text-zinc-500">Connection config</div>
        <div className="space-y-1.5">
          <Label htmlFor="name">连接名称 <span className="text-destructive">*</span></Label>
          <Input
            id="name"
            placeholder="例如：生产数据库"
            value={config.name}
            onChange={(e) => handleChange('name', e.target.value)}
            className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
          />
        </div>

        <div className="space-y-1.5">
          <Label htmlFor="db_type">数据库类型 <span className="text-destructive">*</span></Label>
          <Select value={config.db_type} onValueChange={handleDbTypeChange}>
            <SelectTrigger id="db_type" className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="postgresql">PostgreSQL</SelectItem>
              <SelectItem value="mysql">MySQL</SelectItem>
              <SelectItem value="sqlite">SQLite</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {!isSQLite && (
          <>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1.5">
                <Label htmlFor="host">主机地址 <span className="text-destructive">*</span></Label>
                <Input
                  id="host"
                  placeholder="localhost"
                  value={config.host}
                  onChange={(e) => handleChange('host', e.target.value)}
                  className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="port">端口 <span className="text-destructive">*</span></Label>
                <Input
                  id="port"
                  type="number"
                  value={config.port}
                  onChange={(e) => handleChange('port', parseInt(e.target.value) || 0)}
                  className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <div className="space-y-1.5">
              <Label htmlFor="username">用户名 <span className="text-destructive">*</span></Label>
              <Input
                id="username"
                placeholder="数据库用户名"
                value={config.username}
                onChange={(e) => handleChange('username', e.target.value)}
                className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
              />
              </div>

              <div className="space-y-1.5">
              <Label htmlFor="password">密码 <span className="text-destructive">*</span></Label>
              <Input
                id="password"
                type="password"
                placeholder="数据库密码"
                value={config.password}
                onChange={(e) => handleChange('password', e.target.value)}
                className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
              />
              </div>
            </div>
          </>
        )}

        <div className="space-y-1.5">
          <Label htmlFor="database">
            {isSQLite ? '数据库文件名' : '数据库名称'} <span className="text-destructive">*</span>
          </Label>
          <Input
            id="database"
            placeholder={isSQLite ? 'database.db' : 'my_database'}
            value={config.database}
            onChange={(e) => handleChange('database', e.target.value)}
            className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
          />
          {isSQLite && (
            <p className="text-xs text-zinc-500">
              仅填写文件名，如 database.db
            </p>
          )}
        </div>

        {isSQLite && (
          <div className="space-y-1.5">
            <Label htmlFor="file_path">文件路径（可选）</Label>
            <Input
              id="file_path"
              placeholder="/path/to/database.db"
              value={config.file_path}
              onChange={(e) => handleChange('file_path', e.target.value)}
              className="h-10 rounded-2xl border-zinc-200 bg-zinc-50 text-black placeholder:text-zinc-400"
            />
            <p className="text-xs text-zinc-500">
              如不填写，将使用默认路径
            </p>
          </div>
        )}

        <div className="flex gap-2 pt-1">
          <Button 
            onClick={handleCreate} 
            disabled={isCreating}
            className="h-10 flex-1 rounded-2xl bg-black text-white hover:bg-zinc-800"
          >
            {isCreating ? '创建中...' : '创建连接'}
          </Button>
        </div>
        <div className="flex items-start gap-2 rounded-2xl border border-zinc-200 bg-zinc-50 px-3 py-2 text-xs leading-5 text-zinc-600">
          <ShieldCheck className="h-4 w-4 text-black" />
          连接创建后，可在上方卡片里单独测试连接并同步 Schema。
        </div>
      </CardContent>
    </Card>
  );
}
