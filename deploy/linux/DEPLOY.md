# Debian 直部署

## 目录约定

- 项目目录：`/opt/nl2sql-agent`
- 前端构建输出：`/opt/nl2sql-agent/frontend-dist`
- 后端监听：`127.0.0.1:8000`
- Nginx 对外提供静态站点和 `/api` 反代

## 1. 安装系统依赖

```bash
apt update
apt install -y git python3 python3-venv python3-pip nginx nodejs npm
```

## 2. 拉代码

```bash
mkdir -p /opt
cd /opt
git clone https://github.com/Polaris418/NL2SQL_Agent.git nl2sql-agent
cd /opt/nl2sql-agent
```

## 3. 配置后端

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

建议在 `.env` 中至少确认这些值：

```env
APP_HOST=127.0.0.1
APP_PORT=8000
NL2SQL_MASTER_KEY=change-me
RAG_RERANKER_ENABLED=false
```

## 4. 构建前端

```bash
cd "/opt/nl2sql-agent/NL2SQL Agent Frontend Development"
npm install
npm run build
mkdir -p /opt/nl2sql-agent/frontend-dist
cp -r dist/* /opt/nl2sql-agent/frontend-dist/
```

## 5. 配置 systemd

```bash
cp /opt/nl2sql-agent/deploy/linux/nl2sql-agent.service /etc/systemd/system/nl2sql-agent.service
systemctl daemon-reload
systemctl enable nl2sql-agent
systemctl restart nl2sql-agent
systemctl status nl2sql-agent
```

## 6. 配置 Nginx

```bash
cp /opt/nl2sql-agent/deploy/linux/nl2sql-agent.nginx.conf /etc/nginx/sites-available/nl2sql-agent
ln -sf /etc/nginx/sites-available/nl2sql-agent /etc/nginx/sites-enabled/nl2sql-agent
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl restart nginx
```

## 7. 验证

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1/health
```

浏览器访问服务器公网 IP 或绑定域名即可。
