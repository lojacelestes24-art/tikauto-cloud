#!/bin/bash
# ─────────────────────────────────────────────────────
# TikAuto Cloud — Script de instalação (Ubuntu/Debian VPS)
# Rodar como root: bash install.sh
# ─────────────────────────────────────────────────────

set -e
APP_DIR="/var/www/tikauto"
DOMAIN="boldies.site"

echo "══════════════════════════════════════"
echo "  TikAuto Cloud — Instalação"
echo "══════════════════════════════════════"

# 1. Dependências
echo "→ Instalando dependências..."
apt-get update -qq
apt-get install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx

# 2. Diretório da app
echo "→ Criando diretório $APP_DIR..."
mkdir -p $APP_DIR
cp -r . $APP_DIR/
cd $APP_DIR

# 3. Virtualenv e pacotes
echo "→ Criando virtualenv..."
python3 -m venv venv
venv/bin/pip install -q -r requirements.txt

# 4. Arquivo de variáveis de ambiente
if [ ! -f .env ]; then
  echo "→ Criando .env..."
  cat > .env << 'ENV'
TIKTOK_APP_ID=7610282489889685520
TIKTOK_APP_SECRET=SEU_APP_SECRET_AQUI
TIKTOK_REDIRECT=https://boldies.site/oauth/callback
SECRET_KEY=mude-esta-chave-para-algo-seguro-2026
ENV
  echo "  ⚠  EDITE o arquivo $APP_DIR/.env com seu App Secret!"
fi

# 5. Systemd service
echo "→ Criando serviço systemd..."
cat > /etc/systemd/system/tikauto.service << SERVICE
[Unit]
Description=TikAuto Cloud
After=network.target

[Service]
User=www-data
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/gunicorn -w 2 -b 127.0.0.1:5000 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable tikauto
systemctl start tikauto

# 6. Nginx
echo "→ Configurando Nginx..."
cat > /etc/nginx/sites-available/tikauto << NGINX
server {
    listen 80;
    server_name $DOMAIN www.$DOMAIN;

    location / {
        proxy_pass         http://127.0.0.1:5000;
        proxy_set_header   Host \$host;
        proxy_set_header   X-Real-IP \$remote_addr;
        proxy_set_header   X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto \$scheme;
        proxy_read_timeout 300;
        proxy_connect_timeout 300;
    }
}
NGINX

ln -sf /etc/nginx/sites-available/tikauto /etc/nginx/sites-enabled/tikauto
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

# 7. SSL
echo "→ Instalando SSL com Let's Encrypt..."
certbot --nginx -d $DOMAIN -d www.$DOMAIN --non-interactive --agree-tos -m admin@$DOMAIN || \
  echo "  ⚠  SSL falhou — configure manualmente com: certbot --nginx -d $DOMAIN"

echo ""
echo "══════════════════════════════════════"
echo "  ✓ Instalação concluída!"
echo "  Site: https://$DOMAIN"
echo "  Logs: journalctl -u tikauto -f"
echo ""
echo "  PRÓXIMOS PASSOS:"
echo "  1. Edite $APP_DIR/.env com o App Secret"
echo "  2. systemctl restart tikauto"
echo "══════════════════════════════════════"
