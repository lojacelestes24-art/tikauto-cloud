# TikAuto Cloud — Guia de Instalação

## Estrutura de arquivos

```
tikauto/
├── app.py              ← Backend Flask (API + rotas)
├── templates/
│   └── index.html      ← Painel web completo
├── data/               ← Criado automaticamente
│   ├── bcs.json
│   ├── tokens.json
│   └── jobs.json
├── logs/               ← Logs de cada job
├── requirements.txt
├── install.sh          ← Script de instalação automática
└── .env                ← Variáveis de ambiente (criar manualmente)
```

## Instalação no VPS (Ubuntu)

```bash
# 1. Suba os arquivos para o VPS (via FTP/SCP/Git)
scp -r . root@SEU_IP:/tmp/tikauto

# 2. No VPS, rode o instalador
cd /tmp/tikauto
bash install.sh

# 3. Edite o .env com seu App Secret
nano /var/www/tikauto/.env

# 4. Reinicie
systemctl restart tikauto
```

## .env obrigatório

```env
TIKTOK_APP_ID=7610282489889685520
TIKTOK_APP_SECRET=SEU_APP_SECRET_AQUI
TIKTOK_REDIRECT=https://boldies.site/oauth/callback
SECRET_KEY=qualquer-string-segura-aqui
```

## Configurar o redirect_uri no TikTok Developer

No painel do seu app TikTok em https://business-api.tiktok.com/portal:
- Redirect URI: `https://boldies.site/oauth/callback`

## Uso

### 1. Adicionar um BC
- Vá em "Business Centers" → "Adicionar BC"
- Coloque o nome e os Advertiser IDs (um por linha)

### 2. Conectar via OAuth
- Clique em "🔐 Conectar OAuth" no card do BC
- Clique "Abrir Autorização TikTok"
- Autorize no TikTok
- A URL vai redirecionar para `boldies.site/oauth/callback`
  - O sistema captura o `auth_code` automaticamente
  - Token salvo automaticamente no BC

### 3. Criar Contas em Massa
- Vá em "Criar Contas"
- Selecione o BC conectado
- Configure prefixo, quantidade, moeda e fuso
- Clique "Iniciar"

### 4. Criar Campanhas em Massa
- Vá em "Criar Campanhas"
- Selecione BC, contas, objetivo, segmentação
- Cole o código do post TikTok (Spark Ad)
- Configure URL, CTA, pixel
- Clique "Disparar Campanhas"

## Comandos úteis

```bash
# Ver logs ao vivo
journalctl -u tikauto -f

# Reiniciar após editar código
systemctl restart tikauto

# Status
systemctl status tikauto
```
