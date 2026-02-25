from flask import Flask, render_template, jsonify, request
import threading
import subprocess
import sys
import requests as req
import time
import json
import os
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)

# ─── Estado global ────────────────────────────────────────────────────────────
processos    = {}        # {key: subprocess.Popen}
logs_pend    = []
rels_pend    = []
stop_flags   = {}
log_offsets  = {}        # {bc_id: int}

LOG_DIR    = os.path.join(os.path.dirname(__file__), 'logs')
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
DATA_DIR   = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(LOG_DIR,    exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR,   exist_ok=True)

# ─── Helpers de dados ─────────────────────────────────────────────────────────
def load_json(fname, default):
    p = os.path.join(DATA_DIR, fname)
    if os.path.exists(p):
        try:
            with open(p, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return default

def save_json(fname, data):
    p = os.path.join(DATA_DIR, fname)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_spark_posts():   return load_json('spark_posts.json', [])
def save_spark_posts(d): save_json('spark_posts.json', d)
def get_identities():    return load_json('identities.json', [])
def save_identities(d):  save_json('identities.json', d)

# ─── Worker monitoring ────────────────────────────────────────────────────────
def ler_logs_worker(bc_id):
    arq = os.path.join(LOG_DIR, f'live_{bc_id}.jsonl')
    if not os.path.exists(arq): return []
    offset = log_offsets.get(bc_id, 0)
    novos  = []
    with open(arq, 'r', encoding='utf-8') as f:
        f.seek(offset)
        for linha in f:
            linha = linha.strip()
            if linha:
                try: novos.append(json.loads(linha))
                except: pass
        log_offsets[bc_id] = f.tell()
    return novos

def monitorar_worker(bc_id, key):
    proc = processos.get(key)
    if not proc: return
    while proc.poll() is None:
        for e in ler_logs_worker(bc_id):
            logs_pend.append({'msg': e['msg'], 'type': e['type']})
        time.sleep(1)
    for e in ler_logs_worker(bc_id):
        logs_pend.append({'msg': e['msg'], 'type': e['type']})
    stop_flags.pop(key, None)
    processos.pop(key, None)

# ─── Log helpers ──────────────────────────────────────────────────────────────
def adicionar_log(msg, tipo='info'):
    logs_pend.append({'msg': msg, 'type': tipo})

def salvar_log_arquivo(bc_nome, tipo, conta, status, detalhe=''):
    horario = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    arq = os.path.join(LOG_DIR, f"{bc_nome.replace(' ','_')}_{tipo}.txt")
    with open(arq, 'a', encoding='utf-8') as f:
        f.write(f"[{status.upper()}] {horario} | {conta} | {detalhe}\n")

def adicionar_relatorio(bc_id, bc_nome, conta, tipo, status, detalhe=''):
    horario = datetime.now().strftime('%d/%m/%Y %H:%M')
    rels_pend.append({'bc_id':bc_id,'bc_nome':bc_nome,'conta':conta,
                      'tipo':tipo,'status':status,'detalhe':detalhe,'horario':horario})
    salvar_log_arquivo(bc_nome, tipo, conta, status, detalhe)

def conta_ja_processada(bc_nome, tipo, nome_conta):
    arq = os.path.join(LOG_DIR, f"{bc_nome.replace(' ','_')}_{tipo}.txt")
    if not os.path.exists(arq): return False
    with open(arq, 'r', encoding='utf-8') as f: c = f.read()
    return nome_conta in c and '[SUCESSO]' in c

def importar_contas_arquivo(bc_nome, tipo, caminho):
    horario = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    destino = os.path.join(LOG_DIR, f"{bc_nome.replace(' ','_')}_{tipo}.txt")
    importadas = 0
    try:
        with open(caminho, 'r', encoding='utf-8') as f: linhas = f.readlines()
        with open(destino, 'a', encoding='utf-8') as f:
            for linha in linhas:
                linha = linha.strip()
                if not linha: continue
                if '[SUCESSO]' in linha or '[FALHA]' in linha:
                    f.write(linha + '\n')
                else:
                    nome = linha.split(' - ')[0].strip()
                    if nome: f.write(f"[SUCESSO] {horario} | {nome} | importado\n")
                importadas += 1
    except Exception as e:
        return 0, str(e)
    return importadas, None

# ─── Routes base ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/version')
def version():
    return jsonify({'version': 'v5-full'})

# ─── Stats & Status ───────────────────────────────────────────────────────────
def calcular_estatisticas():
    stats = {}
    if not os.path.exists(LOG_DIR): return stats
    for fname in os.listdir(LOG_DIR):
        if not fname.endswith('.txt'): continue
        for t in ['criacao_conta', 'campanha']:
            sufixo = f'_{t}.txt'
            if fname.endswith(sufixo):
                bc = fname[:-len(sufixo)].replace('_', ' ')
                chave = f"{bc}|{t}"
                sucesso = falha = 0
                caminho = os.path.join(LOG_DIR, fname)
                try:
                    with open(caminho, 'r', encoding='utf-8') as f:
                        for linha in f:
                            if '[SUCESSO]' in linha: sucesso += 1
                            elif '[FALHA]' in linha:  falha   += 1
                except: pass
                stats[chave] = {'bc_nome':bc,'tipo':t,'sucesso':sucesso,'falha':falha,'total':sucesso+falha}
    return stats

@app.route('/api/stats')
def api_stats():
    stats = calcular_estatisticas()
    running = [k for k,p in processos.items() if p.poll() is None]
    return jsonify({'stats': list(stats.values()), 'running': running})

@app.route('/api/status')
def status():
    global logs_pend, rels_pend
    logs = logs_pend.copy(); logs_pend = []
    rels = rels_pend.copy(); rels_pend = []
    running = [k for k,p in processos.items() if p.poll() is None]
    totais = {}
    for fname in os.listdir(LOG_DIR):
        if fname.startswith('total_') and fname.endswith('.txt'):
            bc_id = fname.replace('total_','').replace('.txt','')
            try:
                with open(os.path.join(LOG_DIR, fname), 'r') as f:
                    totais[bc_id] = int(f.read().strip())
            except: pass
    return jsonify({'logs':logs,'relatorios':rels,'running':running,'totais_contas':totais})

# ─── Upload & Logs ────────────────────────────────────────────────────────────
@app.route('/api/upload-contas', methods=['POST'])
def upload_contas():
    if 'arquivo' not in request.files:
        return jsonify({'ok': False, 'message': 'Nenhum arquivo enviado'})
    arquivo = request.files['arquivo']
    bc_nome = request.form.get('bc_nome', 'geral')
    tipo    = request.form.get('tipo', 'campanha')
    if arquivo.filename == '':
        return jsonify({'ok': False, 'message': 'Arquivo inválido'})
    caminho = os.path.join(UPLOAD_DIR, secure_filename(arquivo.filename))
    arquivo.save(caminho)
    importadas, erro = importar_contas_arquivo(bc_nome, tipo, caminho)
    if erro: return jsonify({'ok': False, 'message': f'Erro: {erro}'})
    adicionar_log(f'[{bc_nome}] {importadas} contas importadas!', 'success')
    return jsonify({'ok': True, 'message': f'{importadas} contas importadas com sucesso!'})

@app.route('/api/contas-processadas')
def contas_processadas():
    bc_nome = request.args.get('bc_nome','')
    tipo    = request.args.get('tipo','campanha')
    arq = os.path.join(LOG_DIR, f"{bc_nome.replace(' ','_')}_{tipo}.txt")
    if not os.path.exists(arq):
        return jsonify({'total':0,'sucesso':0,'falha':0,'contas':[]})
    contas = []; sucesso = falha = 0
    with open(arq, 'r', encoding='utf-8') as f:
        for linha in f.readlines():
            linha = linha.strip()
            if not linha: continue
            partes = linha.split(' | ')
            status = 'sucesso' if '[SUCESSO]' in linha else 'falha'
            nome   = partes[1].strip() if len(partes) > 1 else linha
            horario= partes[0].replace('[SUCESSO]','').replace('[FALHA]','').strip() if partes else ''
            if status == 'sucesso': sucesso += 1
            else: falha += 1
            contas.append({'nome':nome,'status':status,'horario':horario})
    return jsonify({'total':len(contas),'sucesso':sucesso,'falha':falha,'contas':contas[-50:]})

# ─── Spark Posts CRUD ─────────────────────────────────────────────────────────
@app.route('/api/spark-posts', methods=['GET'])
def api_get_sparks():
    return jsonify(get_spark_posts())

@app.route('/api/spark-posts', methods=['POST'])
def api_add_spark():
    import uuid, re
    d      = request.json
    value  = d.get('value','').strip()
    # Extrai item_id da URL se for link TikTok
    item_id = d.get('item_id','')
    if not item_id:
        m = re.search(r'/video/(\d{10,25})', value)
        if m: item_id = m.group(1)
        elif re.match(r'^\d{10,25}$', value): item_id = value
    posts = get_spark_posts()
    post  = {
        'id'      : str(uuid.uuid4())[:8],
        'label'   : d.get('label', value[:40]),
        'value'   : value,
        'item_id' : item_id,
        'criado_em': datetime.now().isoformat()
    }
    posts.append(post)
    save_spark_posts(posts)
    return jsonify({'ok': True, 'post': post})

@app.route('/api/spark-posts/<post_id>', methods=['DELETE'])
def api_del_spark(post_id):
    save_spark_posts([p for p in get_spark_posts() if p['id'] != post_id])
    return jsonify({'ok': True})

# ─── Identities CRUD ──────────────────────────────────────────────────────────
@app.route('/api/identities', methods=['GET'])
def api_get_identities():
    return jsonify(get_identities())

@app.route('/api/identities', methods=['POST'])
def api_add_identity():
    import uuid
    d = request.json
    idents = get_identities()
    ident  = {
        'id'         : str(uuid.uuid4())[:8],
        'label'      : d.get('label',''),
        'identity_id': d.get('identity_id',''),
        'bc_id'      : d.get('bc_id','7607905792628621313'),
        'criado_em'  : datetime.now().isoformat()
    }
    idents.append(ident)
    save_identities(idents)
    return jsonify({'ok': True, 'identity': ident})

@app.route('/api/identities/<ident_id>', methods=['DELETE'])
def api_del_identity(ident_id):
    save_identities([i for i in get_identities() if i['id'] != ident_id])
    return jsonify({'ok': True})

# ─── System ───────────────────────────────────────────────────────────────────
@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    data = request.json
    url  = data.get('url','http://local.adspower.net:50325')
    try:
        r = req.get(f"{url}/status", timeout=5)
        return jsonify({'ok': True, 'message': '✅ ADS Power conectado!'})
    except:
        return jsonify({'ok': False, 'message': '❌ Não foi possível conectar ao ADS Power'})

@app.route('/api/stop', methods=['POST'])
def stop():
    for fname in os.listdir(LOG_DIR):
        if fname.startswith('live_') and fname.endswith('.jsonl'):
            bc_id     = fname.replace('live_','').replace('.jsonl','')
            stop_file = os.path.join(LOG_DIR, f'stop_{bc_id}.flag')
            open(stop_file,'w').close()
    for key, proc in list(processos.items()):
        try:
            if proc.poll() is None: proc.terminate()
        except: pass
    processos.clear()
    adicionar_log('Todos os processos foram interrompidos', 'warn')
    return jsonify({'ok': True})

# ─── Start Contas ─────────────────────────────────────────────────────────────
@app.route('/api/start-contas', methods=['POST'])
def start_contas():
    data       = request.json
    profile_id = data['profile_id']
    quantidade = data.get('quantidade', 100)
    bc_id      = str(data['bc_id'])
    bc_nome    = data['bc_nome']
    key        = f"contas_{bc_id}"

    if key in processos and processos[key].poll() is None:
        return jsonify({'ok': False, 'message': f'{bc_nome} já está em execução!'})

    stop_file = os.path.join(LOG_DIR, f'stop_{bc_id}.flag')
    if os.path.exists(stop_file): os.remove(stop_file)
    live_file = os.path.join(LOG_DIR, f'live_{bc_id}.jsonl')
    if os.path.exists(live_file): os.remove(live_file)
    log_offsets[bc_id] = 0

    worker_path = os.path.join(os.path.dirname(__file__), 'worker.py')
    proc = subprocess.Popen(
        [sys.executable, worker_path, 'contas', profile_id, bc_id, bc_nome, str(quantidade)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    processos[key]  = proc
    stop_flags[key] = False
    threading.Thread(target=monitorar_worker, args=(bc_id, key), daemon=True).start()
    return jsonify({'ok': True, 'message': f'Processo iniciado para {bc_nome}'})

# ─── Start Campanhas (via ADS Power / Playwright) ─────────────────────────────
@app.route('/api/start-campanhas', methods=['POST'])
def start_campanhas():
    data        = request.json
    profile_id  = data['profile_id']
    post_code   = data['post_code']
    simultaneas = data.get('simultaneas', 1)
    bc_id       = str(data['bc_id'])
    bc_nome     = data['bc_nome']

    # Novos campos opcionais
    num_conjuntos   = int(data.get('num_conjuntos', 1))
    data_inicio     = data.get('data_inicio', '')   # formato: YYYY-MM-DD HH:MM
    data_fim        = data.get('data_fim', '')       # formato: YYYY-MM-DD HH:MM
    objetivo        = data.get('objetivo', 'VIDEO_VIEWS')
    orcamento       = data.get('orcamento', '')
    paises          = data.get('paises', 'BR')

    key = f"camp_{bc_id}"
    if key in processos and processos[key].poll() is None:
        return jsonify({'ok': False, 'message': f'{bc_nome} já está em execução!'})

    stop_file = os.path.join(LOG_DIR, f'stop_{bc_id}.flag')
    if os.path.exists(stop_file): os.remove(stop_file)
    live_file = os.path.join(LOG_DIR, f'live_{bc_id}.jsonl')
    if os.path.exists(live_file): os.remove(live_file)
    log_offsets[bc_id] = 0

    # Contas selecionadas pelo frontend (ID + nome via API)
    advertiser_ids = data.get('advertiser_ids', [])  # lista selecionada, vazio = todas

    # Salva config da campanha em arquivo para o worker ler
    cfg = {
        'post_code'      : post_code,
        'simultaneas'    : simultaneas,
        'num_conjuntos'  : num_conjuntos,
        'data_inicio'    : data_inicio,
        'data_fim'       : data_fim,
        'objetivo'       : objetivo,
        'orcamento'      : orcamento,
        'paises'         : paises,
        'advertiser_ids' : advertiser_ids,
    }
    cfg_file = os.path.join(LOG_DIR, f'cfg_{bc_id}.json')
    with open(cfg_file, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False)

    worker_path = os.path.join(os.path.dirname(__file__), 'worker.py')
    proc = subprocess.Popen(
        [sys.executable, worker_path, 'campanhas', profile_id, bc_id, bc_nome, post_code, str(simultaneas)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    processos[key]  = proc
    stop_flags[key] = False
    threading.Thread(target=monitorar_worker, args=(bc_id, key), daemon=True).start()
    return jsonify({'ok': True, 'message': f'Campanhas iniciadas para {bc_nome}'})

# ─── Exportar Relatório ───────────────────────────────────────────────────────
@app.route('/api/export-relatorio')
def export_relatorio():
    stats = calcular_estatisticas()
    return jsonify({'stats': list(stats.values()), 'gerado_em': datetime.now().isoformat()})

# ─── Worker inline (fallback se worker.py não existir) ─────────────────────────
def run_criar_contas(profile_id, quantidade, bc_id, bc_nome, key):
    from playwright.sync_api import sync_playwright
    import random, string

    adicionar_log(f'[{bc_nome}] Conectando ao ADS Power...', 'info')
    try:
        res = req.get('http://local.adspower.net:50325/api/v1/browser/start',
                      params={'user_id': profile_id}).json()
        if res['code'] != 0:
            adicionar_log(f'[{bc_nome}] Erro ao conectar: {res}', 'error'); return

        ws_url = res['data']['ws']['puppeteer']
        adicionar_log(f'[{bc_nome}] Conectado! Iniciando criação...', 'success')

        def gerar_nome():
            return f"TKTK_{''.join(random.choices(string.ascii_uppercase,k=5))}_{''.join(random.choices(string.digits,k=3))}"

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws_url)
            context = browser.contexts[0]
            page    = context.pages[0]
            contas  = 0

            while contas < quantidade:
                if stop_flags.get(key):
                    adicionar_log(f'[{bc_nome}] Processo interrompido!', 'warn'); return
                adicionar_log(f'[{bc_nome}] Criando conta {contas+1}/{quantidade}...', 'info')
                try:
                    page.goto('https://business.tiktok.com/manage/accounts/adv', wait_until='domcontentloaded', timeout=60000)
                    page.wait_for_selector('text=Add advertiser account', timeout=30000)
                    time.sleep(random.uniform(2,3))
                    page.click('text=Add advertiser account'); time.sleep(random.uniform(2,3))
                    page.wait_for_selector('text=Create New')
                    page.click('text=Create New'); time.sleep(random.uniform(2,3))
                    page.wait_for_selector('text=Next')
                    page.click('text=Next'); time.sleep(random.uniform(2,3))
                    page.mouse.wheel(0, 600); time.sleep(random.uniform(2,3))
                    nome = gerar_nome()
                    page.wait_for_selector("input[placeholder='Enter ad account name']")
                    page.fill("input[placeholder='Enter ad account name']", nome); time.sleep(random.uniform(2,3))
                    page.click('text=Select a time zone'); time.sleep(random.uniform(2,3))
                    page.fill("input[placeholder='Search']", 'Sao Paulo'); time.sleep(random.uniform(2,3))
                    page.click('text=Sao Paulo Time'); time.sleep(random.uniform(2,3))
                    criada = False
                    for t in range(9):
                        page.click('text=Confirm'); time.sleep(random.uniform(2,3))
                        if page.locator('text=Create advertiser account').count() > 0:
                            adicionar_log(f'[{bc_nome}] Tentativa {t+1}/9...', 'info')
                        else:
                            criada = True; break
                    if not criada:
                        espera = random.uniform(180,300)
                        adicionar_log(f'[{bc_nome}] Limite. Aguardando {round(espera/60,1)}min...', 'warn')
                        time.sleep(espera); continue
                    if page.locator('text=Skip').count() > 0:
                        page.click('text=Skip'); time.sleep(random.uniform(2,3))
                    contas += 1
                    adicionar_log(f'[{bc_nome}] ✅ {nome} criada! ({contas}/{quantidade})', 'success')
                    adicionar_relatorio(bc_id, bc_nome, nome, 'criacao_conta', 'sucesso')
                except Exception as e:
                    espera = random.uniform(180,300)
                    adicionar_log(f'[{bc_nome}] ❌ Erro: {str(e)[:60]}. Aguardando...', 'error')
                    adicionar_relatorio(bc_id, bc_nome, f'conta_{contas+1}', 'criacao_conta', 'falha', str(e)[:100])
                    time.sleep(espera)
        adicionar_log(f'[{bc_nome}] ✅ Concluído! {contas} contas criadas.', 'success')
    except Exception as e:
        adicionar_log(f'[{bc_nome}] ❌ Erro fatal: {str(e)[:100]}', 'error')

def run_criar_campanhas(profile_id, post_code, simultaneas, bc_id, bc_nome, key):
    from playwright.sync_api import sync_playwright
    import math, json

    # Lê config extra se disponível
    cfg_file = os.path.join(LOG_DIR, f'cfg_{bc_id}.json')
    cfg = {}
    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, 'r', encoding='utf-8') as f: cfg = json.load(f)
        except: pass

    num_conjuntos = int(cfg.get('num_conjuntos', 1))
    data_inicio   = cfg.get('data_inicio', '')
    data_fim      = cfg.get('data_fim', '')
    objetivo      = cfg.get('objetivo', 'VIDEO_VIEWS')

    adicionar_log(f'[{bc_nome}] Iniciando campanhas... ({num_conjuntos} conjunto(s))', 'info')
    if data_inicio: adicionar_log(f'[{bc_nome}] Início agendado: {data_inicio}', 'info')
    if data_fim:    adicionar_log(f'[{bc_nome}] Fim agendado: {data_fim}', 'info')

    try:
        res = req.get('http://local.adspower.net:50325/api/v1/browser/start',
                      params={'user_id': profile_id}).json()
        if res['code'] != 0:
            adicionar_log(f'[{bc_nome}] Erro ao conectar', 'error'); return

        ws_url = res['data']['ws']['puppeteer']

        def clicar_ks(page, texto):
            return page.evaluate(f"""
                () => {{
                    function b(r) {{
                        let els = r.querySelectorAll('*');
                        for (let el of els) {{
                            let txt = (el.innerText||'').trim();
                            if (txt === '{texto}' && el.tagName.includes('KS-BUTTON')) {{
                                el.click(); return el.tagName;
                            }}
                            if (el.shadowRoot) {{ let x=b(el.shadowRoot); if(x) return x; }}
                        }}
                        return false;
                    }}
                    return b(document);
                }}
            """)

        with sync_playwright() as p:
            browser    = p.chromium.connect_over_cdp(ws_url)
            context    = browser.contexts[0]
            lista_page = context.pages[0]

            lista_page.goto('https://business.tiktok.com/manage/accounts/adv', wait_until='domcontentloaded')
            time.sleep(10)

            total_txt  = lista_page.evaluate("""
                () => {
                    let els = document.querySelectorAll('*');
                    for (let el of els) {
                        if (el.children.length === 0 && el.textContent.includes('Records in Total'))
                            return el.textContent;
                    }
                    return '';
                }
            """)
            numeros      = ''.join(filter(str.isdigit, total_txt.split('Records')[0]))
            total_contas = int(numeros) if numeros else 82
            total_pags   = math.ceil(total_contas / 10)
            adicionar_log(f'[{bc_nome}] {total_contas} contas em {total_pags} páginas', 'info')

            conta_global = 1
            for pag in range(1, total_pags + 1):
                if stop_flags.get(key): break
                if pag > 1:
                    lista_page.evaluate(f"""
                        () => {{
                            let els = document.querySelectorAll('*');
                            for (let el of els) {{
                                if (el.innerText && el.innerText.trim() === '{pag}' && el.offsetParent !== null) {{
                                    let r = el.getBoundingClientRect();
                                    if (r.width < 50 && r.height < 50) {{ el.click(); return true; }}
                                }}
                            }}
                            return false;
                        }}
                    """)
                    time.sleep(8)

                botoes = lista_page.locator('text=Go to Ads Manager').all()
                for idx in range(len(botoes)):
                    if stop_flags.get(key): break
                    dashboard = None
                    nome_conta = f'conta_{conta_global}'
                    try:
                        botoes = lista_page.locator('text=Go to Ads Manager').all()
                        with context.expect_page(timeout=20000) as pi:
                            botoes[idx].click()
                        dashboard = pi.value
                        time.sleep(10)
                        adicionar_log(f'[{bc_nome}] Processando {conta_global}/{total_contas}...', 'info')

                        if conta_ja_processada(bc_nome, 'campanha', nome_conta):
                            adicionar_log(f'[{bc_nome}] ⏭ {nome_conta} já processada', 'warn')
                            if dashboard:
                                try: dashboard.close()
                                except: pass
                            conta_global += 1
                            continue

                        # Fecha popups
                        try: dashboard.evaluate("""() => { let els = document.querySelectorAll('*'); for (let el of els) { if (el.innerText && el.innerText.trim() === 'Entendi') { el.click(); return true; } } }""")
                        except: pass

                        # Cria campanha
                        dashboard.click('.operation-create-btn', timeout=12000)
                        time.sleep(6)

                        # Seleciona objetivo
                        obj_map = {
                            'VIDEO_VIEWS': ['Visualizações de vídeo', 'Video Views'],
                            'REACH':       ['Alcance', 'Reach'],
                            'TRAFFIC':     ['Tráfego', 'Traffic'],
                            'ENGAGEMENT':  ['Engajamento', 'Engagement'],
                        }
                        obj_labels = obj_map.get(objetivo, ['Visualizações de vídeo', 'Video Views'])
                        selecionado = False
                        deadline = time.time() + 15
                        while time.time() < deadline and not selecionado:
                            for lbl in obj_labels:
                                clicou = dashboard.evaluate(f"""() => {{ let els = document.querySelectorAll('*'); for (let e of els) {{ if (e.innerText && e.innerText.trim() === '{lbl}') {{ e.click(); return true; }} }} return false; }}""")
                                if clicou: selecionado = True; break
                            if not selecionado: time.sleep(0.5)
                        if not selecionado: raise Exception('Objetivo não encontrado')

                        time.sleep(3)
                        clicar_ks(dashboard, 'Continuar')
                        time.sleep(4)

                        # Configura data início se definida
                        if data_inicio:
                            try:
                                # Tenta preencher campo de data/hora de início
                                dashboard.evaluate(f"""
                                    () => {{
                                        let inputs = document.querySelectorAll('input[type=datetime-local], input[placeholder*="data"], input[placeholder*="date"], input[placeholder*="start"]');
                                        for (let inp of inputs) {{
                                            inp.value = '{data_inicio}';
                                            inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                                            inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                                            return true;
                                        }}
                                        return false;
                                    }}
                                """)
                                adicionar_log(f'[{bc_nome}] Data início configurada: {data_inicio}', 'info')
                            except: pass

                        # Configura data fim se definida
                        if data_fim:
                            try:
                                dashboard.evaluate(f"""
                                    () => {{
                                        let inputs = document.querySelectorAll('input[type=datetime-local], input[placeholder*="end"], input[placeholder*="fim"]');
                                        for (let inp of inputs) {{
                                            inp.value = '{data_fim}';
                                            inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                                            inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                                            return true;
                                        }}
                                        return false;
                                    }}
                                """)
                                adicionar_log(f'[{bc_nome}] Data fim configurada: {data_fim}', 'info')
                            except: pass

                        clicar_ks(dashboard, 'Continuar')
                        time.sleep(4)

                        try: dashboard.evaluate("""() => { let els = document.querySelectorAll('*'); for (let el of els) { if (el.innerText && el.innerText.trim() === 'Entendi') { el.click(); return true; } } }""")
                        except: pass
                        time.sleep(3)

                        # Cria múltiplos conjuntos se necessário
                        for conjunto_idx in range(num_conjuntos):
                            if conjunto_idx > 0:
                                # Clica em "Adicionar conjunto de anúncios"
                                try:
                                    clicou = dashboard.evaluate("""
                                        () => {
                                            let els = document.querySelectorAll('*');
                                            for (let el of els) {
                                                let txt = (el.innerText||'').trim();
                                                if ((txt.includes('Add Ad Group') || txt.includes('Adicionar conjunto')) && el.offsetParent !== null) {
                                                    el.click(); return true;
                                                }
                                            }
                                            return false;
                                        }
                                    """)
                                    if clicou:
                                        time.sleep(3)
                                        adicionar_log(f'[{bc_nome}] Conjunto {conjunto_idx+1}/{num_conjuntos} adicionado', 'info')
                                except: pass

                        # Configura criativo
                        dashboard.evaluate("""() => { let els = document.querySelectorAll('*'); for (let el of els) { if (el.innerText && el.innerText.trim() === 'Postagem autorizada' && el.offsetParent !== null) { let r = el.getBoundingClientRect(); if (r.width < 300 && r.height < 60) { el.click(); return true; } } } return false; }""")
                        time.sleep(5)

                        input_el = None
                        for sel in ["textarea", "input[type='text']"]:
                            try:
                                el = dashboard.locator(sel).first
                                if el.is_visible(): input_el = el; break
                            except: pass
                        if not input_el: raise Exception('Campo código não encontrado')

                        input_el.click(force=True); time.sleep(1)
                        input_el.fill(post_code);   time.sleep(2)

                        clicou = clicar_ks(dashboard, 'Pesquisar')
                        if not clicou: clicar_ks(dashboard, 'Search')
                        time.sleep(8)
                        clicar_ks(dashboard, 'Confirmar')
                        time.sleep(12)
                        dashboard.reload(wait_until='domcontentloaded', timeout=30000)
                        time.sleep(8)

                        for t in range(3):
                            clicou = dashboard.evaluate("""() => { function b(r) { let els = r.querySelectorAll('*'); for (let el of els) { let txt = (el.innerText||'').trim(); if ((txt.includes('Vídeos e imagens')||txt.includes('Videos e imagens')) && el.tagName.includes('KS-BUTTON')) { el.click(); return el.tagName; } if (el.shadowRoot) { let x=b(el.shadowRoot); if(x) return x; } } return false; } return b(document); }""")
                            if clicou: break
                            time.sleep(5)
                        if not clicou: raise Exception('Vídeos e imagens não encontrado')
                        time.sleep(5)

                        dashboard.evaluate("""() => { let el = document.querySelector('div.item'); if (el) { el.click(); return true; } return false; }""")
                        time.sleep(3)
                        dashboard.evaluate("""() => { let btns = document.querySelectorAll('button'); for (let btn of btns) { let txt = (btn.innerText||'').trim(); if (txt==='Confirm'||txt==='Confirmar') { btn.click(); return true; } } return false; }""")
                        time.sleep(5)
                        clicar_ks(dashboard, 'Continuar')
                        time.sleep(5)
                        clicar_ks(dashboard, 'Publicar')

                        try: dashboard.wait_for_url('**/manage/campaign**', timeout=30000)
                        except: pass
                        try: dashboard.wait_for_load_state('domcontentloaded', timeout=30000)
                        except: pass
                        time.sleep(5)

                        for t in range(12):
                            toggle = dashboard.evaluate("""() => { let el = document.querySelector('.vi-switch.is-checked'); if (el) { let r = el.getBoundingClientRect(); if (r.width > 0) return true; } return false; }""")
                            if toggle: break
                            time.sleep(5)

                        for t in range(3):
                            dashboard.evaluate("""() => { let el = document.querySelector('.vi-switch.is-checked'); if (el) { el.click(); return true; } return false; }""")
                            time.sleep(3)
                            ainda = dashboard.evaluate("""() => { return document.querySelector('.vi-switch.is-checked') ? true : false; }""")
                            if not ainda: break

                        adicionar_log(f'[{bc_nome}] ✅ Conta {conta_global} finalizada! ({num_conjuntos} conjunto(s))', 'success')
                        adicionar_relatorio(bc_id, bc_nome, nome_conta, 'campanha', 'sucesso')

                    except Exception as e:
                        adicionar_log(f'[{bc_nome}] ❌ Erro conta {conta_global}: {str(e)[:60]}', 'error')
                        adicionar_relatorio(bc_id, bc_nome, nome_conta, 'campanha', 'falha', str(e)[:100])
                    finally:
                        if dashboard:
                            try: dashboard.close()
                            except: pass
                        try: lista_page.bring_to_front()
                        except: pass
                        time.sleep(4)
                    conta_global += 1

        adicionar_log(f'[{bc_nome}] ✅ Processo de campanhas concluído!', 'success')
    except Exception as e:
        adicionar_log(f'[{bc_nome}] ❌ Erro fatal: {str(e)[:100]}', 'error')

# ─── TikTok API helpers ───────────────────────────────────────────────────────
TIKTOK_API   = 'https://business-api.tiktok.com/open_api/v1.3'
TIKTOK_APP_ID     = os.environ.get('TIKTOK_APP_ID', '7610282489889685520')
TIKTOK_APP_SECRET = os.environ.get('TIKTOK_APP_SECRET', '')
TIKTOK_REDIRECT   = os.environ.get('TIKTOK_REDIRECT', 'https://boldies.site/oauth/callback')

def get_bcs_api():    return load_json('bcs_api.json', [])
def save_bcs_api(d):  save_json('bcs_api.json', d)
def get_tokens():     return load_json('tokens.json', {})
def save_tokens(d):   save_json('tokens.json', d)

def tt_get(endpoint, token, advertiser_id, params=None):
    url = f"{TIKTOK_API}/{endpoint}/"
    p = {'advertiser_id': advertiser_id}
    if params: p.update(params)
    try:
        r = req.get(url, headers={'Access-Token': token}, params=p, timeout=15)
        return r.json()
    except Exception as e:
        return {'code': -1, 'message': str(e)}

def tt_post(endpoint, token, payload):
    url = f"{TIKTOK_API}/{endpoint}/"
    try:
        r = req.post(url, headers={'Access-Token': token, 'Content-Type': 'application/json'}, json=payload, timeout=30)
        return r.json()
    except Exception as e:
        return {'code': -1, 'message': str(e)}

def get_token_for_bc(bc_id):
    return get_tokens().get(str(bc_id), {}).get('access_token')

# ─── OAuth ────────────────────────────────────────────────────────────────────
@app.route('/api/oauth/url')
def oauth_url():
    bc_id = request.args.get('bc_id', 'unknown')
    url = (f"https://business-api.tiktok.com/portal/auth"
           f"?app_id={TIKTOK_APP_ID}&state=bcid_{bc_id}&redirect_uri={TIKTOK_REDIRECT}")
    return jsonify({'ok': True, 'url': url})

@app.route('/oauth/callback')
def oauth_callback():
    auth_code = request.args.get('auth_code')
    state     = request.args.get('state', '')
    error     = request.args.get('error_code')
    if error or not auth_code:
        return render_template('index.html')
    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return render_template('index.html')
    bc_id = None
    if state.startswith('bcid_'):
        try: bc_id = state.replace('bcid_', '')
        except: pass
    token_data = result['data']
    tokens = get_tokens()
    key = str(bc_id) if bc_id else str(time.time())
    tokens[key] = {'access_token': token_data['access_token'], 'saved_at': datetime.now().isoformat()}
    save_tokens(tokens)
    # Atualiza BC
    bcs = get_bcs_api()
    for bc in bcs:
        if str(bc.get('tiktokBcId','')) == str(bc_id):
            bc['token_ok'] = True
            break
    save_bcs_api(bcs)
    return render_template('index.html')

@app.route('/api/oauth/exchange', methods=['POST'])
def oauth_exchange():
    auth_code = request.json.get('auth_code')
    bc_id     = request.json.get('bc_id')
    if not auth_code:
        return jsonify({'ok': False, 'error': 'auth_code obrigatorio'})
    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return jsonify(result)
    token_data = result['data']
    tokens = get_tokens()
    key = str(bc_id) if bc_id else str(time.time())
    tokens[key] = {'access_token': token_data['access_token'], 'saved_at': datetime.now().isoformat()}
    save_tokens(tokens)
    # Atualiza token_ok no BC
    bcs = get_bcs_api()
    for bc in bcs:
        if str(bc.get('tiktokBcId','')) == str(bc_id):
            bc['token_ok'] = True
            break
    save_bcs_api(bcs)
    return jsonify({'ok': True})

def _exchange_token(auth_code):
    try:
        r = req.post(f"{TIKTOK_API}/oauth2/access_token/", json={
            'app_id': TIKTOK_APP_ID, 'secret': TIKTOK_APP_SECRET,
            'auth_code': auth_code, 'grant_type': 'authorization_code'
        }, timeout=15)
        d = r.json()
        if d.get('code') == 0:
            return {'ok': True, 'data': d['data']}
        return {'ok': False, 'error': d.get('message', 'Erro API TikTok')}
    except Exception as e:
        return {'ok': False, 'error': str(e)}

# ─── BCs API CRUD ──────────────────────────────────────────────────────────────
@app.route('/api/bcs-api', methods=['GET'])
def api_get_bcs():
    bcs    = get_bcs_api()
    tokens = get_tokens()
    for bc in bcs:
        bc['token_ok'] = str(bc.get('tiktokBcId','')) in tokens
    return jsonify(bcs)

@app.route('/api/bcs-api', methods=['POST'])
def api_add_bc():
    d    = request.json
    bcs  = get_bcs_api()
    bc   = {
        'id'         : str(int(time.time() * 1000)),
        'nome'       : d.get('nome',''),
        'profileId'  : d.get('profileId',''),   # ADS Power
        'tiktokBcId' : d.get('tiktokBcId',''), # TikTok API
        'token_ok'   : False,
        'criado_em'  : datetime.now().isoformat()
    }
    bcs.append(bc)
    save_bcs_api(bcs)
    return jsonify({'ok': True, 'bc': bc})

@app.route('/api/bcs-api/<bc_id>', methods=['DELETE'])
def api_del_bc(bc_id):
    save_bcs_api([b for b in get_bcs_api() if b['id'] != bc_id])
    tokens = get_tokens()
    # Remove token pelo tiktokBcId
    bcs = get_bcs_api()
    for bc in bcs:
        if bc['id'] == bc_id:
            tokens.pop(str(bc.get('tiktokBcId','')), None)
    save_tokens(tokens)
    return jsonify({'ok': True})

# ─── Advertiser IDs do BC ─────────────────────────────────────────────────────
@app.route('/api/bcs-api/<bc_id>/sync', methods=['POST'])
def api_sync_bc(bc_id):
    bcs = get_bcs_api()
    bc  = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc:
        return jsonify({'ok': False, 'error': 'BC não encontrado'})
    token = get_token_for_bc(bc.get('tiktokBcId',''))
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token. Conecte via OAuth primeiro.'})
    # Busca advertiser IDs
    r = req.get(f"{TIKTOK_API}/oauth2/advertiser/get/",
                headers={'Access-Token': token},
                params={'app_id': TIKTOK_APP_ID, 'secret': TIKTOK_APP_SECRET},
                timeout=15)
    d = r.json()
    if d.get('code') != 0:
        return jsonify({'ok': False, 'error': d.get('message','Erro API')})
    adv_list = d['data'].get('list', [])
    adv_ids  = [str(a['advertiser_id']) for a in adv_list]
    # Busca nomes
    accounts = {}
    for adv in adv_list:
        accounts[str(adv['advertiser_id'])] = {
            'name'    : adv.get('advertiser_name', str(adv['advertiser_id'])),
            'currency': adv.get('currency',''),
        }
    bc['advertiser_ids'] = adv_ids
    bc['accounts']       = accounts
    save_bcs_api(bcs)
    return jsonify({'ok': True, 'total': len(adv_ids), 'advertiser_ids': adv_ids})

# ─── Visão Geral: campanhas + gastos ──────────────────────────────────────────
@app.route('/api/bcs-api/<bc_id>/overview', methods=['POST'])
def api_overview(bc_id):
    bcs = get_bcs_api()
    bc  = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc:
        return jsonify({'ok': False, 'error': 'BC não encontrado'})
    token = get_token_for_bc(bc.get('tiktokBcId',''))
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token OAuth'})

    d          = request.json or {}
    start_date = d.get('start_date', datetime.now().strftime('%Y-%m-%d'))
    end_date   = d.get('end_date',   datetime.now().strftime('%Y-%m-%d'))
    adv_ids    = bc.get('advertiser_ids', [])
    accounts   = bc.get('accounts', {})

    if not adv_ids:
        return jsonify({'ok': False, 'error': 'Sincronize o BC primeiro para carregar as contas.'})

    import json as _json
    result = []

    for adv_id in adv_ids:
        acc_name = accounts.get(str(adv_id), {}).get('name', str(adv_id))

        # Campanhas
        rc = tt_get('campaign/get', token, adv_id, {'page_size': 100})
        campaigns = []
        if rc.get('code') == 0:
            for camp in rc.get('data', {}).get('list', []):
                camp_id   = str(camp.get('campaign_id',''))
                camp_name = camp.get('campaign_name','')
                camp_status = camp.get('operation_status','')

                # Conjuntos (ad groups)
                rg = tt_get('adgroup/get', token, adv_id, {
                    'campaign_id': camp_id, 'page_size': 50
                })
                adgroups = []
                if rg.get('code') == 0:
                    for ag in rg.get('data', {}).get('list', []):
                        adgroups.append({
                            'id'    : str(ag.get('adgroup_id','')),
                            'name'  : ag.get('adgroup_name',''),
                            'status': ag.get('operation_status',''),
                        })

                # Gastos do período
                spend = 0
                impressions = 0
                clicks = 0
                try:
                    rr = tt_get('report/integrated/get', token, adv_id, {
                        'report_type'  : 'BASIC',
                        'dimensions'   : _json.dumps(['campaign_id']),
                        'metrics'      : _json.dumps(['spend','impressions','clicks','reach']),
                        'data_level'   : 'AUCTION_CAMPAIGN',
                        'start_date'   : start_date,
                        'end_date'     : end_date,
                        'filtering'    : _json.dumps([{'field_name':'campaign_id','filter_type':'IN','filter_value':_json.dumps([camp_id])}]),
                        'page_size'    : 10,
                    })
                    if rr.get('code') == 0:
                        rows = rr.get('data',{}).get('list',[])
                        for row in rows:
                            m = row.get('metrics',{})
                            spend       += float(m.get('spend',0) or 0)
                            impressions += int(m.get('impressions',0) or 0)
                            clicks      += int(m.get('clicks',0) or 0)
                except: pass

                campaigns.append({
                    'id'         : camp_id,
                    'name'       : camp_name,
                    'status'     : camp_status,
                    'adgroups'   : adgroups,
                    'spend'      : round(spend, 2),
                    'impressions': impressions,
                    'clicks'     : clicks,
                })

        result.append({
            'advertiser_id'  : str(adv_id),
            'advertiser_name': acc_name,
            'campaigns'      : campaigns,
            'total_spend'    : round(sum(c['spend'] for c in campaigns), 2),
        })

    return jsonify({'ok': True, 'data': result, 'start_date': start_date, 'end_date': end_date})

# ─── Desativar campanhas ───────────────────────────────────────────────────────
@app.route('/api/bcs-api/<bc_id>/disable-campaign', methods=['POST'])
def api_disable_campaign(bc_id):
    bcs   = get_bcs_api()
    bc    = next((b for b in bcs if b['id'] == bc_id), None)
    token = get_token_for_bc(bc.get('tiktokBcId','')) if bc else None
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    adv_id      = request.json.get('advertiser_id')
    campaign_id = request.json.get('campaign_id')
    r = tt_post('campaign/status/update', token, {
        'advertiser_id': adv_id,
        'campaign_ids' : [campaign_id],
        'operation_status': 'DISABLE'
    })
    if r.get('code') == 0:
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': r.get('message', str(r))})

@app.route('/api/bcs-api/<bc_id>/disable-all', methods=['POST'])
def api_disable_all(bc_id):
    """Desativa TODAS as campanhas de todas as contas do BC em background."""
    bcs   = get_bcs_api()
    bc    = next((b for b in bcs if b['id'] == bc_id), None)
    token = get_token_for_bc(bc.get('tiktokBcId','')) if bc else None
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    adv_ids = bc.get('advertiser_ids', [])

    def run():
        total = 0
        for adv_id in adv_ids:
            rc = tt_get('campaign/get', token, adv_id, {'page_size': 100})
            if rc.get('code') != 0: continue
            camps = [str(c['campaign_id']) for c in rc.get('data',{}).get('list',[])
                     if c.get('operation_status') != 'DELETE']
            if not camps: continue
            for i in range(0, len(camps), 20):
                lote = camps[i:i+20]
                r = tt_post('campaign/status/update', token, {
                    'advertiser_id': adv_id,
                    'campaign_ids' : lote,
                    'operation_status': 'DISABLE'
                })
                if r.get('code') == 0: total += len(lote)
                time.sleep(0.3)
            adicionar_log(f'[TikAPI] {adv_id}: {len(camps)} campanhas desativadas', 'success')
        adicionar_log(f'[TikAPI] Total desativadas: {total}', 'success')

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True, 'message': f'Desativando campanhas de {len(adv_ids)} contas em background...'})

if __name__ == '__main__':
    import webbrowser
    print('=' * 50)
    print('  TikAuto — v5-full')
    print('  Acessar: http://localhost:5000')
    print('=' * 50)
    webbrowser.open('http://localhost:5000')
    app.run(debug=False, port=5000, threaded=True)
