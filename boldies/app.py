"""
TikAuto Cloud — boldies.site
Flask backend completo para gestão de campanhas TikTok via API oficial
"""

from flask import Flask, render_template, jsonify, request, redirect, session
import requests
import json
import os
import time
import threading
import uuid
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'tikauto-secret-2026')

# ── Configuração da API TikTok ─────────────────────────────────────────
TIKTOK_APP_ID     = os.environ.get('TIKTOK_APP_ID', '7610282489889685520')
TIKTOK_APP_SECRET = os.environ.get('TIKTOK_APP_SECRET', '')  # definir no servidor
TIKTOK_REDIRECT   = os.environ.get('TIKTOK_REDIRECT', 'https://boldies.site/oauth/callback')
TIKTOK_API_BASE   = 'https://business-api.tiktok.com/open_api/v1.3'

# ── Storage em JSON ────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

def load_data(filename, default):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return default

def save_data(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_bcs():      return load_data('bcs.json', [])
def save_bcs(d):    save_data('bcs.json', d)
def get_tokens():   return load_data('tokens.json', {})
def save_tokens(d): save_data('tokens.json', d)
def get_jobs():     return load_data('jobs.json', [])
def save_jobs(d):   save_data('jobs.json', d)

# ── Job runner em thread ───────────────────────────────────────────────
running_jobs = {}   # job_id -> {'thread', 'stop': False, 'logs': []}

def add_log(job_id, msg, level='info'):
    ts = datetime.now().strftime('%H:%M:%S')
    entry = {'ts': ts, 'msg': msg, 'level': level}
    if job_id in running_jobs:
        running_jobs[job_id]['logs'].append(entry)
        # Mantém só os últimos 500 logs em memória
        running_jobs[job_id]['logs'] = running_jobs[job_id]['logs'][-500:]
    # Salva também em arquivo
    log_file = os.path.join(LOGS_DIR, f'{job_id}.jsonl')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')

# ── Helpers API TikTok ─────────────────────────────────────────────────
def tiktok_get(endpoint, token, advertiser_id, params=None):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token}
    p = {'advertiser_id': advertiser_id}
    if params: p.update(params)
    r = requests.get(url, headers=headers, params=p, timeout=15)
    return r.json()

def tiktok_post(endpoint, token, payload):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token, 'Content-Type': 'application/json'}
    r = requests.post(url, headers=headers, json=payload, timeout=15)
    return r.json()

def get_token_for_bc(bc_id):
    tokens = get_tokens()
    return tokens.get(str(bc_id), {}).get('access_token')

def get_advertiser_ids(token):
    """Busca todos os advertiser_ids vinculados ao token."""
    url = f"{TIKTOK_API_BASE}/oauth2/advertiser/get/"
    headers = {'Access-Token': token}
    params  = {'app_id': TIKTOK_APP_ID, 'secret': TIKTOK_APP_SECRET}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        d = r.json()
        if d.get('code') == 0:
            return [str(a['advertiser_id']) for a in d['data'].get('list', [])]
    except Exception as e:
        print(f"Erro get_advertiser_ids: {e}")
    return []

# ══════════════════════════════════════════════════════════════════════
# ROTAS PRINCIPAIS
# ══════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

# ── OAuth ──────────────────────────────────────────────────────────────
@app.route('/oauth/url')
def oauth_url():
    """Gera a URL de autorização para um BC específico."""
    bc_id = request.args.get('bc_id', 'unknown')
    state = f"bcid_{bc_id}"
    url = (
        f"https://business-api.tiktok.com/portal/auth"
        f"?app_id={TIKTOK_APP_ID}"
        f"&state={state}"
        f"&redirect_uri={TIKTOK_REDIRECT}"
    )
    return jsonify({'ok': True, 'url': url})

@app.route('/oauth/callback')
def oauth_callback():
    """TikTok redireciona aqui após autorização."""
    auth_code = request.args.get('auth_code')
    state     = request.args.get('state', '')
    error     = request.args.get('error_code')

    if error:
        return render_template('index.html', oauth_error=error)

    if not auth_code:
        return render_template('index.html', oauth_error='Sem auth_code')

    # Troca auth_code por access_token
    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return render_template('index.html', oauth_error=result.get('error', 'Erro desconhecido'))

    # Extrai bc_id do state
    bc_id = None
    if state.startswith('bcid_'):
        try: bc_id = int(state.replace('bcid_', ''))
        except: pass

    # Salva token
    token_data = result['data']
    tokens = get_tokens()
    key    = str(bc_id) if bc_id else token_data['advertiser_id']
    tokens[key] = {
        'access_token'  : token_data['access_token'],
        'advertiser_id' : token_data.get('advertiser_id', ''),
        'scope'         : token_data.get('scope', ''),
        'token_type'    : token_data.get('token_type', 'bearer'),
        'saved_at'      : datetime.now().isoformat(),
        'bc_id'         : bc_id
    }
    save_tokens(tokens)

    # Busca advertiser IDs automaticamente
    advs = get_advertiser_ids(token_data['access_token'])

    # Atualiza BC com advertiser IDs se soubermos qual é
    if bc_id:
        bcs = get_bcs()
        for bc in bcs:
            if bc['id'] == bc_id:
                bc['advertiser_ids'] = advs
                bc['token_ok'] = True
                break
        save_bcs(bcs)

    return render_template('index.html',
                           oauth_success=True,
                           advertiser_count=len(advs))

@app.route('/api/oauth/exchange', methods=['POST'])
def oauth_exchange():
    """Troca auth_code por token (chamada manual)."""
    auth_code = request.json.get('auth_code')
    bc_id     = request.json.get('bc_id')
    if not auth_code:
        return jsonify({'ok': False, 'error': 'auth_code obrigatório'})

    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return jsonify(result)

    token_data = result['data']
    tokens = get_tokens()
    key    = str(bc_id) if bc_id else token_data.get('advertiser_id', str(time.time()))
    tokens[key] = {
        'access_token' : token_data['access_token'],
        'advertiser_id': token_data.get('advertiser_id', ''),
        'saved_at'     : datetime.now().isoformat(),
        'bc_id'        : bc_id
    }
    save_tokens(tokens)

    advs = get_advertiser_ids(token_data['access_token'])
    if bc_id:
        bcs = get_bcs()
        for bc in bcs:
            if str(bc['id']) == str(bc_id):
                bc['advertiser_ids'] = advs
                bc['token_ok'] = True
                break
        save_bcs(bcs)

    return jsonify({'ok': True, 'advertiser_count': len(advs), 'advertiser_ids': advs})

def _exchange_token(auth_code):
    try:
        url = f"{TIKTOK_API_BASE}/oauth2/access_token/"
        payload = {
            'app_id'    : TIKTOK_APP_ID,
            'secret'    : TIKTOK_APP_SECRET,
            'auth_code' : auth_code,
            'grant_type': 'authorization_code'
        }
        r = requests.post(url, json=payload, timeout=15)
        d = r.json()
        if d.get('code') == 0:
            return {'ok': True, 'data': d['data']}
        return {'ok': False, 'error': d.get('message', 'Erro API TikTok')}
    except Exception as e:
        return {'ok': False, 'error': str(e)}

# ── BCs ────────────────────────────────────────────────────────────────
@app.route('/api/bcs', methods=['GET'])
def api_get_bcs():
    bcs    = get_bcs()
    tokens = get_tokens()
    for bc in bcs:
        bc['token_ok'] = str(bc['id']) in tokens
    return jsonify(bcs)

@app.route('/api/bcs', methods=['POST'])
def api_add_bc():
    d = request.json
    bcs = get_bcs()
    bc  = {
        'id'            : int(time.time() * 1000),
        'nome'          : d['nome'],
        'advertiser_ids': d.get('advertiser_ids', []),
        'token_ok'      : False,
        'criado_em'     : datetime.now().isoformat()
    }
    bcs.append(bc)
    save_bcs(bcs)
    return jsonify({'ok': True, 'bc': bc})

@app.route('/api/bcs/<int:bc_id>', methods=['DELETE'])
def api_del_bc(bc_id):
    bcs = [b for b in get_bcs() if b['id'] != bc_id]
    save_bcs(bcs)
    tokens = get_tokens()
    tokens.pop(str(bc_id), None)
    save_tokens(tokens)
    return jsonify({'ok': True})

@app.route('/api/bcs/<int:bc_id>/advertisers', methods=['GET'])
def api_get_advertisers(bc_id):
    """Busca lista atualizada de advertiser IDs do BC."""
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token. Conecte via OAuth primeiro.'})
    advs = get_advertiser_ids(token)
    # Salva no BC
    bcs = get_bcs()
    for bc in bcs:
        if bc['id'] == bc_id:
            bc['advertiser_ids'] = advs
            break
    save_bcs(bcs)
    return jsonify({'ok': True, 'advertiser_ids': advs, 'total': len(advs)})

# ── Pixel / Data Connection ────────────────────────────────────────────
@app.route('/api/pixels/<int:bc_id>', methods=['GET'])
def api_get_pixels(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    bcs  = get_bcs()
    bc   = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc or not bc.get('advertiser_ids'):
        return jsonify({'ok': False, 'error': 'Sem advertiser IDs'})
    adv_id = bc['advertiser_ids'][0]
    d = tiktok_get('pixel/list', token, adv_id)
    if d.get('code') == 0:
        pixels = [{'id': p['pixel_id'], 'name': p['pixel_name'], 'status': p.get('status')}
                  for p in d['data'].get('pixels', [])]
        return jsonify({'ok': True, 'pixels': pixels})
    return jsonify({'ok': False, 'error': d.get('message', 'Erro')})

# ══════════════════════════════════════════════════════════════════════
# CRIAÇÃO DE CONTAS EM MASSA
# ══════════════════════════════════════════════════════════════════════

@app.route('/api/criar-contas', methods=['POST'])
def criar_contas():
    d       = request.json
    bc_id   = d['bc_id']
    quantidade = d.get('quantidade', 10)
    prefixo    = d.get('prefixo', 'TK')
    moeda      = d.get('moeda', 'EUR')
    timezone   = d.get('timezone', 'Etc/GMT+3')

    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token OAuth'})

    job_id = str(uuid.uuid4())[:8]
    running_jobs[job_id] = {'logs': [], 'stop': False, 'status': 'running',
                             'total': quantidade, 'done': 0, 'sucesso': 0, 'falha': 0}

    # Salva job
    jobs = get_jobs()
    jobs.append({'id': job_id, 'tipo': 'criar_contas', 'bc_id': bc_id,
                 'criado_em': datetime.now().isoformat(), 'status': 'running'})
    save_jobs(jobs)

    def run():
        add_log(job_id, f'Iniciando criação de {quantidade} contas...', 'info')
        bcs = get_bcs()
        bc  = next((b for b in bcs if b['id'] == bc_id), None)
        if not bc:
            add_log(job_id, 'BC não encontrado!', 'error')
            return

        novos_ids = []
        for i in range(quantidade):
            if running_jobs[job_id]['stop']:
                add_log(job_id, 'Parado pelo usuário.', 'warn')
                break

            nome = f"{prefixo}_{int(time.time() * 1000) % 1000000:06d}"
            try:
                payload = {
                    'advertiser_name'    : nome,
                    'currency'           : moeda,
                    'timezone'           : timezone,
                    'app_id'             : TIKTOK_APP_ID,
                    'secret'             : TIKTOK_APP_SECRET,
                    'registration_detail': {
                        'company_name'   : nome,
                        'address'        : 'Online',
                        'industry'       : 'ECOMMERCE'
                    }
                }
                r = tiktok_post('advertiser/create', token, payload)
                if r.get('code') == 0:
                    adv_id = r['data']['advertiser_id']
                    novos_ids.append(str(adv_id))
                    running_jobs[job_id]['sucesso'] += 1
                    add_log(job_id, f'[{i+1}/{quantidade}] ✓ Conta criada: {nome} ({adv_id})', 'success')
                else:
                    running_jobs[job_id]['falha'] += 1
                    add_log(job_id, f'[{i+1}/{quantidade}] ✗ Erro: {r.get("message", "?")}', 'error')
            except Exception as e:
                running_jobs[job_id]['falha'] += 1
                add_log(job_id, f'[{i+1}/{quantidade}] ✗ Exceção: {str(e)[:80]}', 'error')

            running_jobs[job_id]['done'] = i + 1
            time.sleep(0.5)  # rate limit

        # Salva advertiser IDs novos no BC
        if novos_ids:
            bcs = get_bcs()
            for bc in bcs:
                if bc['id'] == bc_id:
                    existing = bc.get('advertiser_ids', [])
                    bc['advertiser_ids'] = list(set(existing + novos_ids))
                    break
            save_bcs(bcs)

        running_jobs[job_id]['status'] = 'done'
        total_s = running_jobs[job_id]['sucesso']
        total_f = running_jobs[job_id]['falha']
        add_log(job_id, f'Concluído: {total_s} criadas, {total_f} falhas', 'success')

    t = threading.Thread(target=run, daemon=True)
    running_jobs[job_id]['thread'] = t
    t.start()
    return jsonify({'ok': True, 'job_id': job_id})

# ══════════════════════════════════════════════════════════════════════
# CRIAÇÃO DE CAMPANHAS EM MASSA
# ══════════════════════════════════════════════════════════════════════

@app.route('/api/criar-campanhas', methods=['POST'])
def criar_campanhas():
    d = request.json

    bc_id          = d['bc_id']
    objetivo       = d.get('objetivo', 'VIDEO_VIEWS')   # VIDEO_VIEWS | CONVERSIONS | TRAFFIC | etc
    campaign_name  = d.get('campaign_name', 'Campanha Auto')
    budget         = float(d.get('budget', 20))
    budget_mode    = d.get('budget_mode', 'BUDGET_MODE_DAY')  # daily ou total
    # Ad Group
    adgroup_name   = d.get('adgroup_name', 'AdGroup Auto')
    num_adgroups   = int(d.get('num_adgroups', 1))
    pixel_id       = d.get('pixel_id', '')
    optimization   = d.get('optimization_event', 'PURCHASE')
    # Segmentação
    paises         = d.get('countries', ['BR'])
    age_min        = d.get('age_min', 18)
    age_max        = d.get('age_max', 44)
    gender         = d.get('gender', 'GENDER_UNLIMITED')
    languages      = d.get('languages', ['pt'])
    # Criativo
    post_code      = d.get('post_code', '')
    product_url    = d.get('product_url', '')
    cta            = d.get('cta', 'LEARN_MORE')
    identity_id    = d.get('identity_id', '')   # TikTok account identity
    # Seleção de contas
    advertiser_ids = d.get('advertiser_ids', [])   # [] = todas do BC

    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token OAuth'})

    # Se não especificou advertiser_ids, usa todas do BC
    if not advertiser_ids:
        bcs = get_bcs()
        bc  = next((b for b in bcs if b['id'] == bc_id), None)
        if bc:
            advertiser_ids = bc.get('advertiser_ids', [])

    if not advertiser_ids:
        return jsonify({'ok': False, 'error': 'Nenhum advertiser ID disponível. Sincronize as contas do BC.'})

    job_id = str(uuid.uuid4())[:8]
    total  = len(advertiser_ids)
    running_jobs[job_id] = {
        'logs': [], 'stop': False, 'status': 'running',
        'total': total, 'done': 0, 'sucesso': 0, 'falha': 0
    }

    jobs = get_jobs()
    jobs.append({'id': job_id, 'tipo': 'criar_campanhas', 'bc_id': bc_id,
                 'criado_em': datetime.now().isoformat(), 'status': 'running'})
    save_jobs(jobs)

    # Mapeamento objetivo → configuração de campanha
    OBJETIVO_MAP = {
        'VIDEO_VIEWS'  : {'objective_type': 'VIDEO_VIEWS',   'billing_event': 'oCPM'},
        'CONVERSIONS'  : {'objective_type': 'CONVERSIONS',   'billing_event': 'oCPM'},
        'TRAFFIC'      : {'objective_type': 'TRAFFIC',       'billing_event': 'CPC'},
        'APP_PROMOTION': {'objective_type': 'APP_PROMOTION', 'billing_event': 'oCPM'},
        'LEAD_GENERATION': {'objective_type': 'LEAD_GENERATION', 'billing_event': 'oCPM'},
        'REACH'        : {'objective_type': 'REACH',         'billing_event': 'CPM'},
        'ENGAGEMENT'   : {'objective_type': 'ENGAGEMENT',    'billing_event': 'oCPM'},
    }
    obj_cfg = OBJETIVO_MAP.get(objetivo, OBJETIVO_MAP['VIDEO_VIEWS'])

    # Mapeamento de faixas de idade
    def build_age_groups(mn, mx):
        faixas_map = {
            (13,17): 'AGE_13_17', (18,24): 'AGE_18_24',
            (25,34): 'AGE_25_34', (35,44): 'AGE_35_44',
            (45,54): 'AGE_45_54', (55,100): 'AGE_55_100'
        }
        result = []
        for (fmin, fmax), val in faixas_map.items():
            if fmin >= mn and fmax <= mx:
                result.append(val)
        return result or ['AGE_18_24', 'AGE_25_34', 'AGE_35_44']

    age_groups = build_age_groups(age_min, age_max)

    def run():
        add_log(job_id, f'Iniciando campanhas em {total} contas — objetivo: {objetivo}', 'info')

        for idx, adv_id in enumerate(advertiser_ids):
            if running_jobs[job_id]['stop']:
                add_log(job_id, 'Parado pelo usuário.', 'warn')
                break

            add_log(job_id, f'[{idx+1}/{total}] Conta {adv_id}...', 'info')

            try:
                # ── 1. Criar Campanha ────────────────────────────────
                ts = datetime.now().strftime('%Y%m%d%H%M%S')
                camp_payload = {
                    'advertiser_id'   : adv_id,
                    'campaign_name'   : f"{campaign_name}_{ts}",
                    'objective_type'  : obj_cfg['objective_type'],
                    'budget_mode'     : budget_mode,
                    'budget'          : budget,
                    'campaign_type'   : 'REGULAR_CAMPAIGN',
                    'special_industries': [],
                }
                # Para conversão: precisa de pixel
                if objetivo == 'CONVERSIONS' and pixel_id:
                    camp_payload['conversion_bid_type'] = 'BID_TYPE_NO_BID'

                r_camp = tiktok_post('campaign/create', token, camp_payload)
                if r_camp.get('code') != 0:
                    raise Exception(f"Campanha: {r_camp.get('message', '?')}")

                camp_id = r_camp['data']['campaign_id']
                add_log(job_id, f'  ✓ Campanha criada: {camp_id}', 'success')

                # ── 2. Criar Ad Group(s) ─────────────────────────────
                adgroup_ids = []
                for ag_i in range(num_adgroups):
                    ag_name = adgroup_name if num_adgroups == 1 else f"{adgroup_name}_{ag_i+1}"
                    ag_payload = {
                        'advertiser_id' : adv_id,
                        'campaign_id'   : camp_id,
                        'adgroup_name'  : ag_name,
                        'placement_type': 'PLACEMENT_TYPE_NORMAL',
                        'placements'    : ['PLACEMENT_TIKTOK'],
                        'location_ids'  : _get_location_ids(paises),
                        'age_groups'    : age_groups,
                        'gender'        : gender,
                        'languages'     : languages,
                        'budget_mode'   : budget_mode,
                        'budget'        : budget,
                        'schedule_type' : 'SCHEDULE_START_END' if False else 'SCHEDULE_FROM_NOW',
                        'optimization_goal': _get_opt_goal(objetivo),
                        'billing_event' : obj_cfg['billing_event'],
                        'bid_type'      : 'BID_TYPE_NO_BID',
                        'pacing'        : 'PACING_MODE_SMOOTH',
                        'operation_status': 'ENABLE',
                    }
                    # Pixel para conversão
                    if objetivo == 'CONVERSIONS' and pixel_id:
                        ag_payload['pixel_id']           = pixel_id
                        ag_payload['optimization_event'] = optimization

                    r_ag = tiktok_post('adgroup/create', token, ag_payload)
                    if r_ag.get('code') != 0:
                        add_log(job_id, f'  ✗ Ad Group {ag_i+1}: {r_ag.get("message","?")}', 'error')
                        continue

                    ag_id = r_ag['data']['adgroup_id']
                    adgroup_ids.append(ag_id)
                    add_log(job_id, f'  ✓ Ad Group {ag_i+1}: {ag_id}', 'success')
                    time.sleep(0.3)

                # ── 3. Criar Ad (criativo) ───────────────────────────
                if not adgroup_ids:
                    raise Exception("Nenhum Ad Group criado com sucesso")

                for ag_id in adgroup_ids:
                    ad_payload = {
                        'advertiser_id': adv_id,
                        'adgroup_id'   : ag_id,
                        'ad_name'      : f"Ad_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                        'ad_format'    : 'SINGLE_VIDEO',
                        'call_to_action': cta,
                        'operation_status': 'ENABLE',
                    }
                    # TikTok post (Spark Ad)
                    if post_code and identity_id:
                        ad_payload['tiktok_item_id'] = post_code
                        ad_payload['identity_id']    = identity_id
                        ad_payload['identity_type']  = 'CUSTOMIZED_USER'
                    # URL de destino
                    if product_url:
                        ad_payload['landing_page_url'] = product_url

                    r_ad = tiktok_post('ad/create', token, ad_payload)
                    if r_ad.get('code') != 0:
                        add_log(job_id, f'  ✗ Ad: {r_ad.get("message","?")}', 'error')
                    else:
                        add_log(job_id, f'  ✓ Ad criado: {r_ad["data"]["ad_id"]}', 'success')
                    time.sleep(0.3)

                running_jobs[job_id]['sucesso'] += 1
                add_log(job_id, f'[{idx+1}/{total}] ✓ Conta {adv_id} concluída!', 'success')

            except Exception as e:
                running_jobs[job_id]['falha'] += 1
                add_log(job_id, f'[{idx+1}/{total}] ✗ Erro conta {adv_id}: {str(e)[:120]}', 'error')

            running_jobs[job_id]['done'] = idx + 1
            time.sleep(0.5)

        running_jobs[job_id]['status'] = 'done'
        s = running_jobs[job_id]['sucesso']
        f = running_jobs[job_id]['falha']
        add_log(job_id, f'✓ Concluído: {s} sucessos, {f} falhas', 'success')

        # Atualiza status do job no disco
        jobs = get_jobs()
        for j in jobs:
            if j['id'] == job_id:
                j['status'] = 'done'
                j['sucesso'] = s
                j['falha']   = f
                break
        save_jobs(jobs)

    t = threading.Thread(target=run, daemon=True)
    running_jobs[job_id]['thread'] = t
    t.start()
    return jsonify({'ok': True, 'job_id': job_id, 'total_contas': total})

def _get_location_ids(paises):
    """Mapeia códigos de país ISO para IDs da API TikTok."""
    # IDs reais da API TikTok para os países mais comuns
    MAP = {
        'BR': '6252001', 'PT': '2264397', 'US': '6252001',
        'MX': '3996063', 'AR': '3865483', 'CO': '3686110',
        'FR': '3017382', 'DE': '2921044', 'ES': '2510769',
        'IT': '3175395', 'GB': '2635167', 'AE': '290557',
    }
    ids = [MAP[p] for p in paises if p in MAP]
    return ids or ['6252001']  # Brasil como fallback

def _get_opt_goal(objetivo):
    MAP = {
        'VIDEO_VIEWS'    : 'VIDEO_VIEWS',
        'CONVERSIONS'    : 'CONVERT',
        'TRAFFIC'        : 'CLICK',
        'APP_PROMOTION'  : 'INSTALL',
        'LEAD_GENERATION': 'LEAD_GENERATION',
        'REACH'          : 'REACH',
        'ENGAGEMENT'     : 'ENGAGEMENT',
    }
    return MAP.get(objetivo, 'VIDEO_VIEWS')

# ── Job control ────────────────────────────────────────────────────────
@app.route('/api/job/<job_id>/logs')
def job_logs(job_id):
    offset = int(request.args.get('offset', 0))
    if job_id in running_jobs:
        logs   = running_jobs[job_id]['logs']
        status = running_jobs[job_id]['status']
        done   = running_jobs[job_id]['done']
        total  = running_jobs[job_id]['total']
        sucesso= running_jobs[job_id]['sucesso']
        falha  = running_jobs[job_id]['falha']
    else:
        # Lê do arquivo
        log_file = os.path.join(LOGS_DIR, f'{job_id}.jsonl')
        logs = []
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: logs.append(json.loads(line))
                    except: pass
        status = 'done'; done = 0; total = 0; sucesso = 0; falha = 0
    return jsonify({
        'logs'   : logs[offset:],
        'total_logs': len(logs),
        'status' : status,
        'done'   : done,
        'total'  : total,
        'sucesso': sucesso,
        'falha'  : falha,
    })

@app.route('/api/job/<job_id>/stop', methods=['POST'])
def job_stop(job_id):
    if job_id in running_jobs:
        running_jobs[job_id]['stop'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Job não encontrado'})

@app.route('/api/jobs')
def list_jobs():
    return jsonify(get_jobs()[-50:])  # últimos 50

# ── Dashboard stats ────────────────────────────────────────────────────
@app.route('/api/stats')
def api_stats():
    bcs    = get_bcs()
    tokens = get_tokens()
    jobs   = get_jobs()
    contas_total = sum(len(b.get('advertiser_ids', [])) for b in bcs)
    camp_jobs    = [j for j in jobs if j.get('tipo') == 'criar_campanhas']
    camp_suc     = sum(j.get('sucesso', 0) for j in camp_jobs)
    camp_fal     = sum(j.get('falha', 0) for j in camp_jobs)
    rodando      = len([j for j in running_jobs.values() if j.get('status') == 'running'])
    return jsonify({
        'total_bcs'     : len(bcs),
        'bcs_conectados': sum(1 for b in bcs if str(b['id']) in tokens),
        'total_contas'  : contas_total,
        'camp_sucesso'  : camp_suc,
        'camp_falha'    : camp_fal,
        'rodando'       : rodando,
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
