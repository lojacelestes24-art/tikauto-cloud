"""
TikAuto Cloud — boldies.site
Flask backend completo para gestão de campanhas TikTok via API oficial
v4-saas-pro
"""

from flask import Flask, render_template, jsonify, request
import requests
import json
import os
import re
import time
import threading
import uuid
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'tikauto-secret-2026')

TIKTOK_APP_ID     = os.environ.get('TIKTOK_APP_ID', '7610282489889685520')
TIKTOK_APP_SECRET = os.environ.get('TIKTOK_APP_SECRET', '')
TIKTOK_REDIRECT   = os.environ.get('TIKTOK_REDIRECT', 'https://boldies.site/oauth/callback')
TIKTOK_API_BASE   = 'https://business-api.tiktok.com/open_api/v1.3'

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# ── Storage ────────────────────────────────────────────────────────────
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
def get_sparks():   return load_data('sparks.json', [])
def save_sparks(d): save_data('sparks.json', d)

# ── Job runner ─────────────────────────────────────────────────────────
running_jobs = {}

def add_log(job_id, msg, level='info'):
    ts = datetime.now().strftime('%H:%M:%S')
    entry = {'ts': ts, 'msg': msg, 'level': level}
    if job_id in running_jobs:
        running_jobs[job_id]['logs'].append(entry)
        running_jobs[job_id]['logs'] = running_jobs[job_id]['logs'][-500:]
    log_file = os.path.join(LOGS_DIR, f'{job_id}.jsonl')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')

# ── Extract item_id ────────────────────────────────────────────────────
def extract_item_id_from_url(text):
    if not text:
        return None
    text = text.strip()
    if re.match(r'^\d{10,25}$', text):
        return text
    m = re.search(r'/video/(\d{10,25})', text)
    if m:
        return m.group(1)
    if 'vm.tiktok.com' in text or 'vt.tiktok.com' in text:
        try:
            r = requests.head(text, allow_redirects=True, timeout=10)
            m = re.search(r'/video/(\d{10,25})', r.url)
            if m:
                return m.group(1)
        except:
            pass
    return None

# ── TikTok API helpers ─────────────────────────────────────────────────
def tiktok_get(endpoint, token, advertiser_id, params=None):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token}
    p = {'advertiser_id': advertiser_id}
    if params: p.update(params)
    r = requests.get(url, headers=headers, params=p, timeout=15)
    try: return r.json()
    except: return {'code': -1, 'message': f'Resposta invalida: {r.text[:200]}'}

def tiktok_post(endpoint, token, payload):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token, 'Content-Type': 'application/json'}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    try: return r.json()
    except: return {'code': -1, 'message': f'Resposta invalida: {r.text[:200]}'}

def get_token_for_bc(bc_id):
    return get_tokens().get(str(bc_id), {}).get('access_token')

def get_advertiser_ids(token):
    url = f"{TIKTOK_API_BASE}/oauth2/advertiser/get/"
    try:
        r = requests.get(url, headers={'Access-Token': token},
                         params={'app_id': TIKTOK_APP_ID, 'secret': TIKTOK_APP_SECRET}, timeout=15)
        d = r.json()
        if d.get('code') == 0:
            return [str(a['advertiser_id']) for a in d['data'].get('list', [])]
    except Exception as e:
        print(f"Erro get_advertiser_ids: {e}")
    return []

def get_advertiser_info(token, advertiser_id):
    r = tiktok_get('advertiser/info', token, advertiser_id,
                   {'fields': '["name","status","currency","timezone"]'})
    if r.get('code') == 0:
        info_list = r.get('data', {}).get('list', [])
        if info_list:
            return info_list[0]
    return {}

def get_active_campaigns(token, advertiser_id):
    try:
        # Filtra diretamente por status ativo usando o filtering da API
        import json as _json
        filtering = _json.dumps({"status": "CAMPAIGN_STATUS_ENABLE"})
        r = tiktok_get('campaign/get', token, advertiser_id, {
            'page_size': 100,
            'filtering': filtering,
        })
        if r.get('code') == 0:
            return r.get('data', {}).get('list', [])
        print(f"get_active_campaigns erro adv {advertiser_id}: code={r.get('code')} msg={r.get('message')}")
        return []
    except Exception as e:
        print(f"get_active_campaigns exception adv {advertiser_id}: {e}")
        return []

# ── Objetivo config ────────────────────────────────────────────────────
OBJETIVO_CONFIG = {
    'VIDEO_VIEWS':     {'objective_type':'VIDEO_VIEWS',     'optimization_goal':'ENGAGED_VIEW', 'billing_event':'CPV'},
    'REACH':           {'objective_type':'REACH',           'optimization_goal':'REACH',        'billing_event':'CPM'},
    'TRAFFIC':         {'objective_type':'TRAFFIC',         'optimization_goal':'CLICK',        'billing_event':'CPC'},
    'ENGAGEMENT':      {'objective_type':'ENGAGEMENT',      'optimization_goal':'SHOW',         'billing_event':'CPM'},
    'LEAD_GENERATION': {'objective_type':'LEAD_GENERATION', 'optimization_goal':'LEAD_GENERATION','billing_event':'OCPM'},
    'APP_PROMOTION':   {'objective_type':'APP_PROMOTION',   'optimization_goal':'INSTALL',      'billing_event':'OCPM'},
    'CONVERSIONS':     {'objective_type':'WEB_CONVERSIONS', 'optimization_goal':'CONVERT',      'billing_event':'OCPM'},
    'PRODUCT_SALES':   {'objective_type':'PRODUCT_SALES',   'optimization_goal':'CONVERT',      'billing_event':'OCPM'},
}

LOCATION_MAP = {
    'BR':3469034,'PT':2264397,'US':6252001,'MX':3996063,'AR':3865483,
    'CO':3686110,'FR':3017382,'DE':2921044,'ES':2510769,'IT':3175395,
    'GB':2635167,'AE':290557,'CL':3895114,'PE':3932488,'EC':3658394,
}

def get_location_ids(paises):
    ids = [str(LOCATION_MAP[p]) for p in paises if p in LOCATION_MAP]
    return ids if ids else ['3469034']

# ══════════════════════════════════════════════════════════════════════
# ROTAS
# ══════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/version')
def version():
    return jsonify({'version': 'v4-saas-pro'})

# ── OAuth ──────────────────────────────────────────────────────────────
@app.route('/oauth/url')
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
    if error:
        return render_template('index.html', oauth_error=error)
    if not auth_code:
        return render_template('index.html', oauth_error='Sem auth_code')
    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return render_template('index.html', oauth_error=result.get('error'))
    bc_id = None
    if state.startswith('bcid_'):
        try: bc_id = int(state.replace('bcid_', ''))
        except: pass
    token_data = result['data']
    tokens = get_tokens()
    key = str(bc_id) if bc_id else token_data.get('advertiser_id', str(time.time()))
    tokens[key] = {'access_token': token_data['access_token'],
                   'advertiser_id': token_data.get('advertiser_id', ''),
                   'saved_at': datetime.now().isoformat(), 'bc_id': bc_id}
    save_tokens(tokens)
    advs = get_advertiser_ids(token_data['access_token'])
    if bc_id:
        bcs = get_bcs()
        for bc in bcs:
            if bc['id'] == bc_id:
                bc['advertiser_ids'] = advs
                bc['token_ok'] = True
                break
        save_bcs(bcs)
    return render_template('index.html', oauth_success=True, advertiser_count=len(advs))

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
    key = str(bc_id) if bc_id else token_data.get('advertiser_id', str(time.time()))
    tokens[key] = {'access_token': token_data['access_token'],
                   'advertiser_id': token_data.get('advertiser_id', ''),
                   'saved_at': datetime.now().isoformat(), 'bc_id': bc_id}
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
        r = requests.post(f"{TIKTOK_API_BASE}/oauth2/access_token/", json={
            'app_id': TIKTOK_APP_ID, 'secret': TIKTOK_APP_SECRET,
            'auth_code': auth_code, 'grant_type': 'authorization_code'
        }, timeout=15)
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
    bc = {
        'id': int(time.time() * 1000),
        'nome': d['nome'],
        'advertiser_ids': d.get('advertiser_ids', []),
        'accounts': {},
        'token_ok': False,
        'criado_em': datetime.now().isoformat()
    }
    bcs.append(bc)
    save_bcs(bcs)
    return jsonify({'ok': True, 'bc': bc})

@app.route('/api/bcs/<int:bc_id>', methods=['DELETE'])
def api_del_bc(bc_id):
    save_bcs([b for b in get_bcs() if b['id'] != bc_id])
    tokens = get_tokens()
    tokens.pop(str(bc_id), None)
    save_tokens(tokens)
    return jsonify({'ok': True})

@app.route('/api/bcs/<int:bc_id>/advertisers', methods=['GET'])
def api_get_advertisers(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token. Conecte via OAuth primeiro.'})
    advs = get_advertiser_ids(token)
    bcs  = get_bcs()
    for bc in bcs:
        if bc['id'] == bc_id:
            bc['advertiser_ids'] = advs
            if 'accounts' not in bc:
                bc['accounts'] = {}
            for adv_id in advs:
                info = get_advertiser_info(token, adv_id)
                existing = bc['accounts'].get(adv_id, {})
                bc['accounts'][adv_id] = {
                    'name': info.get('name', existing.get('name', adv_id)),
                    'status': info.get('status', ''),
                    'currency': info.get('currency', ''),
                    'campaigns_active': existing.get('campaigns_active', 0)
                }
            break
    save_bcs(bcs)
    return jsonify({'ok': True, 'advertiser_ids': advs, 'total': len(advs)})

# ── NOVO: campaigns-overview assíncrono ────────────────────────────────
# Inicia um job em background e retorna job_id imediatamente
@app.route('/api/bcs/<int:bc_id>/campaigns-overview', methods=['POST'])
def api_campaigns_overview_start(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token. Conecte o BC via OAuth.'})
    bcs = get_bcs()
    bc  = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc:
        return jsonify({'ok': False, 'error': 'BC nao encontrado'})

    adv_ids = bc.get('advertiser_ids', [])
    if not adv_ids:
        return jsonify({'ok': False, 'error': 'BC sem advertiser IDs. Vá em Business Centers e sincronize as contas.'})

    job_id = f"ov_{str(uuid.uuid4())[:8]}"
    running_jobs[job_id] = {
        'logs': [], 'stop': False, 'status': 'running',
        'total': len(adv_ids), 'done': 0, 'sucesso': 0, 'falha': 0,
        'campaigns': []  # acumula resultados
    }

    def run():
        accounts = bc.get('accounts', {})
        result   = []
        add_log(job_id, f'Buscando campanhas em {len(adv_ids)} contas...', 'info')

        for idx, adv_id in enumerate(adv_ids):
            if running_jobs[job_id]['stop']:
                break
            try:
                campaigns = get_active_campaigns(token, adv_id)
                acc_name  = accounts.get(str(adv_id), {}).get('name', str(adv_id))
                if campaigns:
                    for camp in campaigns:
                        result.append({
                            'advertiser_id'  : str(adv_id),
                            'advertiser_name': acc_name,
                            'campaign_id'    : str(camp.get('campaign_id', '')),
                            'campaign_name'  : camp.get('campaign_name', ''),
                            'status'         : camp.get('status', 'CAMPAIGN_STATUS_ENABLE'),
                            'budget'         : camp.get('budget', 0),
                            'objective'      : camp.get('objective_type', ''),
                        })
                    add_log(job_id, f'[{idx+1}/{len(adv_ids)}] {acc_name}: {len(campaigns)} ativas', 'success')
                    running_jobs[job_id]['sucesso'] += 1
                else:
                    add_log(job_id, f'[{idx+1}/{len(adv_ids)}] {acc_name}: sem campanhas', 'info')

                # atualiza cache no BC
                if str(adv_id) in accounts:
                    accounts[str(adv_id)]['campaigns_active'] = len(campaigns)

            except Exception as e:
                add_log(job_id, f'[{idx+1}/{len(adv_ids)}] Erro conta {adv_id}: {str(e)[:80]}', 'error')
                running_jobs[job_id]['falha'] += 1

            running_jobs[job_id]['done'] = idx + 1
            running_jobs[job_id]['campaigns'] = result[:]  # snapshot parcial disponível para o frontend

        # salva cache atualizado
        try:
            fresh_bcs = get_bcs()
            for b in fresh_bcs:
                if b['id'] == bc_id:
                    b['accounts'] = accounts
                    break
            save_bcs(fresh_bcs)
        except: pass

        running_jobs[job_id]['status'] = 'done'
        running_jobs[job_id]['campaigns'] = result
        total_camp = len(result)
        add_log(job_id, f'Concluído — {total_camp} campanhas ativas encontradas', 'success' if total_camp > 0 else 'warn')

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id, 'total_contas': len(adv_ids)})

# Polling: retorna logs + campanhas encontradas até agora
@app.route('/api/overview-job/<job_id>')
def api_overview_job_status(job_id):
    offset = int(request.args.get('offset', 0))
    if job_id not in running_jobs:
        return jsonify({'ok': False, 'error': 'Job não encontrado'})
    j = running_jobs[job_id]
    return jsonify({
        'ok'        : True,
        'status'    : j['status'],
        'done'      : j['done'],
        'total'     : j['total'],
        'sucesso'   : j['sucesso'],
        'falha'     : j['falha'],
        'logs'      : j['logs'][offset:],
        'campaigns' : j['campaigns'],
    })

@app.route('/api/bcs/<int:bc_id>/disable-campaign', methods=['POST'])
def api_disable_campaign(bc_id):
    token       = get_token_for_bc(bc_id)
    adv_id      = request.json.get('advertiser_id')
    campaign_id = request.json.get('campaign_id')
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    r = tiktok_post('campaign/status/update', token, {
        'advertiser_id': adv_id,
        'campaign_ids' : [campaign_id],
        'operation_status': 'DISABLE'
    })
    if r.get('code') == 0:
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': r.get('message', str(r))})

@app.route('/api/bcs/<int:bc_id>/disable-all-campaigns', methods=['POST'])
def api_disable_all_campaigns(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    bcs = get_bcs()
    bc  = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc:
        return jsonify({'ok': False, 'error': 'BC nao encontrado'})

    adv_ids = bc.get('advertiser_ids', [])
    job_id  = str(uuid.uuid4())[:8]
    running_jobs[job_id] = {'logs':[], 'stop':False, 'status':'running',
                             'total':len(adv_ids), 'done':0, 'sucesso':0, 'falha':0}

    def run():
        add_log(job_id, f'Desativando campanhas em {len(adv_ids)} contas...', 'warn')
        total_disabled = 0
        for idx, adv_id in enumerate(adv_ids):
            if running_jobs[job_id]['stop']:
                break
            try:
                campaigns = get_active_campaigns(token, adv_id)
                acc_name  = bc.get('accounts', {}).get(str(adv_id), {}).get('name', adv_id)
                if not campaigns:
                    add_log(job_id, f'  [{idx+1}] {acc_name} — sem campanhas ativas')
                    running_jobs[job_id]['done'] = idx + 1
                    continue
                camp_ids = [str(c['campaign_id']) for c in campaigns]
                r = tiktok_post('campaign/status/update', token, {
                    'advertiser_id': adv_id,
                    'campaign_ids' : camp_ids,
                    'operation_status': 'DISABLE'
                })
                if r.get('code') == 0:
                    add_log(job_id, f'  [{idx+1}] ✓ {acc_name} — {len(camp_ids)} desativadas', 'success')
                    total_disabled += len(camp_ids)
                    running_jobs[job_id]['sucesso'] += 1
                else:
                    add_log(job_id, f'  [{idx+1}] ✗ {acc_name}: {r.get("message","")}', 'error')
                    running_jobs[job_id]['falha'] += 1
            except Exception as e:
                add_log(job_id, f'  [{idx+1}] Erro conta {adv_id}: {str(e)[:100]}', 'error')
                running_jobs[job_id]['falha'] += 1
            running_jobs[job_id]['done'] = idx + 1
            time.sleep(0.3)

        running_jobs[job_id]['status'] = 'done'
        add_log(job_id, f'Concluido — {total_disabled} campanhas desativadas no total',
                'success' if running_jobs[job_id]['falha'] == 0 else 'warn')

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id})

# ── Pixels ─────────────────────────────────────────────────────────────
@app.route('/api/pixels/<int:bc_id>', methods=['GET'])
def api_get_pixels(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'Sem token'})
    bcs = get_bcs()
    bc  = next((b for b in bcs if b['id'] == bc_id), None)
    if not bc or not bc.get('advertiser_ids'):
        return jsonify({'ok': False, 'error': 'Sem advertiser IDs'})
    adv_id = bc['advertiser_ids'][0]
    d = tiktok_get('pixel/list', token, adv_id)
    if d.get('code') == 0:
        pixels = [{'id': p['pixel_id'], 'name': p['pixel_name']} for p in d['data'].get('pixels', [])]
        return jsonify({'ok': True, 'pixels': pixels})
    return jsonify({'ok': False, 'error': d.get('message', 'Erro')})

# ── Spark Posts ────────────────────────────────────────────────────────
@app.route('/api/sparks', methods=['GET'])
def api_get_sparks():
    return jsonify(get_sparks())

@app.route('/api/sparks', methods=['POST'])
def api_add_spark():
    d      = request.json
    sparks = get_sparks()
    value  = d.get('value', '')
    spark  = {
        'id'         : str(uuid.uuid4())[:8],
        'label'      : d.get('label', ''),
        'type'       : d.get('type', 'post'),
        'value'      : value,
        'item_id'    : extract_item_id_from_url(value) or d.get('item_id', ''),
        'bc_id'      : d.get('bc_id', ''),
        'identity_id': d.get('identity_id', ''),
        'criado_em'  : datetime.now().isoformat()
    }
    sparks.append(spark)
    save_sparks(sparks)
    return jsonify({'ok': True, 'spark': spark})

@app.route('/api/sparks/<spark_id>', methods=['DELETE'])
def api_del_spark(spark_id):
    save_sparks([s for s in get_sparks() if s['id'] != spark_id])
    return jsonify({'ok': True})

@app.route('/api/resolve-item-id', methods=['POST'])
def resolve_item_id():
    text    = request.json.get('text', '').strip()
    item_id = extract_item_id_from_url(text)
    if item_id:
        return jsonify({'ok': True, 'item_id': item_id})
    return jsonify({'ok': False, 'error': 'Nao foi possivel extrair o item_id.'})

# ── Campanhas em massa ─────────────────────────────────────────────────
@app.route('/api/criar-campanhas', methods=['POST'])
def criar_campanhas():
    d = request.json
    bc_id         = d['bc_id']
    adv_ids       = d.get('advertiser_ids', [])
    objetivo      = d.get('objetivo', 'VIDEO_VIEWS')
    paises        = d.get('paises', ['BR'])
    idade_min     = d.get('idade_min', 18)
    idade_max     = d.get('idade_max', 55)
    gender        = d.get('gender', 'GENDER_UNLIMITED')
    pixel_id      = d.get('pixel_id', '')
    optimization  = d.get('optimization_event', 'PURCHASE')
    post_code     = d.get('post_code', '')
    post_type     = d.get('post_type', 'SINGLE_VIDEO')
    identity_id   = d.get('identity_id', '')
    product_url   = d.get('product_url', '')
    cta           = d.get('cta', 'LEARN_MORE')
    num_adgroups  = int(d.get('num_adgroups', 1))
    budget        = float(d.get('budget', 50))
    cbo_on        = bool(d.get('budget_optimize_on', False))
    campaign_name = d.get('campaign_name', 'TikAuto')
    adgroup_name  = d.get('adgroup_name', 'AdGroup')

    raw_item_input = d.get('item_id', '').strip()
    item_id_input  = extract_item_id_from_url(raw_item_input) if raw_item_input else ''

    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token OAuth'})

    if not adv_ids:
        bcs = get_bcs()
        bc  = next((b for b in bcs if b['id'] == bc_id), None)
        adv_ids = bc.get('advertiser_ids', []) if bc else []
    if not adv_ids:
        return jsonify({'ok': False, 'error': 'Nenhum advertiser ID disponivel'})

    obj_cfg = OBJETIVO_CONFIG.get(objetivo, OBJETIVO_CONFIG['VIDEO_VIEWS'])
    age_map = {13:'AGE_13_17',18:'AGE_18_24',25:'AGE_25_34',
               35:'AGE_35_44',45:'AGE_45_54',55:'AGE_55_100'}
    age_groups = [lbl for val,lbl in age_map.items() if idade_min<=val<=idade_max] \
                 or ['AGE_18_24','AGE_25_34','AGE_35_44']

    total  = len(adv_ids)
    job_id = str(uuid.uuid4())[:8]
    running_jobs[job_id] = {'logs':[],'stop':False,'status':'running',
                             'total':total,'done':0,'sucesso':0,'falha':0}
    jobs = get_jobs()
    jobs.append({'id':job_id,'tipo':'criar_campanhas','bc_id':bc_id,
                 'objetivo':objetivo,'criado_em':datetime.now().isoformat(),'status':'running'})
    save_jobs(jobs)

    def run():
        add_log(job_id, f'Iniciando campanhas em {total} contas — objetivo: {objetivo}', 'info')
        if item_id_input:
            add_log(job_id, f'Item ID resolvido: {item_id_input} (entrada: "{raw_item_input}")', 'info')

        for idx, adv_id in enumerate(adv_ids):
            if running_jobs[job_id]['stop']:
                add_log(job_id, 'Parado pelo usuario.', 'warn'); break
            add_log(job_id, f'[{idx+1}/{total}] Conta {adv_id}...')
            try:
                ts = datetime.now().strftime('%Y%m%d%H%M%S')
                if cbo_on:
                    camp_payload = {'advertiser_id':adv_id,'campaign_name':f"{campaign_name}_{ts}",
                                    'objective_type':obj_cfg['objective_type'],'budget_optimize_on':True,
                                    'budget_mode':'BUDGET_MODE_DAY','budget':budget,
                                    'campaign_type':'REGULAR_CAMPAIGN','special_industries':[]}
                else:
                    camp_payload = {'advertiser_id':adv_id,'campaign_name':f"{campaign_name}_{ts}",
                                    'objective_type':obj_cfg['objective_type'],'budget_mode':'BUDGET_MODE_INFINITE',
                                    'campaign_type':'REGULAR_CAMPAIGN','special_industries':[]}

                r_camp = tiktok_post('campaign/create', token, camp_payload)
                if r_camp.get('code') != 0:
                    raise Exception(f"Campanha: {r_camp.get('message', str(r_camp))}")
                camp_id = r_camp['data']['campaign_id']
                add_log(job_id, f'  Campanha criada: {camp_id}', 'success')

                adgroup_ids = []
                for ag_i in range(num_adgroups):
                    ag_name = adgroup_name if num_adgroups==1 else f"{adgroup_name}_{ag_i+1}"
                    ag_payload = {
                        'advertiser_id':adv_id,'campaign_id':camp_id,'adgroup_name':ag_name,
                        'placement_type':'PLACEMENT_TYPE_NORMAL','placements':['PLACEMENT_TIKTOK'],
                        'location_ids':get_location_ids(paises),'age_groups':age_groups,'gender':gender,
                        'budget_mode':'BUDGET_MODE_DAY','budget':budget,'schedule_type':'SCHEDULE_FROM_NOW',
                        'schedule_start_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'optimization_goal':obj_cfg['optimization_goal'],'billing_event':obj_cfg['billing_event'],
                        'bid_type':'BID_TYPE_NO_BID','pacing':'PACING_MODE_SMOOTH','operation_status':'ENABLE',
                    }
                    if objetivo=='CONVERSIONS' and pixel_id:
                        ag_payload['pixel_id']=pixel_id; ag_payload['optimization_event']=optimization
                    r_ag = tiktok_post('adgroup/create', token, ag_payload)
                    if r_ag.get('code') != 0:
                        add_log(job_id, f'  Ad Group {ag_i+1}: {r_ag.get("message")}', 'error'); continue
                    adgroup_ids.append(r_ag['data']['adgroup_id'])
                    add_log(job_id, f'  Ad Group {ag_i+1}: {r_ag["data"]["adgroup_id"]}', 'success')
                    time.sleep(0.5)

                if not adgroup_ids:
                    raise Exception("Nenhum Ad Group criado")

                resolved_identity_id   = identity_id
                resolved_item_id       = item_id_input

                if not resolved_item_id and post_code:
                    safe_code = post_code.replace('+', '%2B')
                    r_info = tiktok_get('tt_video/info', token, adv_id, {'auth_code': safe_code})
                    if r_info.get('code') == 0:
                        data_info        = r_info.get('data', {})
                        resolved_item_id = str(data_info.get('item_info', {}).get('item_id', ''))
                        if not resolved_identity_id:
                            resolved_identity_id = data_info.get('user_info', {}).get('identity_id', '')

                if not resolved_identity_id:
                    for id_type in ['TT_USER','AUTH_CODE','BC_AUTH_TT']:
                        r_ident = tiktok_get('identity/get', token, adv_id, {'identity_type': id_type})
                        if r_ident.get('code') == 0:
                            available = [i for i in r_ident.get('data',{}).get('identity_list',[])
                                         if i.get('available_status')=='AVAILABLE']
                            if available:
                                resolved_identity_id = available[0].get('identity_id',''); break
                    if not resolved_identity_id:
                        add_log(job_id, '  Nenhum identity encontrado', 'warn')

                for ag_id in adgroup_ids:
                    if not resolved_item_id:
                        add_log(job_id, '  Ad pulado: sem item_id', 'warn'); break
                    if not resolved_identity_id:
                        add_log(job_id, '  Ad pulado: sem identity_id', 'warn'); break
                    creative = {
                        'ad_name': f"Ad_{ts}", 'ad_format': post_type,
                        'identity_type': 'BC_AUTH_TT', 'identity_id': resolved_identity_id,
                        'identity_authorized_bc_id': '7607905792628621313',
                        'tiktok_item_id': resolved_item_id, 'call_to_action': cta,
                    }
                    if product_url: creative['landing_page_url'] = product_url
                    r_ad = tiktok_post('ad/create', token,
                                       {'advertiser_id':adv_id,'adgroup_id':ag_id,'creatives':[creative]})
                    if r_ad.get('code') != 0:
                        add_log(job_id, f'  Ad: {r_ad.get("message")}', 'error')
                    else:
                        add_log(job_id, f'  Ad criado: {r_ad.get("data",{}).get("ad_ids",[])}', 'success')
                    time.sleep(0.3)

                running_jobs[job_id]['sucesso'] += 1
                add_log(job_id, f'[{idx+1}/{total}] Conta concluida!', 'success')
            except Exception as e:
                running_jobs[job_id]['falha'] += 1
                add_log(job_id, f'[{idx+1}/{total}] Erro: {str(e)[:200]}', 'error')

            running_jobs[job_id]['done'] = idx + 1
            time.sleep(0.5)

        running_jobs[job_id]['status'] = 'done'
        s = running_jobs[job_id]['sucesso']
        f = running_jobs[job_id]['falha']
        add_log(job_id, f'Concluido: {s} sucessos, {f} falhas', 'success' if f==0 else 'warn')
        jobs = get_jobs()
        for j in jobs:
            if j['id']==job_id: j['status']='done'; j['sucesso']=s; j['falha']=f; break
        save_jobs(jobs)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id, 'total_contas': total})

# ── Job control ────────────────────────────────────────────────────────
@app.route('/api/job/<job_id>/logs')
def job_logs(job_id):
    offset = int(request.args.get('offset', 0))
    if job_id in running_jobs:
        j = running_jobs[job_id]
        logs=j['logs']; status=j['status']; done=j['done']
        total=j['total']; sucesso=j['sucesso']; falha=j['falha']
    else:
        log_file = os.path.join(LOGS_DIR, f'{job_id}.jsonl')
        logs = []
        if os.path.exists(log_file):
            with open(log_file,'r',encoding='utf-8') as f:
                for line in f:
                    try: logs.append(json.loads(line))
                    except: pass
        status='done'; done=0; total=0; sucesso=0; falha=0
    return jsonify({'logs':logs[offset:],'total_logs':len(logs),'status':status,
                    'done':done,'total':total,'sucesso':sucesso,'falha':falha})

@app.route('/api/job/<job_id>/stop', methods=['POST'])
def job_stop(job_id):
    if job_id in running_jobs:
        running_jobs[job_id]['stop'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Job nao encontrado'})

@app.route('/api/jobs')
def list_jobs():
    return jsonify(get_jobs()[-50:])

@app.route('/api/stats')
def api_stats():
    bcs    = get_bcs()
    tokens = get_tokens()
    jobs   = get_jobs()
    contas_total = sum(len(b.get('advertiser_ids', [])) for b in bcs)
    camp_jobs    = [j for j in jobs if j.get('tipo') == 'criar_campanhas']
    rodando      = len([j for j in running_jobs.values() if j.get('status') == 'running'])
    return jsonify({
        'total_bcs'     : len(bcs),
        'bcs_conectados': sum(1 for b in bcs if str(b['id']) in tokens),
        'total_contas'  : contas_total,
        'camp_sucesso'  : sum(j.get('sucesso', 0) for j in camp_jobs),
        'camp_falha'    : sum(j.get('falha', 0) for j in camp_jobs),
        'rodando'       : rodando,
        'total_sparks'  : len(get_sparks()),
    })

@app.route('/api/debug/identity/<int:bc_id>')
def debug_identity(bc_id):
    token = get_token_for_bc(bc_id)
    if not token: return jsonify({'error': 'sem token'})
    results = {}
    for id_type in ['TT_USER','AUTH_CODE','BC_AUTH_TT']:
        creative = {'ad_name':f'debug_{id_type}','ad_format':'SINGLE_VIDEO',
                    'identity_type':id_type,'identity_id':'7610243151726575617',
                    'tiktok_item_id':'7610455329033227541','call_to_action':'LEARN_MORE'}
        if id_type=='BC_AUTH_TT': creative['identity_authorized_bc_id']='7607905792628621313'
        r = tiktok_post('ad/create', token, {'advertiser_id':'7608267784702853138',
                                              'adgroup_id':'1858021589796114','creatives':[creative]})
        results[id_type] = {'code':r.get('code'),'message':r.get('message','')}
    return jsonify(results)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
