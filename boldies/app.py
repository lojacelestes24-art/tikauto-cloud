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
TIKTOK_APP_SECRET = os.environ.get('TIKTOK_APP_SECRET', '')
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

# ── Helpers API TikTok ─────────────────────────────────────────────────
def tiktok_get(endpoint, token, advertiser_id, params=None):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token}
    p = {'advertiser_id': advertiser_id}
    if params: p.update(params)
    r = requests.get(url, headers=headers, params=p, timeout=15)
    try:
        return r.json()
    except:
        return {'code': -1, 'message': f'Resposta inválida da API: {r.text[:200]}'}

def tiktok_post(endpoint, token, payload):
    url = f"{TIKTOK_API_BASE}/{endpoint}/"
    headers = {'Access-Token': token, 'Content-Type': 'application/json'}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    try:
        return r.json()
    except:
        return {'code': -1, 'message': f'Resposta inválida da API: {r.text[:200]}'}

def get_token_for_bc(bc_id):
    tokens = get_tokens()
    return tokens.get(str(bc_id), {}).get('access_token')

def get_advertiser_ids(token):
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

# ── Mapeamentos da API TikTok ──────────────────────────────────────────

# Configuração por objetivo: optimization_goal + billing_event corretos
OBJETIVO_CONFIG = {
    # VIDEO_VIEW depreciado na v1.3 — usar ENGAGED_VIEW (oficial)
    'VIDEO_VIEWS': {
        'objective_type'   : 'VIDEO_VIEWS',
        'optimization_goal': 'ENGAGED_VIEW',
        'billing_event'    : 'CPV',
    },
    'REACH': {
        'objective_type'   : 'REACH',
        'optimization_goal': 'REACH',
        'billing_event'    : 'CPM',
    },
    'TRAFFIC': {
        'objective_type'   : 'TRAFFIC',
        'optimization_goal': 'CLICK',
        'billing_event'    : 'CPC',
    },
    'ENGAGEMENT': {
        'objective_type'   : 'ENGAGEMENT',
        'optimization_goal': 'SHOW',
        'billing_event'    : 'CPM',
    },
    'LEAD_GENERATION': {
        'objective_type'   : 'LEAD_GENERATION',
        'optimization_goal': 'LEAD_GENERATION',
        'billing_event'    : 'OCPM',
    },
    'APP_PROMOTION': {
        'objective_type'   : 'APP_PROMOTION',
        'optimization_goal': 'INSTALL',
        'billing_event'    : 'OCPM',
    },
    'CONVERSIONS': {
        'objective_type'   : 'WEB_CONVERSIONS',
        'optimization_goal': 'CONVERT',
        'billing_event'    : 'OCPM',
    },
    'PRODUCT_SALES': {
        'objective_type'   : 'PRODUCT_SALES',
        'optimization_goal': 'CONVERT',
        'billing_event'    : 'OCPM',
    },
}

# IDs de localização TikTok (GeoName IDs)
LOCATION_MAP = {
    'BR': 3469034,  'PT': 2264397,  'US': 6252001,
    'MX': 3996063,  'AR': 3865483,  'CO': 3686110,
    'FR': 3017382,  'DE': 2921044,  'ES': 2510769,
    'IT': 3175395,  'GB': 2635167,  'AE': 290557,
    'CL': 3895114,  'PE': 3932488,  'EC': 3658394,
}

def get_location_ids(paises):
    ids = [str(LOCATION_MAP[p]) for p in paises if p in LOCATION_MAP]
    return ids if ids else ['3469034']  # Brasil como fallback

# ══════════════════════════════════════════════════════════════════════
# ROTAS PRINCIPAIS
# ══════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/version')
def version():
    return jsonify({'version': 'v2-debug-241'})

# ── OAuth ──────────────────────────────────────────────────────────────
@app.route('/oauth/url')
def oauth_url():
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
    auth_code = request.args.get('auth_code')
    state     = request.args.get('state', '')
    error     = request.args.get('error_code')

    if error:
        return render_template('index.html', oauth_error=error)
    if not auth_code:
        return render_template('index.html', oauth_error='Sem auth_code')

    result = _exchange_token(auth_code)
    if not result.get('ok'):
        return render_template('index.html', oauth_error=result.get('error', 'Erro desconhecido'))

    bc_id = None
    if state.startswith('bcid_'):
        try: bc_id = int(state.replace('bcid_', ''))
        except: pass

    token_data = result['data']
    tokens = get_tokens()
    key    = str(bc_id) if bc_id else token_data.get('advertiser_id', str(time.time()))
    tokens[key] = {
        'access_token'  : token_data['access_token'],
        'advertiser_id' : token_data.get('advertiser_id', ''),
        'scope'         : token_data.get('scope', ''),
        'token_type'    : token_data.get('token_type', 'bearer'),
        'saved_at'      : datetime.now().isoformat(),
        'bc_id'         : bc_id
    }
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
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token. Conecte via OAuth primeiro.'})
    advs = get_advertiser_ids(token)
    bcs = get_bcs()
    for bc in bcs:
        if bc['id'] == bc_id:
            bc['advertiser_ids'] = advs
            break
    save_bcs(bcs)
    return jsonify({'ok': True, 'advertiser_ids': advs, 'total': len(advs)})

# ── Pixel ──────────────────────────────────────────────────────────────
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
# CRIAÇÃO DE CONTAS EM MASSA (requer parceiro TikTok)
# ══════════════════════════════════════════════════════════════════════

@app.route('/api/criar-contas', methods=['POST'])
def criar_contas():
    return jsonify({
        'ok': False,
        'error': 'Criação de contas via API requer acesso de parceiro TikTok (não disponível na API padrão).'
    })

# ══════════════════════════════════════════════════════════════════════
# CRIAÇÃO DE CAMPANHAS EM MASSA
# ══════════════════════════════════════════════════════════════════════

@app.route('/api/criar-campanhas', methods=['POST'])
def criar_campanhas():
    d = request.json

    bc_id        = d['bc_id']
    adv_ids      = d.get('advertiser_ids', [])
    objetivo     = d.get('objetivo', 'VIDEO_VIEWS')
    paises       = d.get('paises', ['BR'])
    idade_min    = d.get('idade_min', 18)
    idade_max    = d.get('idade_max', 55)
    gender       = d.get('gender', 'GENDER_UNLIMITED')
    pixel_id     = d.get('pixel_id', '')
    optimization = d.get('optimization_event', 'PURCHASE')
    post_code    = d.get('post_code', '')
    item_id_input= d.get('item_id', '').strip()
    post_type    = d.get('post_type', 'SINGLE_VIDEO')
    identity_id  = d.get('identity_id', '')
    product_url  = d.get('product_url', '')
    cta          = d.get('cta', 'LEARN_MORE')
    num_adgroups = int(d.get('num_adgroups', 1))
    budget       = float(d.get('budget', 50))
    budget_mode  = 'BUDGET_MODE_DAY'
    cbo_on        = bool(d.get('budget_optimize_on', False))
    campaign_name = d.get('campaign_name', 'TikAuto')
    adgroup_name  = d.get('adgroup_name', 'AdGroup')

    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'ok': False, 'error': 'BC sem token OAuth'})

    if not adv_ids:
        bcs = get_bcs()
        bc  = next((b for b in bcs if b['id'] == bc_id), None)
        adv_ids = bc.get('advertiser_ids', []) if bc else []

    if not adv_ids:
        return jsonify({'ok': False, 'error': 'Nenhum advertiser ID disponível'})

    obj_cfg = OBJETIVO_CONFIG.get(objetivo, OBJETIVO_CONFIG['VIDEO_VIEWS'])

    # Faixas de idade TikTok
    age_map = {
        13: 'AGE_13_17', 18: 'AGE_18_24', 25: 'AGE_25_34',
        35: 'AGE_35_44', 45: 'AGE_45_54', 55: 'AGE_55_100'
    }
    age_groups = []
    for age_val, age_label in age_map.items():
        if idade_min <= age_val <= idade_max:
            age_groups.append(age_label)
    if not age_groups:
        age_groups = ['AGE_18_24', 'AGE_25_34', 'AGE_35_44']

    total   = len(adv_ids)
    job_id  = str(uuid.uuid4())[:8]
    running_jobs[job_id] = {
        'logs': [], 'stop': False, 'status': 'running',
        'total': total, 'done': 0, 'sucesso': 0, 'falha': 0
    }

    jobs = get_jobs()
    jobs.append({
        'id': job_id, 'tipo': 'criar_campanhas', 'bc_id': bc_id,
        'objetivo': objetivo, 'criado_em': datetime.now().isoformat(), 'status': 'running'
    })
    save_jobs(jobs)

    def run():
        add_log(job_id, f'Iniciando campanhas em {total} contas — objetivo: {objetivo}', 'info')

        for idx, adv_id in enumerate(adv_ids):
            if running_jobs[job_id]['stop']:
                add_log(job_id, 'Parado pelo usuário.', 'warn')
                break

            add_log(job_id, f'[{idx+1}/{total}] Conta {adv_id}...')
            try:
                ts = datetime.now().strftime('%Y%m%d%H%M%S')

                # ── 1. Criar Campanha ────────────────────────────────
                if cbo_on:
                    # CBO ativado: orçamento na campanha, TikTok distribui entre ad groups
                    camp_payload = {
                        'advertiser_id'    : adv_id,
                        'campaign_name'    : f"{campaign_name}_{ts}",
                        'objective_type'   : obj_cfg['objective_type'],
                        'budget_optimize_on': True,
                        'budget_mode'      : 'BUDGET_MODE_DAY',
                        'budget'           : budget,
                        'campaign_type'    : 'REGULAR_CAMPAIGN',
                        'special_industries': [],
                    }
                else:
                    # CBO desativado: campanha ilimitada, orçamento fica no ad group
                    camp_payload = {
                        'advertiser_id'    : adv_id,
                        'campaign_name'    : f"{campaign_name}_{ts}",
                        'objective_type'   : obj_cfg['objective_type'],
                        'budget_mode'      : 'BUDGET_MODE_INFINITE',
                        'campaign_type'    : 'REGULAR_CAMPAIGN',
                        'special_industries': [],
                    }

                r_camp = tiktok_post('campaign/create', token, camp_payload)
                if r_camp.get('code') != 0:
                    raise Exception(f"Campanha: {r_camp.get('message', str(r_camp))}")

                camp_id = r_camp['data']['campaign_id']
                add_log(job_id, f'  ✓ Campanha criada: {camp_id}', 'success')

                # ── 2. Criar Ad Group(s) ─────────────────────────────
                adgroup_ids = []
                for ag_i in range(num_adgroups):
                    ag_name = adgroup_name if num_adgroups == 1 else f"{adgroup_name}_{ag_i+1}"

                    ag_payload = {
                        'advertiser_id'    : adv_id,
                        'campaign_id'      : camp_id,
                        'adgroup_name'     : ag_name,
                        'placement_type'   : 'PLACEMENT_TYPE_NORMAL',
                        'placements'       : ['PLACEMENT_TIKTOK'],
                        'location_ids'     : get_location_ids(paises),
                        'age_groups'       : age_groups,
                        'gender'           : gender,
                        'budget_mode'      : budget_mode,
                        'budget'           : budget,
                        'schedule_type'    : 'SCHEDULE_FROM_NOW',
                        'schedule_start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'optimization_goal': obj_cfg['optimization_goal'],
                        'billing_event'    : obj_cfg['billing_event'],
                        'bid_type'         : 'BID_TYPE_NO_BID',
                        'pacing'           : 'PACING_MODE_SMOOTH',
                        'operation_status' : 'ENABLE',
                    }

                    # Pixel — só para CONVERSIONS
                    if objetivo == 'CONVERSIONS' and pixel_id:
                        ag_payload['pixel_id']           = pixel_id
                        ag_payload['optimization_event'] = optimization

                    r_ag = tiktok_post('adgroup/create', token, ag_payload)
                    if r_ag.get('code') != 0:
                        add_log(job_id, f'  ✗ Ad Group {ag_i+1}: {r_ag.get("message", str(r_ag))}', 'error')
                        continue

                    ag_id = r_ag['data']['adgroup_id']
                    adgroup_ids.append(ag_id)
                    add_log(job_id, f'  ✓ Ad Group {ag_i+1}: {ag_id}', 'success')
                    time.sleep(0.5)

                if not adgroup_ids:
                    raise Exception("Nenhum Ad Group criado com sucesso")

                # ── 3. Resolver item_id e identity a partir do auth_code (Spark Ad) ──
                resolved_identity_id   = identity_id
                resolved_identity_type = 'TT_USER'
                resolved_item_id       = item_id_input  # usa item_id manual se fornecido

                if resolved_item_id:
                    add_log(job_id, f'  ✓ Item ID manual: {resolved_item_id}', 'info')
                elif not post_code:
                    add_log(job_id, '  ⚠ Sem código de post nem item_id — ad será pulado', 'warn')
                else:
                    # Tenta /tt_video/info/ primeiro (mais direto)
                    safe_code = post_code.replace('+', '%2B')
                    r_info = tiktok_get('tt_video/info', token, adv_id,
                                        {'auth_code': safe_code})
                    if r_info.get('code') == 0:
                        data_info = r_info.get('data', {})
                        resolved_item_id = str(data_info.get('item_info', {}).get('item_id', ''))
                        if not resolved_identity_id:
                            resolved_identity_id   = data_info.get('user_info', {}).get('identity_id', '')
                            resolved_identity_type = data_info.get('user_info', {}).get('identity_type', 'AUTH_CODE')
                        add_log(job_id,
                            f'  ✓ Post info: item_id={resolved_item_id} | identity={resolved_identity_id}', 'info')
                    else:
                        # Fallback: busca via /tt_video/list/ e filtra pelo auth_code
                        add_log(job_id, '  ↻ Buscando post via lista (fallback)...', 'info')
                        found = False
                        page  = 1
                        while not found:
                            r_list = tiktok_get('tt_video/list', token, adv_id,
                                                {'page': page, 'page_size': 50})
                            add_log(job_id, f'  [debug] tt_video/list code={r_list.get("code")} msg={r_list.get("message", "")[:80]}', 'info')
                            if r_list.get('code') != 0:
                                add_log(job_id,
                                    f'  ✗ Erro ao listar posts: {r_list.get("message", str(r_list))}', 'error')
                                break
                            posts      = r_list.get('data', {}).get('list', [])
                            page_info  = r_list.get('data', {}).get('page_info', {})
                            for post in posts:
                                item_info = post.get('item_info', {})
                                if item_info.get('auth_code', '') == post_code:
                                    resolved_item_id = str(item_info.get('item_id', ''))
                                    if not resolved_identity_id:
                                        resolved_identity_id   = post.get('user_info', {}).get('identity_id', '')
                                        resolved_identity_type = post.get('user_info', {}).get('identity_type', 'AUTH_CODE')
                                    add_log(job_id,
                                        f'  ✓ Post encontrado: item_id={resolved_item_id} | identity={resolved_identity_id}', 'info')
                                    found = True
                                    break
                            if found or page >= page_info.get('total_page', 1):
                                break
                            page += 1
                        if not found:
                            add_log(job_id, '  ✗ Post não encontrado na lista — verifique o auth_code', 'error')

                # fallback: busca identity na conta se ainda não tiver
                if not resolved_identity_id:
                    for id_type in ['TT_USER', 'AUTH_CODE', 'BC_AUTH_TT']:
                        r_ident = tiktok_get('identity/get', token, adv_id,
                                             {'identity_type': id_type})
                        if r_ident.get('code') == 0:
                            ident_list = r_ident.get('data', {}).get('identity_list', [])
                            available  = [i for i in ident_list if i.get('available_status') == 'AVAILABLE']
                            if available:
                                resolved_identity_id   = available[0].get('identity_id', '')
                                resolved_identity_type = id_type
                                add_log(job_id,
                                    f'  ✓ Identity [{id_type}]: {resolved_identity_id}', 'info')
                                break
                    if not resolved_identity_id:
                        add_log(job_id, '  ⚠ Nenhum identity encontrado para esta conta', 'warn')

                # ── 4. Criar Ad (Spark Ad) ───────────────────────────────
                for ag_id in adgroup_ids:
                    if not resolved_item_id:
                        add_log(job_id, '  ⚠ Ad pulado: item_id não resolvido (verifique o auth_code)', 'warn')
                        break
                    if not resolved_identity_id:
                        add_log(job_id, '  ⚠ Ad pulado: sem identity_id', 'warn')
                        break

                    ad_name = f"Ad_{datetime.now().strftime('%Y%m%d%H%M%S')}"

                    creative = {
                        'ad_name'                   : ad_name,
                        'ad_format'                 : post_type,
                        'identity_type'             : 'BC_AUTH_TT',
                        'identity_id'               : resolved_identity_id,
                        'identity_authorized_bc_id' : '7607905792628621313',
                        'tiktok_item_id'            : resolved_item_id,
                        'call_to_action'            : cta,
                    }

                    if product_url:
                        creative['landing_page_url'] = product_url

                    ad_payload = {
                        'advertiser_id': adv_id,
                        'adgroup_id'   : ag_id,
                        'creatives'    : [creative],
                    }

                    r_ad = tiktok_post('ad/create', token, ad_payload)
                    if r_ad.get('code') != 0:
                        add_log(job_id, f'  ✗ Ad: {r_ad.get("message", str(r_ad))}', 'error')
                    else:
                        ad_ids = r_ad.get('data', {}).get('ad_ids', [])
                        add_log(job_id, f'  ✓ Ad criado: {ad_ids}', 'success')
                    time.sleep(0.3)

                running_jobs[job_id]['sucesso'] += 1
                add_log(job_id, f'[{idx+1}/{total}] ✓ Conta {adv_id} concluída!', 'success')

            except Exception as e:
                running_jobs[job_id]['falha'] += 1
                add_log(job_id, f'[{idx+1}/{total}] ✗ Erro conta {adv_id}: {str(e)[:200]}', 'error')

            running_jobs[job_id]['done'] = idx + 1
            time.sleep(0.5)

        running_jobs[job_id]['status'] = 'done'
        s = running_jobs[job_id]['sucesso']
        f = running_jobs[job_id]['falha']
        add_log(job_id, f'✓ Concluído: {s} sucessos, {f} falhas', 'success' if f == 0 else 'warn')

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

# ── Debug identity ────────────────────────────────────────────────────
@app.route('/api/debug/identity/<int:bc_id>')
def debug_identity(bc_id):
    token = get_token_for_bc(bc_id)
    if not token:
        return jsonify({'error': 'sem token'})
    bcs = get_bcs()
    bc = next((b for b in bcs if b['id'] == bc_id), None)
    adv_id = '7608267784702853138'  # conta de teste
    results = {}
    identity_id = '7610243151726575617'
    item_id     = '7610455329033227541'
    bc_id_str   = '7607905792628621313'

    for id_type in ['TT_USER', 'AUTH_CODE', 'BC_AUTH_TT']:
        creative = {
            'ad_name'       : f'debug_{id_type}',
            'ad_format'     : 'SINGLE_VIDEO',
            'identity_type' : id_type,
            'identity_id'   : identity_id,
            'tiktok_item_id': item_id,
            'call_to_action': 'LEARN_MORE',
        }
        if id_type == 'BC_AUTH_TT':
            creative['identity_authorized_bc_id'] = bc_id_str
        payload = {
            'advertiser_id': adv_id,
            'adgroup_id'   : '1858021589796114',  # ad group existente
            'creatives'    : [creative],
        }
        r = tiktok_post('ad/create', token, payload)
        results[id_type] = {'code': r.get('code'), 'message': r.get('message', '')}
    return jsonify(results)

# ── Job control ────────────────────────────────────────────────────────
@app.route('/api/job/<job_id>/logs')
def job_logs(job_id):
    offset = int(request.args.get('offset', 0))
    if job_id in running_jobs:
        logs    = running_jobs[job_id]['logs']
        status  = running_jobs[job_id]['status']
        done    = running_jobs[job_id]['done']
        total   = running_jobs[job_id]['total']
        sucesso = running_jobs[job_id]['sucesso']
        falha   = running_jobs[job_id]['falha']
    else:
        log_file = os.path.join(LOGS_DIR, f'{job_id}.jsonl')
        logs = []
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try: logs.append(json.loads(line))
                    except: pass
        status = 'done'; done = 0; total = 0; sucesso = 0; falha = 0
    return jsonify({
        'logs'      : logs[offset:],
        'total_logs': len(logs),
        'status'    : status,
        'done'      : done,
        'total'     : total,
        'sucesso'   : sucesso,
        'falha'     : falha,
    })

@app.route('/api/job/<job_id>/stop', methods=['POST'])
def job_stop(job_id):
    if job_id in running_jobs:
        running_jobs[job_id]['stop'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False, 'error': 'Job não encontrado'})

@app.route('/api/jobs')
def list_jobs():
    return jsonify(get_jobs()[-50:])

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
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
