
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import os
import random
import datetime
import sqlite3
import threading

app = Flask(__name__, static_folder='.')
CORS(app, resources={r"/api/*": {"origins": "*"}})

# --- BANCO PERMANENTE ---
# Se tiver DATABASE_URL / POSTGRES_URL (Vercel Postgres, Supabase, Neon) usa Postgres permanente
# Se não tiver, usa SQLite local que é permanente na Manus/Render e temporário na Vercel (/tmp)
DB_PATH = os.environ.get('DB_PATH', 'blaze_history.db')
DATABASE_URL = os.environ.get('DATABASE_URL') or os.environ.get('POSTGRES_URL') or os.environ.get('POSTGRES_PRISMA_URL')
USE_POSTGRES = bool(DATABASE_URL)

if os.environ.get('VERCEL') and not USE_POSTGRES:
    DB_PATH = '/tmp/blaze_history.db'
    print("AVISO Vercel: sem DATABASE_URL, usando /tmp que apaga a cada deploy. Configure POSTGRES_URL para histórico permanente de meses!")

# Import psycopg2 só se precisar
pg_conn = None
if USE_POSTGRES:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        print(f"Usando Postgres permanente: {DATABASE_URL[:20]}...")
    except ImportError:
        print("psycopg2 não instalado, caindo para SQLite")
        USE_POSTGRES = False


db_lock = threading.Lock()

def get_pg_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)

def init_db():
    if USE_POSTGRES:
        conn = get_pg_conn()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS rolls (
                id TEXT PRIMARY KEY,
                roll INTEGER,
                color TEXT,
                timestamp TEXT,
                created_at TEXT,
                raw_json TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON rolls(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_roll ON rolls(roll)")
        conn.commit()
        conn.close()
        print("Postgres: tabela rolls criada/verificada")
        return
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS rolls (
                id TEXT PRIMARY KEY,
                roll INTEGER,
                color TEXT,
                timestamp TEXT,
                created_at TEXT,
                raw_json TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON rolls(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_roll ON rolls(roll)")
        conn.commit()
        conn.close()


def save_to_db(records):
    """Salva histórico real no banco, ignora duplicatas"""
    if not records:
        return 0
    if USE_POSTGRES:
        conn = get_pg_conn()
        c = conn.cursor()
        saved = 0
        for item in records:
            try:
                roll_val = item.get('roll')
                if roll_val is None:
                    roll_val = item.get('number')
                roll_val = int(roll_val)
                id_val = str(item.get('id') or item.get('_id') or f"{item.get('created_at')}_{roll_val}")
                ts = item.get('created_at') or item.get('timestamp') or datetime.datetime.now().isoformat()
                color_val = item.get('color')
                if isinstance(color_val, int):
                    color_str = 'white' if color_val==0 else 'red' if color_val==1 else 'black'
                else:
                    color_str = 'white' if roll_val==0 else 'red' if 1<=roll_val<=7 else 'black'
                c.execute("INSERT INTO rolls (id, roll, color, timestamp, created_at, raw_json) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING",
                          (id_val, roll_val, color_str, ts, ts, str(item)[:2000]))
                if c.rowcount > 0:
                    saved += 1
            except:
                continue
        conn.commit()
        conn.close()
        if saved>0:
            print(f"Postgres: {saved} novos salvos. Total: {count_db()}")
        return saved

    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        saved = 0
        for item in records:
            try:
                roll_val = item.get('roll')
                if roll_val is None:
                    roll_val = item.get('number')
                roll_val = int(roll_val)
                id_val = str(item.get('id') or item.get('_id') or f"{item.get('created_at')}_{roll_val}")
                ts = item.get('created_at') or item.get('timestamp') or datetime.datetime.now().isoformat()
                color_val = item.get('color')
                if isinstance(color_val, int):
                    color_str = 'white' if color_val==0 else 'red' if color_val==1 else 'black'
                else:
                    color_str = 'white' if roll_val==0 else 'red' if 1<=roll_val<=7 else 'black'
                c.execute("INSERT OR IGNORE INTO rolls (id, roll, color, timestamp, created_at, raw_json) VALUES (?,?,?,?,?,?)",
                          (id_val, roll_val, color_str, ts, ts, str(item)[:2000]))
                if c.rowcount > 0:
                    saved += 1
            except:
                continue
        conn.commit()
        conn.close()
        if saved>0:
            print(f"SQLite: {saved} novos salvos. Total: {count_db()}")
        return saved


def load_from_db(limit=1000):
    if USE_POSTGRES:
        conn = get_pg_conn()
        c = conn.cursor()
        c.execute("SELECT id, roll, color, timestamp, created_at FROM rolls ORDER BY timestamp DESC LIMIT %s", (limit,))
        rows = c.fetchall()
        conn.close()
        return [{'id': r[0], 'roll': r[1], 'color': r[2], 'timestamp': r[3], 'created_at': r[4]} for r in rows]
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, roll, color, timestamp, created_at FROM rolls ORDER BY timestamp DESC LIMIT ?", (limit,))
        rows = c.fetchall()
        conn.close()
    return [{'id': r[0], 'roll': r[1], 'color': r[2], 'timestamp': r[3], 'created_at': r[4]} for r in rows]


def count_db():
    if USE_POSTGRES:
        conn = get_pg_conn()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM rolls")
        cnt = c.fetchone()[0]
        conn.close()
        return cnt
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM rolls")
        cnt = c.fetchone()[0]
        conn.close()
    return cnt


init_db()

history_cache = []
mock_generated_at = None

def generate_mock_history(n=360, fixed_base=None):
    if fixed_base is None:
        fixed_base = datetime.datetime.now()
    mock = []
    for i in range(n):
        roll = random.randint(0, 14)
        if roll == 0:
            color = 'white'
        elif 1 <= roll <= 7:
            color = 'red'
        else:
            color = 'black'
        ts = fixed_base - datetime.timedelta(seconds=i*18)
        mock.append({
            'roll': roll,
            'color': color,
            'timestamp': ts.isoformat(),
            'id': f'persist-mock-{i}',
            'created_at': ts.isoformat()
        })
    return mock

def fetch_blaze_real():
    global history_cache, mock_generated_at
    urls = [
        'https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/history?limit=200',
        'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/history?limit=200',
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://blaze.bet.br/',
        'Origin': 'https://blaze.bet.br',
        'Accept': 'application/json'
    }
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=12)
            if r.status_code == 200:
                data = r.json()
                records = data if isinstance(data, list) else data.get('records', data.get('data', []))
                if records and len(records) > 10:
                    print(f"Blaze OK {url}: {len(records)}")
                    # Salva permanentemente
                    save_to_db(records)
                    history_cache = records
                    return records
        except Exception as e:
            print(f"Erro {url}: {e}")
    # Se falhar, tenta carregar do banco permanente
    db_records = load_from_db(500)
    if db_records:
        print(f"Usando DB permanente: {len(db_records)} giros")
        history_cache = db_records
        return db_records
    # Só gera mock se não tem nada no DB
    if not history_cache:
        if mock_generated_at is None:
            mock_generated_at = datetime.datetime.now()
        history_cache = generate_mock_history(360, fixed_base=mock_generated_at)
    return history_cache

@app.route('/')
def home():
    return send_from_directory('.', 'index.html')

@app.route('/api/rolls')
def get_rolls():
    try:
        limit = int(request.args.get('limit', 1000))
    except:
        limit = 1000
    # Sempre tenta buscar real e salvar
    if not history_cache:
        fetch_blaze_real()
    # Carrega do banco para garantir histórico de dias/meses
    db_data = load_from_db(limit)
    # Mescla: prioriza DB, se vazio usa cache
    source = db_data if len(db_data) >= 20 else history_cache
    result = []
    seen_ids = set()
    for item in source[:limit*2]:
        roll = item.get('roll')
        if roll is None:
            roll = item.get('number')
        try:
            roll = int(roll)
        except:
            continue
        id_val = str(item.get('id') or item.get('_id') or '')
        if id_val in seen_ids:
            continue
        seen_ids.add(id_val)
        color_val = item.get('color')
        if isinstance(color_val, int):
            display_color = 'white' if color_val==0 else 'red' if color_val==1 else 'black'
        else:
            if roll == 0:
                display_color = 'white'
            elif 1 <= roll <= 7:
                display_color = 'red'
            else:
                display_color = 'black'
        result.append({
            'roll': roll,
            'color': display_color,
            'display_color': display_color,
            'analysis_color': display_color,
            'is_special_06': False,
            'timestamp': item.get('created_at') or item.get('timestamp') or datetime.datetime.now().isoformat(),
            'id': id_val or f'id-{len(result)}'
        })
        if len(result) >= limit:
            break
    return jsonify(result)

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'cached': len(history_cache), 'db_total': count_db(), 'db_path': DB_PATH})

@app.route('/api/history/days')
def history_days():
    """Retorna quantos dias de histórico temos"""
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM rolls")
        cnt, min_ts, max_ts = c.fetchone()
        conn.close()
    return jsonify({'total': cnt, 'oldest': min_ts, 'newest': max_ts, 'db_path': DB_PATH})

if __name__ == '__main__':
    fetch_blaze_real()
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
