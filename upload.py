import os
import pickle
import json
import datetime
import hashlib
import time
import threading
import argparse
import signal
import sys
import tempfile
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# --- Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive']
CACHE_FILE = 'folder_cache.json'
LOG_FILE = 'backup_log.json'
UPLOADED_DB = 'uploaded_files.json'
# I/O Bound task ke liye 20-25 workers optimized hain
MAX_WORKERS = 20 
SAVE_INTERVAL = 30 
CHUNK_SIZE = 5 * 1024 * 1024 # 5MB chunks for RAM safety
MAX_CACHE_SIZE = 1500 # Folder contents cache limit

# --- Global Locks & States ---
_db_lock = threading.Lock()
_cache_lock = threading.Lock()
_log_lock = threading.Lock()
_service_lock = threading.Lock()
_fcl_meta_lock = threading.Lock()

thread_local = threading.local()
folder_cache = {}
uploaded_db = {}  
drive_folder_contents = OrderedDict() # LRU Cache implementation
_folder_creation_locks = {}
stop_event = threading.Event()

def atomic_save_json(data, filename):
    """Data corruption se bachne ke liye atomic write operation."""
    temp_dir = os.path.dirname(os.path.abspath(filename))
    fd, temp_path = tempfile.mkstemp(dir=temp_dir, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as tmp:
            json.dump(data, tmp)
        os.replace(temp_path, filename) # OS-level atomic replace
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e

def signal_handler(sig, frame):
    """Graceful shutdown logic."""
    if not stop_event.is_set():
        print("\n🛑 Shutdown signal received. Saving progress and exiting...")
        stop_event.set()
        save_databases()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_folder_creation_lock(key):
    with _fcl_meta_lock:
        if key not in _folder_creation_locks:
            _folder_creation_locks[key] = threading.Lock()
        return _folder_creation_locks[key]

def get_service():
    """Thread-safe service with atomic token refresh."""
    if not hasattr(thread_local, "service"):
        with _service_lock:
            creds = None
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    creds = pickle.load(token)
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0, open_browser=False)
                
                # Atomic pickle save
                with tempfile.NamedTemporaryFile('wb', delete=False, dir='.') as tmp:
                    pickle.dump(creds, tmp)
                    tmp_name = tmp.name
                os.replace(tmp_name, 'token.pickle')
            
            thread_local.service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return thread_local.service

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        if os.path.getsize(file_path) == 0:
            return hash_md5.hexdigest()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(131072), b""): # 128KB chunks for speed
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return None

def load_databases():
    global folder_cache, uploaded_db
    def safe_load(path, default):
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ Warning: {path} read failed ({e}). Starting fresh.")
        return default

    folder_cache = safe_load(CACHE_FILE, {})
    uploaded_db = safe_load(UPLOADED_DB, {})

def save_databases():
    try:
        with _db_lock:
            atomic_save_json(uploaded_db, UPLOADED_DB)
        with _cache_lock:
            atomic_save_json(folder_cache, CACHE_FILE)
    except Exception as e:
        write_log("SAVE_DB_ERR", "N/A", str(e))

def write_log(status, path, reason=""):
    entry = {"ts": datetime.datetime.now().isoformat(), "status": status, "path": path, "msg": reason}
    with _log_lock:
        with open(LOG_FILE, 'a', encoding='utf-8') as f: 
            f.write(json.dumps(entry) + "\n")

def execute_with_retry(func, *args, **kwargs):
    for n in range(5):
        if stop_event.is_set(): return None
        try:
            return func(*args, **kwargs)
        except HttpError as e:
            if e.resp.status in [403, 429, 500, 502, 503, 504]:
                time.sleep((2 ** n) + 1.0) # Adaptive backoff
                continue
            raise
        except Exception as e:
            if n < 4: 
                time.sleep(1)
                continue
            raise
    return None

def fetch_folder_contents(service, folder_id):
    """LRU Cache mechanism for folder contents."""
    with _cache_lock:
        if folder_id in drive_folder_contents:
            drive_folder_contents.move_to_end(folder_id) # Refresh position
            return drive_folder_contents[folder_id]
    
    files = {}
    page_token = None
    try:
        while not stop_event.is_set():
            query = f"'{folder_id}' in parents and trashed=false"
            results = execute_with_retry(service.files().list(
                q=query, fields="nextPageToken, files(id, name, md5Checksum, size)", 
                pageSize=1000, pageToken=page_token).execute)
            
            if not results: break
            for f in results.get('files', []):
                files[f['name']] = {'id': f['id'], 'md5': f.get('md5Checksum'), 'size': int(f.get('size', 0))}
            
            page_token = results.get('nextPageToken')
            if not page_token: break
    except Exception as e:
        write_log("FETCH_ERR", folder_id, str(e))

    with _cache_lock:
        if len(drive_folder_contents) >= MAX_CACHE_SIZE:
            drive_folder_contents.popitem(last=False) # Remove oldest
        drive_folder_contents[folder_id] = files
    return files

def get_or_create_folder(service, folder_name, parent_id):
    cache_key = f"{parent_id}_{folder_name}"
    with _cache_lock:
        if cache_key in folder_cache: return folder_cache[cache_key]

    with get_folder_creation_lock(cache_key):
        with _cache_lock:
            if cache_key in folder_cache: return folder_cache[cache_key]

        contents = fetch_folder_contents(service, parent_id)
        if folder_name in contents:
            f_id = contents[folder_name]['id']
        else:
            meta = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
            folder = execute_with_retry(service.files().create(body=meta, fields='id').execute)
            f_id = folder.get('id') if folder else None

        if f_id:
            with _cache_lock: folder_cache[cache_key] = f_id
        return f_id

def upload_worker(file_path, root_folder_id, source_path):
    if stop_event.is_set(): return "CANCELLED"
    service = get_service()
    filename = os.path.basename(file_path)
    
    try:
        stat = os.stat(file_path)
        mtime, size = stat.st_mtime, stat.st_size
        
        with _db_lock:
            existing = uploaded_db.get(file_path)
        
        # 1. Lazy check (Fast Path)
        if existing and existing.get('mtime') == mtime and existing.get('size') == size:
            return "SKIPPED_LOCAL_FAST"

        # 2. Heavy check (Slow Path: MD5)
        file_hash = calculate_md5(file_path)
        if existing and existing.get('md5') == file_hash:
            with _db_lock:
                uploaded_db[file_path].update({"mtime": mtime, "size": size})
            return "SKIPPED_LOCAL_MD5"

        # Folder construction
        rel_path = os.path.relpath(file_path, source_path)
        path_parts = rel_path.split(os.sep)[:-1]
        curr_pid = root_folder_id
        for part in path_parts:
            curr_pid = get_or_create_folder(service, part, curr_pid)
            if not curr_pid: return "ERR_FOLDER"

        ext = os.path.splitext(filename)[1].lower().strip('.') or 'others'
        ext_id = get_or_create_folder(service, ext, curr_pid)

        # 3. Drive Check
        drive_files = fetch_folder_contents(service, ext_id)
        if filename in drive_files:
            df = drive_files[filename]
            if df['md5'] == file_hash or (df['size'] == size and df['md5'] is None):
                with _db_lock:
                    uploaded_db[file_path] = {"md5": file_hash, "id": df['id'], "mtime": mtime, "size": size}
                return "SKIPPED_DRIVE"

        # 4. Upload
        if not os.access(file_path, os.R_OK): return "ERR_PERM"
        meta = {'name': filename, 'parents': [ext_id]}
        media = MediaFileUpload(file_path, resumable=True, chunksize=CHUNK_SIZE)
        up_file = execute_with_retry(service.files().create(body=meta, media_body=media, fields='id').execute)
        
        if up_file:
            f_id = up_file.get('id')
            with _cache_lock:
                if ext_id in drive_folder_contents:
                    drive_folder_contents[ext_id][filename] = {"id": f_id, "md5": file_hash, "size": size}
            with _db_lock:
                uploaded_db[file_path] = {"md5": file_hash, "id": f_id, "mtime": mtime, "size": size}
            return "UPLOADED"
        
        return "FAILED"

    except Exception as e:
        write_log("WORKER_ERR", file_path, str(e))
        return f"ERR_{type(e).__name__}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default="/storage/emulated/0")
    args = parser.parse_args()

    print(f"🚀 [Elite-Edition] Production Backup Starting (Workers: {MAX_WORKERS})")
    load_databases()
    
    root_folder_id = get_or_create_folder(get_service(), "Android_Smart_Backup", "root")

    all_files = []
    for root, dirs, files in os.walk(args.source):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        if any(x in root for x in ['/Android/data', '/Android/obb', '/cache']): continue
        for file in files:
            if not file.startswith('.'):
                all_files.append(os.path.join(root, file))

    total = len(all_files)
    print(f"📦 Found {total} files. Starting sync engine...")
    
    stats = {"UPLOADED": 0, "SKIPPED": 0, "FAILED": 0, "ERROR": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(upload_worker, fp, root_folder_id, args.source): fp for fp in all_files}
        
        with tqdm(total=total, unit="file", desc="Backup Progress") as pbar:
            for i, future in enumerate(as_completed(futures)):
                if stop_event.is_set(): break
                try:
                    res = future.result()
                    if res == "UPLOADED": stats["UPLOADED"] += 1
                    elif "SKIPPED" in res: stats["SKIPPED"] += 1
                    elif "ERR_" in res: 
                        stats["ERROR"] += 1
                        write_log("WORKER_FAIL", futures[future], res)
                    else: 
                        stats["FAILED"] += 1
                        write_log("GENERIC_FAIL", futures[future], res)
                    
                    pbar.set_description(f"Syncing: {os.path.basename(futures[future])[:20]}")
                    pbar.update(1)
                    if i % SAVE_INTERVAL == 0: save_databases()
                except Exception as e:
                    stats["ERROR"] += 1
                    write_log("CRITICAL_THREAD_FAIL", futures[future], str(e))

    save_databases()
    print(f"\n✅ BACKUP FINISHED.")
    print(f"📊 Stats: {stats}")
    print(f"📝 Full log available at: {LOG_FILE}")

if __name__ == "__main__":
    main()
