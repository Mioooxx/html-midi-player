import json
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

# 配置并行下载参数
MAX_WORKERS = 10  # 并发线程数
RETRY_ATTEMPTS = 3  # 重试次数
RETRY_DELAY = 1  # 重试延迟（秒）

# 用于线程安全的打印
print_lock = Lock()

def thread_safe_print(*args, **kwargs):
    """线程安全的打印函数"""
    with print_lock:
        print(*args, **kwargs)


def get_pitches_array(min_pitch, max_pitch):
    return list(range(min_pitch, max_pitch + 1))


def download_file(url, save_path, file_description=""):
    """
    下载单个文件，支持重试
    
    Args:
        url: 文件URL
        save_path: 保存路径
        file_description: 文件描述（用于日志）
    
    Returns:
        (success, message) 元组
    """
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                file_contents = response.read()
            
            # 确保目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # 保存文件
            with open(save_path, 'wb') as file:
                file.write(file_contents)
            
            return (True, f"Downloaded {file_description}")
        
        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
                continue
            else:
                return (False, f"Failed to download {file_description}: {str(e)}")
    
    return (False, f"Failed to download {file_description} after {RETRY_ATTEMPTS} attempts")


base_url = 'https://storage.googleapis.com/magentadata/js/soundfonts'
soundfont_path = 'sgm_plus'
soundfont_json_url = f"{base_url}/{soundfont_path}/soundfont.json"

# Download soundfont.json
soundfont_json = ""

if not os.path.exists('soundfont.json'):
    try:
        with urllib.request.urlopen(soundfont_json_url, timeout=30) as response:
            soundfont_json = response.read()

        # Save soundfont.json
        with open('soundfont.json', 'wb') as file:
            file.write(soundfont_json)
        
        thread_safe_print("✓ Downloaded soundfont.json")

    except Exception as e:
        thread_safe_print(f"✗ Failed to download soundfont.json: {str(e)}")

else:
    # If file exists, get it from the file system
    with open('soundfont.json', 'rb') as file:
        soundfont_json = file.read()
    
    thread_safe_print("✓ Using existing soundfont.json")

# Parse soundfont.json
soundfont_data = json.loads(soundfont_json)

if soundfont_data is not None:
    
    # 第一步：并行下载所有乐器的 instrument.json
    print("\n📥 Stage 1: Downloading instrument.json files...")
    
    instrument_names = list(soundfont_data['instruments'].keys())
    
    def download_instrument_json(instrument_id):
        """下载单个乐器的 instrument.json"""
        instrument_name = soundfont_data['instruments'][instrument_id]
        instrument_path = f"{soundfont_path}/{instrument_name}"
        instrument_json_url = f"{base_url}/{instrument_path}/instrument.json"
        save_path = f"{instrument_name}/instrument.json"
        
        if os.path.exists(save_path):
            thread_safe_print(f"⊘ Skipping {save_path} - File already exists")
            return (instrument_id, instrument_name, True, None)
        
        success, message = download_file(instrument_json_url, save_path, save_path)
        
        if success:
            thread_safe_print(f"✓ {message}")
        else:
            thread_safe_print(f"✗ {message}")
        
        return (instrument_id, instrument_name, success, message)
    
    # 使用线程池并行下载
    instrument_results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_instrument_json, iid): iid for iid in instrument_names}
        for future in as_completed(futures):
            instrument_id, instrument_name, success, message = future.result()
            instrument_results[instrument_name] = success
    
    # 第二步：并行下载所有音频文件
    print("\n📥 Stage 2: Downloading audio files...")
    
    # 收集所有待下载的任务
    download_tasks = []
    
    for instrument_id, instrument_name in soundfont_data['instruments'].items():
        
        if not os.path.isdir(instrument_name):
            os.makedirs(instrument_name, exist_ok=True)
        
        instrument_path = f"{soundfont_path}/{instrument_name}"
        instrument_json_path = f"{instrument_name}/instrument.json"
        
        # 检查 instrument.json 是否存在
        if not os.path.exists(instrument_json_path):
            thread_safe_print(f"⚠ Skipping {instrument_name} - instrument.json not found")
            continue
        
        # 读取 instrument.json
        try:
            with open(instrument_json_path, 'rb') as file:
                instrument_json = file.read()
            instrument_data = json.loads(instrument_json)
        except Exception as e:
            thread_safe_print(f"✗ Failed to parse {instrument_name}/instrument.json: {str(e)}")
            continue
        
        if instrument_data is None:
            thread_safe_print(f"✗ Failed to parse instrument.json for {instrument_name}")
            continue
        
        # 收集该乐器的所有下载任务
        for velocity in instrument_data['velocities']:
            pitches = get_pitches_array(instrument_data['minPitch'], instrument_data['maxPitch'])
            
            for pitch in pitches:
                file_name = f'p{pitch}_v{velocity}.mp3'
                save_path = f"{instrument_name}/{file_name}"
                
                # 跳过已存在的文件
                if os.path.exists(save_path):
                    continue
                
                file_url = f"{base_url}/{instrument_path}/{file_name}"
                download_tasks.append((file_url, save_path, file_name, instrument_name))
    
    # 并行下载所有音频文件
    total_tasks = len(download_tasks)
    progress = {'completed': 0}
    
    if total_tasks > 0:
        thread_safe_print(f"\n⏱ Downloading {total_tasks} audio files with {MAX_WORKERS} workers...")
        
        def download_audio_file(task):
            file_url, save_path, file_name, instrument_name = task
            success, message = download_file(file_url, save_path, f"{instrument_name}/{file_name}")
            progress['completed'] += 1
            
            if success:
                thread_safe_print(f"✓ [{progress['completed']}/{total_tasks}] {message}")
            else:
                thread_safe_print(f"✗ [{progress['completed']}/{total_tasks}] {message}")
            
            return success
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_audio_file, task) for task in download_tasks]
            completed = sum(1 for future in as_completed(futures) if future.result())
        
        print(f"\n✓ Completed: {completed}/{total_tasks} files downloaded successfully")
    else:
        print("\n✓ All audio files already exist")

else:
    print('✗ Failed to parse soundfont.json')
