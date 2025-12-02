

from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks, Request
from fastapi.responses import Response
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from collections import deque
from dataclasses import dataclass, field
import uvicorn
import os
import struct
import hashlib
import time
import json
import threading
from pathlib import Path
from typing import Dict, List, Optional
import requests
from collections import defaultdict
import logging
import zlib
import redis
import uuid


ACCESS_WINDOW_SECONDS = int(os.getenv("ACCESS_WINDOW_SECONDS", "300")) 
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.3")) 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] %(message)s'
)
logger = logging.getLogger("StoreService")

app = FastAPI(title="Store Service")


limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


STORE_ID = os.getenv("STORE_ID", "store-1")
STORE_URL = os.getenv("STORE_URL", "http://localhost:8000")
VOLUME_DIRECTORY = os.getenv("VOLUME_DIRECTORY", "/data/volumes")
MAX_VOLUME_SIZE = int(os.getenv("MAX_VOLUME_SIZE", 4 * 1024 * 1024 * 1024))
COMPACTION_THRESHOLD = float(os.getenv("COMPACTION_EFFICIENCY_THRESHOLD", 0.60))
DIRECTORY_SERVICE_URL = os.getenv("DIRECTORY_SERVICE_URL", "http://nginx")
REPLICATION_MANAGER_URL = os.getenv("REPLICATION_MANAGER_URL", "http://replication:9003")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
STATS_REPORT_INTERVAL = int(os.getenv("STATS_REPORT_INTERVAL", 60))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 30))


try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=False)
    redis_client.ping()
    logger.info("  Connected to Redis successfully")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Cache push disabled.")
    redis_client = None


volumes: Dict[str, 'Volume'] = {}
volume_lock = threading.RLock()
access_stats = defaultdict(int)
access_stats_lock = threading.Lock()
stats_window_start = time.time()


compaction_in_progress = False
compaction_lock = threading.Lock()


registration_successful = False

@dataclass
class AccessEvent:
    timestamp: float
    count: int = 1
    
    def __repr__(self):
        return f"AccessEvent(ts={self.timestamp:.2f}, count={self.count})"
    

class AccessTracker:
    
    def __init__(self, window_seconds: int = 300, alpha: float = 0.3):
        self.window_seconds = window_seconds
        self.alpha = alpha
        
        
        self.events: Dict[str, deque] = {}
        self.ewma_rates: Dict[str, float] = {}
        self.running_sums: Dict[str, int] = {}  
        self.last_update: Dict[str, float] = {}
        
        
        self.lock = threading.RLock()
        
        
        self.total_events_recorded = 0
        self.total_events_pruned = 0
        self.photos_tracked = 0
        
        logger.info(f"AccessTracker initialized: window={window_seconds}s, alpha={alpha}")
    
    def record_access(self, photo_id: str, count: int = 1) -> None:
    
        now = time.time()
        
        with self.lock:
            
            if photo_id not in self.events:
                self.events[photo_id] = deque()
                self.ewma_rates[photo_id] = 0.0
                self.running_sums[photo_id] = 0
                self.last_update[photo_id] = now
                self.photos_tracked += 1
            
            
            self.events[photo_id].append(AccessEvent(timestamp=now, count=count))
            self.running_sums[photo_id] += count
            self.last_update[photo_id] = now
            self.total_events_recorded += count
            
           
            self._prune_old_events(photo_id, now)
            
           
            self._update_ewma(photo_id, now)
    
    def _prune_old_events(self, photo_id: str, current_time: float) -> None:
        cutoff = current_time - self.window_seconds
        events = self.events[photo_id]
        
        pruned_count = 0
        while events and events[0].timestamp < cutoff:
            event = events.popleft()
            self.running_sums[photo_id] -= event.count
            pruned_count += 1
        
        if pruned_count > 0:
            self.total_events_pruned += pruned_count
        
        
        if not events:
            del self.events[photo_id]
            del self.ewma_rates[photo_id]
            del self.running_sums[photo_id]
            del self.last_update[photo_id]
            self.photos_tracked -= 1
    
    def _update_ewma(self, photo_id: str, current_time: float) -> None:
        
        events = self.events[photo_id]
        
        if not events:
            self.ewma_rates[photo_id] = 0.0
            return
        
        
        total_accesses = self.running_sums[photo_id]
        
        
        window_start = events[0].timestamp
        window_duration_seconds = current_time - window_start
        window_duration_minutes = max(window_duration_seconds / 60.0, 0.01)
        
       
        raw_rate = total_accesses / window_duration_minutes
        
        
        old_ewma = self.ewma_rates[photo_id]
        new_ewma = (self.alpha * raw_rate) + ((1 - self.alpha) * old_ewma)
        self.ewma_rates[photo_id] = new_ewma
    
    def get_stats_for_reporting(self) -> List[dict]:
        with self.lock:
            now = time.time()
            stats = []
            
            for photo_id in list(self.events.keys()):
                self._prune_old_events(photo_id, now)
                
                events = self.events.get(photo_id)
                if not events:
                    continue
                
                self._update_ewma(photo_id, now)
                
                total_accesses = self.running_sums[photo_id]
                window_start = events[0].timestamp
                window_end = now
                window_duration_minutes = max((window_end - window_start) / 60.0, 0.01)
                
                raw_rate = total_accesses / window_duration_minutes
                
                stats.append({
                    'photo_id': photo_id,
                    'ewma_rate_per_minute': round(self.ewma_rates[photo_id], 2),
                    'raw_rate_per_minute': round(raw_rate, 2),
                    'total_accesses_in_window': total_accesses,
                    'window_start': window_start,
                    'window_end': window_end,
                    'window_duration_seconds': round(window_end - window_start, 2),
                    'event_count': len(events)
                })
            
            logger.info(f"Generated stats for {len(stats)} photos "
                       f"(tracked={self.photos_tracked}, recorded={self.total_events_recorded})")
            
            return stats
    
    def get_current_rate(self, photo_id: str) -> float:
        with self.lock:
            return self.ewma_rates.get(photo_id, 0.0)
    
    def get_tracker_stats(self) -> dict:
        """Get internal tracker statistics"""
        with self.lock:
            return {
                'photos_tracked': self.photos_tracked,
                'total_events_recorded': self.total_events_recorded,
                'total_events_pruned': self.total_events_pruned,
                'window_seconds': self.window_seconds,
                'alpha': self.alpha
            }



access_tracker = AccessTracker(
    window_seconds=ACCESS_WINDOW_SECONDS,
    alpha=EWMA_ALPHA
)

class Needle:
    MAGIC_NUMBER = 0xDEADBEEF
    HEADER_SIZE = 4 + 4 + 1 + 8 + 4
    
    def __init__(self, photo_id: str, photo_data: bytes, deleted: bool = False):
        self.photo_id = photo_id
        self.photo_data = photo_data
        self.deleted = deleted
        self.timestamp = int(time.time())
        self.size = len(photo_data)
    
    def serialize(self) -> bytes:
        photo_id_bytes = self.photo_id.encode('utf-8')
        photo_id_len = len(photo_id_bytes)
        flags = 1 if self.deleted else 0

        header = struct.pack(
            '>I I B Q I',
            self.MAGIC_NUMBER,
            photo_id_len,
            flags,
            self.timestamp,
            self.size
        )

        needle_bytes = header + photo_id_bytes + self.photo_data
        checksum_int = zlib.crc32(needle_bytes) & 0xFFFFFFFF
        checksum = struct.pack('>I', checksum_int)

        return needle_bytes + checksum
    
    @staticmethod
    def deserialize(data: bytes, offset: int = 0):
        header_data = data[offset:offset + Needle.HEADER_SIZE]
        magic, photo_id_len, flags, timestamp, photo_size = struct.unpack(
            '>I I B Q I', header_data
        )

        if magic != Needle.MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number at offset {offset}")

        photo_id_start = offset + Needle.HEADER_SIZE
        photo_id_bytes = data[photo_id_start:photo_id_start + photo_id_len]
        photo_id = photo_id_bytes.decode('utf-8')

        photo_data_start = photo_id_start + photo_id_len
        photo_data = data[photo_data_start:photo_data_start + photo_size]

        checksum_start = photo_data_start + photo_size
        stored_checksum_bytes = data[checksum_start:checksum_start + 4]

        needle_bytes = data[offset:checksum_start]
        calc_checksum_int = zlib.crc32(needle_bytes) & 0xFFFFFFFF
        calc_checksum_bytes = struct.pack('>I', calc_checksum_int)

        if stored_checksum_bytes != calc_checksum_bytes:
            raise ValueError(f"Checksum mismatch for photo {photo_id}")

        needle = Needle(photo_id, photo_data, deleted=(flags == 1))
        needle.timestamp = timestamp
        return needle, checksum_start + 4
    
    def total_size(self) -> int:
        return self.HEADER_SIZE + len(self.photo_id.encode('utf-8')) + self.size + 4


class Volume:
    
    def __init__(self, volume_id: str, volume_path: str, max_size: int):
        self.volume_id = volume_id
        self.volume_path = volume_path
        self.max_size = max_size
        self.current_size = 0
        self.created_at = int(time.time())
        self.index: Dict[str, dict] = {}
        self.lock = threading.RLock()
        
        if not os.path.exists(volume_path):
            Path(volume_path).touch()
            logger.info(f"Created new volume file: {volume_path}")
        else:
            self._rebuild_index()
    
    def _rebuild_index(self):
        logger.info(f"Rebuilding index for volume {self.volume_id}")
        
        with open(self.volume_path, 'rb') as f:
            file_data = f.read()
        
        offset = 0
        while offset < len(file_data):
            try:
                needle, next_offset = Needle.deserialize(file_data, offset)
                
                if needle.deleted:
                    if needle.photo_id in self.index:
                        self.index[needle.photo_id]['deleted'] = True
                else:
                    self.index[needle.photo_id] = {
                        'offset': offset,
                        'size': needle.size,
                        'deleted': False,
                        'timestamp': needle.timestamp
                    }
                
                self.current_size = next_offset
                offset = next_offset
            except Exception as e:
                logger.error(f"Error parsing needle at offset {offset}: {e}")
                break
        
        active_count = len([p for p in self.index.values() if not p['deleted']])
        logger.info(f"Rebuilt index: {active_count} active photos, {self.current_size} bytes")
    
    def write(self, photo_id: str, photo_data: bytes) -> dict:
        with self.lock:
            with compaction_lock:
                if compaction_in_progress:
                    raise Exception("Volume compaction in progress, try again")
            
            if photo_id in self.index and not self.index[photo_id]['deleted']:
                logger.info(f"Photo {photo_id[:16]}... already exists in volume {self.volume_id}")
                return {
                    'photo_id': photo_id,
                    'volume_id': self.volume_id,
                    'offset': self.index[photo_id]['offset'],
                    'size': self.index[photo_id]['size']
                }
            
            needle = Needle(photo_id, photo_data)
            needle_bytes = needle.serialize()
            
            if self.current_size + len(needle_bytes) > self.max_size:
                raise Exception(f"Volume {self.volume_id} is full")
            
            offset = self.current_size
            with open(self.volume_path, 'ab') as f:
                f.write(needle_bytes)
                f.flush()
                os.fsync(f.fileno())
            
            self.index[photo_id] = {
                'offset': offset,
                'size': needle.size,
                'deleted': False,
                'timestamp': needle.timestamp
            }
            self.current_size += len(needle_bytes)
            
            logger.info(f"Wrote photo {photo_id[:16]}... to {self.volume_id}")
            
            return {
                'photo_id': photo_id,
                'volume_id': self.volume_id,
                'offset': offset,
                'size': needle.size
            }
    
    def read(self, photo_id: str) -> Optional[bytes]:
        with self.lock:
            if photo_id not in self.index:
                return None
            
            info = self.index[photo_id]
            if info['deleted']:
                return None
            
            with open(self.volume_path, 'rb') as f:
                f.seek(info['offset'])
                needle_size = Needle.HEADER_SIZE + len(photo_id.encode('utf-8')) + info['size'] + 4
                needle_data = f.read(needle_size)
            
            try:
                needle, _ = Needle.deserialize(needle_data)
                return needle.photo_data
            except Exception as e:
                logger.error(f"Error reading photo {photo_id[:16]}...: {e}")
                return None
    
    def delete(self, photo_id: str) -> bool:
        with self.lock:
            if photo_id not in self.index:
                return False
            
            if self.index[photo_id]['deleted']:
                return True
            
            tombstone = Needle(photo_id, b'', deleted=True)
            tombstone_bytes = tombstone.serialize()
            
            if self.current_size + len(tombstone_bytes) > self.max_size:
                logger.warning(f"Volume full, cannot write tombstone for {photo_id[:16]}...")
                self.index[photo_id]['deleted'] = True
                return True
            
            offset = self.current_size
            with open(self.volume_path, 'ab') as f:
                f.write(tombstone_bytes)
                f.flush()
                os.fsync(f.fileno())
            
            self.current_size += len(tombstone_bytes)
            self.index[photo_id]['deleted'] = True
            
            logger.info(f"Wrote tombstone for {photo_id[:16]}...")
            return True
    
    def get_efficiency(self) -> float:
        if self.current_size == 0:
            return 1.0
        
        valid_bytes = sum(
            info['size'] for info in self.index.values() 
            if not info['deleted']
        )
        return valid_bytes / self.current_size
    
    def compact(self) -> 'Volume':
    
        logger.info(f"Starting compaction for volume {self.volume_id}")
        
        new_volume_id = f"{self.volume_id}-new"
        new_volume_path = f"{self.volume_path}.new"
        new_volume = Volume(new_volume_id, new_volume_path, self.max_size)
        
      
        global compaction_in_progress
        with compaction_lock:
            compaction_in_progress = True
        
        try:
            with self.lock:
                for photo_id, info in self.index.items():
                    if not info['deleted']:
                        photo_data = self.read(photo_id)
                        if photo_data:
                            new_volume.write(photo_id, photo_data)
            
            logger.info(
                f"Compaction complete. Old: {self.current_size} bytes, "
                f"New: {new_volume.current_size} bytes, "
                f"Saved: {self.current_size - new_volume.current_size} bytes"
            )
            
            return new_volume
        finally:
            
            with compaction_lock:
                compaction_in_progress = False
    
    def get_all_photo_ids(self) -> List[str]:
        with self.lock:
            return [
                photo_id for photo_id, info in self.index.items()
                if not info['deleted']
            ]
    
    def to_dict(self) -> dict:
        
        return {
            'volume_id': self.volume_id,
            'volume_path': self.volume_path,
            'max_size': self.max_size,
            'current_size': self.current_size,
            'created_at': self.created_at,
            'photo_count': len([p for p in self.index.values() if not p['deleted']]),
            'efficiency': self.get_efficiency()
        }


def get_or_create_volume() -> Volume:
    with volume_lock:
        with compaction_lock:
            if compaction_in_progress:
                raise HTTPException(status_code=503, detail="Compaction in progress")
        
        for volume in volumes.values():
            if volume.current_size < volume.max_size * 0.9:
                return volume
        
        volume_id = f"volume-{len(volumes) + 1}"
        volume_path = os.path.join(VOLUME_DIRECTORY, f"{volume_id}.dat")
        volume = Volume(volume_id, volume_path, MAX_VOLUME_SIZE)
        volumes[volume_id] = volume
        logger.info(f"Created new volume: {volume_id}")
        return volume


def initialize_volumes():
    
    os.makedirs(VOLUME_DIRECTORY, exist_ok=True)
    
    for filename in os.listdir(VOLUME_DIRECTORY):
        if filename.endswith('.dat') and not filename.endswith('.new'):
            volume_id = filename[:-4]
            volume_path = os.path.join(VOLUME_DIRECTORY, filename)
            volume = Volume(volume_id, volume_path, MAX_VOLUME_SIZE)
            volumes[volume_id] = volume
            logger.info(f"Loaded volume: {volume_id}")
    
    if not volumes:
        get_or_create_volume()


def push_to_cache(photo_id: str, photo_data: bytes):
   
    if not redis_client:
        return
    
    try:
        redis_client.setex(f"photo:{photo_id}", 3600, photo_data)
        logger.debug(f"Pushed {photo_id[:16]}... to cache")
    except Exception as e:
        logger.warning(f"Failed to push to cache: {e}")



def get_leader_url() -> str:
    
    try:
        response = requests.get(f"{DIRECTORY_SERVICE_URL}/stats", timeout=5)
        if response.status_code == 200:
            stats = response.json()
            leader_url = stats.get('current_leader_url')
            if leader_url:
                return leader_url
        return DIRECTORY_SERVICE_URL
    except:
        return DIRECTORY_SERVICE_URL




@app.post("/write")
@limiter.limit("100/minute")
async def write_photo(
    request: Request,
    background_tasks: BackgroundTasks,
    photo: UploadFile = File(...),
    photo_id: Optional[str] = Form(None)
):
   
    request_id = str(uuid.uuid4())
    
    try:
        photo_data = await photo.read()
        
        if not photo_id:
            photo_id = hashlib.sha256(photo_data).hexdigest()
        
        logger.info(f"[{request_id}] Writing photo {photo_id[:16]}... ({len(photo_data)} bytes)")
        
        volume = get_or_create_volume()
        result = volume.write(photo_id, photo_data)
        result['store_id'] = STORE_ID
        result['request_id'] = request_id
        
        background_tasks.add_task(push_to_cache, photo_id, photo_data)
        
        logger.info(f"[{request_id}]   Wrote photo {photo_id[:16]}...")
        
        return result
        
    except Exception as e:
        logger.error(f"[{request_id}] ✗ Error writing photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/read/{photo_id}")
@limiter.limit("1000/minute")
async def read_photo(photo_id: str, request: Request, background_tasks: BackgroundTasks):
    request_id = str(uuid.uuid4())
    
    try:
        photo_data = None
        for volume in volumes.values():
            photo_data = volume.read(photo_id)
            if photo_data:
                break
        
        if not photo_data:
            logger.debug(f"[{request_id}] Photo {photo_id[:16]}... not found")
            raise HTTPException(status_code=404, detail="Photo not found")
        
        background_tasks.add_task(track_access, photo_id)
        
        return Response(content=photo_data, media_type="image/jpeg")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error reading photo {photo_id[:16]}...: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete/{photo_id}")
@limiter.limit("50/minute")
async def delete_photo(photo_id: str, request: Request):
    request_id = str(uuid.uuid4())
    
    try:
        deleted = False
        for volume in volumes.values():
            if volume.delete(photo_id):
                deleted = True
                break
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Photo not found")
        
        if redis_client:
            try:
                redis_client.delete(f"photo:{photo_id}")
            except:
                pass
        
        logger.info(f"[{request_id}]   Deleted {photo_id[:16]}...")
        
        return {"deleted": True, "photo_id": photo_id, "request_id": request_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error deleting photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/replicate")
async def replicate_photo(request: dict):
    request_id = request.get('request_id', str(uuid.uuid4()))
    
    try:
        photo_id = request['photo_id']
        source_store_url = request['source_store_url']
        
        logger.info(f"[{request_id}] Replicating photo {photo_id[:16]}... from {source_store_url}")
        
        response = requests.get(f"{source_store_url}/read/{photo_id}", timeout=30)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch from source: {response.status_code}")
        
        photo_data = response.content
        
        volume = get_or_create_volume()
        result = volume.write(photo_id, photo_data)
        result['store_id'] = STORE_ID
        result['request_id'] = request_id
        
        # Push to cache
        threading.Thread(
            target=push_to_cache,
            args=(photo_id, photo_data),
            daemon=True
        ).start()
        
        logger.info(f"[{request_id}]   Replicated photo {photo_id[:16]}...")
        
        return result
        
    except Exception as e:
        logger.error(f"[{request_id}] Error replicating photo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    total_capacity = MAX_VOLUME_SIZE * 10
    used_capacity = sum(v.current_size for v in volumes.values())
    
    return {
        "store_id": STORE_ID,
        "total_capacity": total_capacity,
        "used_capacity": used_capacity,
        "available_capacity": total_capacity - used_capacity,
        "volumes": [v.to_dict() for v in volumes.values()],
        "access_tracker": access_tracker.get_tracker_stats(),
        "registered": registration_successful
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "store_id": STORE_ID,
        "volumes": len(volumes),
        "registered": registration_successful
    }


def track_access(photo_id: str):
    access_tracker.record_access(photo_id)


def report_stats_worker():
    logger.info(f"Stats reporting worker started (interval={STATS_REPORT_INTERVAL}s)")
    
    while True:
        try:
            time.sleep(STATS_REPORT_INTERVAL)
            
            stats = access_tracker.get_stats_for_reporting()
            
            if not stats:
                logger.debug("No active photos to report")
                continue
            
            report = {
                'store_id': STORE_ID,
                'timestamp': time.time(),
                'access_stats': stats,
                'tracker_info': access_tracker.get_tracker_stats()
            }
            
            try:
                response = requests.post(
                    f"{REPLICATION_MANAGER_URL}/stats/report",
                    json=report,
                    timeout=5
                )
                
                if response.status_code == 200:
                    logger.info(f"Reported stats for {len(stats)} photos")
                else:
                    logger.warning(f"Stats report failed: HTTP {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to send stats: {e}")
        
        except Exception as e:
            logger.error(f"Stats reporting error: {e}", exc_info=True)
            time.sleep(10)


def heartbeat_worker():
    global registration_successful
    time.sleep(15)
    
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            
            if not registration_successful:
                logger.debug("Skipping heartbeat - not registered yet")
                continue
            
            total_capacity = MAX_VOLUME_SIZE * 10
            used_capacity = sum(v.current_size for v in volumes.values())
            
            heartbeat_data = {
                "store_id": STORE_ID,
                "store_url": STORE_URL,
                "total_capacity": total_capacity,
                "available_capacity": total_capacity - used_capacity,
                "volumes": [v.volume_id for v in volumes.values()]
            }
            
            response = requests.post(
                f"{DIRECTORY_SERVICE_URL}/stores/heartbeat",
                json=heartbeat_data,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.debug("♥ Heartbeat sent successfully")
            elif response.status_code == 404:
                logger.warning("⚠ Directory doesn't recognize store, re-registering...")
                registration_successful = False
                threading.Thread(target=register_with_directory_worker, daemon=True).start()
                
        except requests.exceptions.ConnectionError:
            logger.warning("Directory service unavailable for heartbeat")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")


def compaction_worker():
    while True:
        try:
            time.sleep(3600)
            
            with volume_lock:
                for volume_id, volume in list(volumes.items()):
                    efficiency = volume.get_efficiency()
                    
                    if efficiency < COMPACTION_THRESHOLD:
                        logger.info(f"Volume {volume_id} efficiency {efficiency:.2%} below threshold, starting compaction")
                        
                        try:
                            new_volume = volume.compact()
                            
                            old_path = volume.volume_path
                            new_path = new_volume.volume_path
                            backup_path = f"{old_path}.old"
                            
                            os.rename(old_path, backup_path)
                            os.rename(new_path, old_path)
                            
                            new_volume.volume_id = volume_id
                            new_volume.volume_path = old_path
                            volumes[volume_id] = new_volume
                            
                            os.remove(backup_path)
                            
                            logger.info(f"  Compaction complete for {volume_id}")
                            
                        except Exception as e:
                            logger.error(f"Error compacting volume {volume_id}: {e}")
                            
        except Exception as e:
            logger.error(f"Error in compaction worker: {e}")


def garbage_collection_worker():
    time.sleep(300)
    
    while True:
        try:
            time.sleep(3600 * 6)
            
            logger.info("Starting garbage collection scan")
            
            local_photos = set()
            for volume in volumes.values():
                local_photos.update(volume.get_all_photo_ids())
            
            if not local_photos:
                continue
            
            logger.info(f"GC: Checking {len(local_photos)} photos")
            
            
            leader_url = get_leader_url()
            
            orphaned = []
            batch_size = 100
            photo_list = list(local_photos)
            
            for i in range(0, len(photo_list), batch_size):
                batch = photo_list[i:i+batch_size]
                
                try:
                    response = requests.post(
                        f"{leader_url}/verify_photos",
                        json={'photo_ids': batch, 'store_id': STORE_ID},
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        
    
                        registered = set(result.get('registered_photo_ids', []))
                        batch_set = set(batch)
                        not_found = batch_set - registered
                        orphaned.extend(not_found)
                        
                except Exception as e:
                    logger.error(f"GC verification failed: {e}")
            
           
            deleted_count = 0
            for photo_id in orphaned:
                try:
                    for volume in volumes.values():
                        if volume.delete(photo_id):
                            deleted_count += 1
                            logger.info(f"GC: Deleted orphaned photo {photo_id[:16]}...")
                            break
                except Exception as e:
                    logger.error(f"GC: Error deleting {photo_id[:16]}...: {e}")
            
            logger.info(f"  GC complete: cleaned {deleted_count} orphaned photos")
            
        except Exception as e:
            logger.error(f"Error in garbage collection: {e}")


def register_with_directory_worker():

    global registration_successful
    retry_delay = 5
    attempt = 0
    
    while not registration_successful:
        try:
            attempt += 1
            
            total_capacity = MAX_VOLUME_SIZE * 10
            used_capacity = sum(v.current_size for v in volumes.values())
            volume_list = [v.volume_id for v in volumes.values()]
            
            register_data = {
                "store_id": STORE_ID,
                "store_url": STORE_URL,
                "total_capacity": total_capacity,
                "available_capacity": total_capacity - used_capacity,
                "volumes": volume_list
            }
            
            response = requests.post(
                f"{DIRECTORY_SERVICE_URL}/stores/register",
                json=register_data,
                timeout=10
            )
            
            if response.status_code == 200:
                registration_successful = True
                logger.info(f"  Successfully registered with Directory Service (attempt {attempt})")
                return
            else:
                logger.warning(f"Registration attempt {attempt} returned {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            logger.warning(f"Registration attempt {attempt}: Directory not ready")
        except Exception as e:
            logger.error(f"Registration attempt {attempt} error: {e}")
        
        
        time.sleep(min(retry_delay, 60))
        retry_delay = min(retry_delay * 1.5, 60)


@app.get("/debug/access_stats")
async def get_access_stats(photo_id: Optional[str] = None):
    
    if photo_id:
        with access_tracker.lock:
            if photo_id not in access_tracker.events:
                raise HTTPException(status_code=404, detail="Photo not tracked")
            
            events = list(access_tracker.events[photo_id])
            return {
                'photo_id': photo_id,
                'ewma_rate_per_minute': access_tracker.ewma_rates.get(photo_id, 0.0),
                'last_update': access_tracker.last_update.get(photo_id, 0.0),
                'events': [{'timestamp': e.timestamp, 'count': e.count} for e in events],
                'event_count': len(events)
            }
    else:
        stats = access_tracker.get_stats_for_reporting()
        tracker_stats = access_tracker.get_tracker_stats()
        
        return {
            'tracker_stats': tracker_stats,
            'photos': stats
        }

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 80)
    logger.info(f"Starting Store Service: {STORE_ID}")
    logger.info("=" * 80)
    
    initialize_volumes()
    
    threading.Thread(target=register_with_directory_worker, daemon=True, name="Registration").start()
    threading.Thread(target=report_stats_worker, daemon=True, name="StatsReporter").start()
    threading.Thread(target=heartbeat_worker, daemon=True, name="Heartbeat").start()
    threading.Thread(target=compaction_worker, daemon=True, name="Compaction").start()
    threading.Thread(target=garbage_collection_worker, daemon=True, name="GarbageCollection").start()
    
    logger.info("  Store Service started successfully")
    logger.info("=" * 80)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)