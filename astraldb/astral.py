
import hashlib, uuid
import numpy as np
import os, json, time
import asyncio

from .utils import *


class Store:
    pass


def get_hash(val : str) -> str:
    return hashlib.sha256(bytes(val, 'UTF-8')).hexdigest()

def get_hash_bytes(b : bytes) -> bytes:
    return hashlib.sha256(b).hexdigest().encode()


class Shard:
    
    def __init__(
            self, 
            name : str, 
            store : Store,
            max_size : int = max_shard_size, 
            max_rate : int = max_update_rate,
        ):
        self.name = name
        self.store = store
        self.id = str(uuid.uuid4())
        self.max_size = max_size
        self.max_rate = max_rate
        
        self._data : dict[str, np.ndarray] = dict()
        self.keys : set[str] = set()
        
        logger.info(f'Created shard `{self.name}`')
        if os.path.exists(self.disk_location):
            self._read_from_disk()
        
        self.last_write : int = time.time()
        self.another_write_in_progress : bool = False
        
        self.save_task = None
        
        

    @property
    def disk_location(self) -> str:
        return os.path.join(self.store.disk_location, f'{self.name}.shard')


    @property
    def keys_location(self) -> str:
        return os.path.join(self.store.disk_location, f'{self.name}.shard.keys')
    
    
    def __str__(self):
        return (
            f'Shard(name={self.name}, id={self.id}, '
                f'max_size={self.max_size}, max_rate={self.max_rate}, '
                f'location={self.disk_location}'
            ')'
        )
    
    
    def _get_data_bytes(self) -> bytes:
        allbytes = []
        for key in self._data:
            allbytes += list(bytes((key), 'UTF-8'))
            allbytes += list(self._data[key].nbytes.to_bytes(16, signed=False))
            allbytes += list(self._data[key].tobytes())
        return bytes(allbytes)
    
    
    
    
    def _parse_and_store_bytes(self, allbytes : bytes):
        allbytes = list(allbytes)
        
        i = 0
        while i < len(allbytes):
            key = bytes.decode(bytes(allbytes[i : i + 64]))
            i = i + 64
            size = int.from_bytes(bytes(allbytes[i : i + 16]), signed=False)
            i = i + 16
            data_bytes = np.frombuffer(bytes(allbytes[i : i + size]), dtype=default_dtype)
            self._data[key] = data_bytes
            i = i + size
        
    
    def _read_from_disk(self):
        logger.info(f'Shard[ {self.store.name}/{self.name} ]: Reading from disk...')
        with open(self.disk_location, 'rb') as file:
            allbytes = file.read()
        self._parse_and_store_bytes(allbytes)
        with open(self.keys_location, 'r') as file:
            obj = json.load(file)
            self.keys = set(obj['keys'])
            self.id = obj['id']
    
    
    
    
    async def _save_to_disk_internal(self):
        if self.another_write_in_progress: return
        
        curr = time.time()
        if (curr - self.last_write) < (1 / self.max_rate):
            self.another_write_in_progress = True
            await asyncio.sleep(int((1 / self.max_rate) - (curr - self.last_write)))
            self.another_write_in_progress = False
        
        logger.info(f'Shard[ {self.store.name}/{self.name} ]: Saving to disk...')
        with open(self.disk_location, 'wb') as file:
            file.write(self._get_data_bytes())
        with open(self.keys_location, 'w') as file:
            json.dump({
                'keys' : list(self.keys),
                'id' : self.id
            }, file)
        
        self.last_write += (1 / self.max_rate)
        self.save_task = None
    
    def _save_to_disk(self):
        if self.save_task is None:
            self.save_task = asyncio.create_task(self._save_to_disk_internal())
        
    
    
    def get(self, key : str) -> np.ndarray:
        return self._data[get_hash(key)]
    
    def set(self, key : str, value : np.ndarray):
        value = np.array(value, dtype=default_dtype)
        self._data[get_hash(key)] = value
        self.keys.add(key)
        self._save_to_disk()
        return get_hash(key)
    
    def delete(self, key : str):
        ghk = get_hash(key)
        if ghk in self._data:
            del self._data[ghk]
            self._save_to_disk()
        return self
        
    



class Store:
    
    def __init__(self, name : str):
        self.name = name
        self.id = str(uuid.uuid4())
        
        os.makedirs(self.disk_location, exist_ok=True)
        if not os.path.exists(os.path.join(self.disk_location, '.storeinfo')):
            self._save_store_info()
        
        logger.info(f'Created store `{self.name}`')
        
        self.shards : list[Shard] = []
        self._load_store()
        
        if len(self.shards) == 0:
            self._add_shard()
        
        self.mapping : dict[str, Shard] = dict()
        
    
    def _add_shard(self):
        self.shards.append(Shard(
            name = f'shard{len(self.shards)}',
            store= self,
        ))
    
    
    def __str__(self):
        return f'Store(name={self.name}, id={self.id}, shards={(self.shards)})'
    
    @property
    def disk_location(self) -> str:
        return os.path.join(astral_dir, 'stores', self.name)
    
    def _load_store(self):
        with open(os.path.join(self.disk_location, '.storeinfo'), 'r') as file:
            obj = json.load(file)
        self.id = obj['id']
        
        shards = os.listdir(self.disk_location)
        shards = list(map(
            lambda x: os.path.basename(x),
            shards
        ))
        shards = list(filter(
            lambda x: x.split('.')[-1] == 'shard',
            shards
        ))
        shards = list(map(
            lambda x: x.split('.')[0],
            shards
        ))
        
        self.shards = list(map(
            lambda x: Shard(x, self),
            shards
        ))
    
    def _save_store_info(self):
        with open(os.path.join(self.disk_location, '.storeinfo'), 'w') as file:
            json.dump({
                'id' : self.id,
                'name' : self.name
            }, file)


    def get(self, key : str) -> np.ndarray:
        return self.shards[0].get(key)
    
    def set(self, key : str, value : np.ndarray):
        key_hash = self.shards[0].set(key, value)
        return key_hash, self.shards[0]
    
    def delete(self, key : str):
        self.shards[0].delete(key)
        return self
        

