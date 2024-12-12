
import shutil
import numpy as np
import uvicorn, sys, os

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from pydantic import BaseModel

from .utils import astral_dir, default_port, logger
from .astral import Store


server = FastAPI()
port = default_port
debug = False

curr = os.getcwd()
store_dir = os.path.join(curr, astral_dir, 'stores')

def get_stores() -> list[str]:
    return list(map(
        lambda x: os.path.basename(x),
        os.listdir(store_dir)
    ))

stores : dict[str, Store] = dict()

def update_current_stores():
    global stores
    stores = { name : Store(name) for name in get_stores() }


@server.get('/ping')
async def pinger():
    return 'Astral server up and running!'

@server.get('/stores')
async def store_getter():
    return get_stores()


class SetBody(BaseModel):
    store : str
    key : str | list[str]
    value : list[int | float] | list[list[int | float]]


@server.post('/set')
async def set_val(data: SetBody):
    store_name = data.store
    key = data.key
    value = data.value
    
    if store_name not in stores:
        return JSONResponse(content={
            'message' : f'Store `{store_name}` doesn\'t exist!'
        }, status_code=404)
    
    
    if type(key) != list:
        try:
            kh, shard = stores[store_name].set(key, value)
            return {
                'message' : f'Successfully changed `{key}` in DB!',
                'key_hash' : kh, 'shard_id' : shard.id
            }
        except Exception as e:
            return JSONResponse(content={
                'message' : f'Couldn\'t add value to DB!',
                'error' : e
            }, status_code=500)
    else:
        khs, ids = dict(), dict()
        for k, v in zip(key, value):
            try:
                kh, shard = stores[store_name].set(k, v)
                khs[k] = kh
                ids[k] = shard.id
            except Exception as e:
                return JSONResponse(content={
                    'message' : f'Couldn\'t add value of `{k}` to DB!',
                    'error' : e
                }, status_code=500)
        return {
            'message' : f'Successfully changed keys in DB!',
            'key_hashes' : khs, 'shard_ids' : ids
        }


class GetBody(BaseModel):
    store : str
    key : str

@server.post('/get')
async def get_val(data: GetBody):
    store_name = data.store
    key = data.key
    
    if store_name not in stores:
        return JSONResponse(content={
            'message' : f'Store `{store_name}` doesn\'t exist!'
        }, status_code=404)
    
    try:
        value = stores[store_name].get(key).tolist()
        return { 'value' : value, }
    except Exception as e:
        return JSONResponse(content={
            'message' : f'Couldn\'t get value from DB!',
            'error' : e
        }, status_code=500)


@server.post('/delete')
async def del_val(data: GetBody):
    store_name = data.store
    key = data.key
    
    if store_name not in stores:
        return JSONResponse(content={
            'message' : f'Store `{store_name}` doesn\'t exist!'
        }, status_code=404)
    
    try:
        stores[store_name].delete(key)
        return { 'message' : f'Deleted `{key}` successfully!', }
    except Exception as e:
        return JSONResponse(content={
            'message' : f'Couldn\'t delete value from DB!',
            'error' : e
        }, status_code=500)



class StoreCreateBody(BaseModel):
    name : str

@server.post('/create_store')
async def create_store(data : StoreCreateBody):
    name = data.name
    
    logger.warning(f'Making new store `{name}`...')
    
    if name in stores:
        logger.error(f'Cannot create store `{name}`; already exists!')
        return JSONResponse(content={
            'message' : f'Cannot create store `{name}`; already exists!'
        }, status_code=400)
    
    newstore = Store(name)
    
    stores[name] = newstore
    
    return {
        'message' : f'Created new store `{name}` successfully!',
        'id' : newstore.id
    }

@server.post('/delete_store')
async def delete_store(data : StoreCreateBody):
    name = data.name
    
    if name not in stores:
        return JSONResponse(content={
            'message' : f'Store `{name}` doesn\'t exist!'
        }, status_code=400)
    
    logger.critical(f'Attempting to delete store `{name}`!')
    shutil.rmtree(stores[name].disk_location)
    del stores[name]
    logger.critical(f'Deleted store `{name}`!')
    
    
    return { 'message' : f'Deleted `{name}` successfully!' }




@server.post('/get_keys')
async def get_keys(data : StoreCreateBody):
    name = data.name
    if name not in stores:
        return JSONResponse(content={
            'message' : f'Store `{name}` doesn\'t exist!'
        }, status_code=400)
    
    shards = stores[name].shards
    
    keys = sum(list(map(lambda x: list(x.keys), shards)), start=[])
    
    return { 'keys' : keys }
    






def start_server(flags : dict[str, str]):
    global port, debug
    if 'port' in flags: port = int(flags['port'])
    if 'debug' in flags: debug = int(flags['debug'])
    
    dl = 25
    logger.info(('-' * dl) + 'Reading existing stores' + ('-' * dl))
    update_current_stores()
    logger.info(('-' * dl) + 'Reading stores complete!' + ('-' * dl))
    
    
    uvicorn.run(server, port = port, log_level='info' if debug else 'critical')

