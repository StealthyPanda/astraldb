
import asyncio
from ..astral import Store
from ..utils import logger


class StoreClient:
    
    def __init__(self, store_name : str) -> None:
        self.store_name = store_name
        self.store = Store(self.store_name)
        self.store._client_mode = True
        
        self.get = self.store.get
        self.set = self.store.set
        self.delete = self.store.delete
        self.keys = self.store.keys
    
    
    def __enter__(self):
        return self
    
    
    def __exit__(self, exc_type, exc_value, traceback):
        
        if exc_type:
            print(f"{exc_value}")
            return False
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        
        try:
            # print(f'Current number of tasks : {len(self.store.write_tasks)}')
            loop.run_until_complete(asyncio.gather(*self.store.write_tasks))
        except Exception as e:
            print(e)
            logger.critical(f'Store[ {self.store_name} ] Failed to write some or all updates to disk!')
        else:
            pass
            # print('Finished all write tasks')
        finally:
            loop.close()
