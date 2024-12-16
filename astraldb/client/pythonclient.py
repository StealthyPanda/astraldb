
import asyncio
from typing import Iterable

import numpy as np
from ..astral import Store
from ..utils import logger


class StoreClient:
    """Creates a client to interact with a store.
    
        NOTE: Always use in a 'with' block.
    """
    def __init__(self, store_name : str) -> None:
        self.store_name = store_name
        self.store = Store(self.store_name)
        self.store._client_mode = True
        
    
    def get(self, key : str) -> np.ndarray:
        """Retrieves a single vector from the store.

        Args:
            key (str): key of the vector to retrieve.

        Returns:
            np.ndarray: vector as a numpy array.
        """
        return self.store.get(key)

    
    def set(self, key : str, vector : list[int | float]) -> str:
        """Sets the value of a single vector in the store.

        Args:
            key (str): key of the vector to set.
            vector (list[int | float]): vector to set.

        Returns:
            str: hash of the key in the store.
        """
        keyhash, _ = self.store.set(key, vector)
        return keyhash
    
    def delete(self, key : str) -> None:
        """Deletes a key-vector pair in the store.
        If the key doesn't exist, it does nothing.

        Args:
            key (str): key of the vector to delete.
        """
        self.store.delete(key)
    
    @property
    def keys(self):
        """All the keys in this store.

        Returns:
            set[str]: all the keys as a set.
        """
        return self.store.keys
    
    
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
