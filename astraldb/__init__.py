
"""
# 🚀✨AstralDB

Yet another vector database.

[Quickstart guide](https://stealthypanda.github.io/astralwebpage/).

Python client example:
```python
from astraldb import StoreClient
with StoreClient('store-name') as store:
    store.set('x', [1, 2, 3])
    store.set('y', [4, 5, 6])
    print(store.get('x')) # [1, 2, 3]
    print(store.keys) # {'x', 'y'}
```

"""



from .server import *
from .astral import *
from .client import *

