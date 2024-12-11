# 🚀✨AstralDB


A fast and lightweight vector database.

## Quickstart

This database is completely written in python, for python.

Install using:

```bash
pip install git+https://github.com/StealthyPanda/astraldb
```

That's it! Try it out now:
```bash
mkdir astraltest
cd astraltest
astral # or python -m astral
```

Everything related to the database is local to this folder, saved in `.astral`. 

Vectors are organised simply as key-value pairs, and stored in binary format. You can interact with the database either through the client in python, or via `http://localhost:4269`.


## Endpoints


- GET `/stores `  -> returns all stores in this 🚀✨AstralDB.

---

- POST `/get {store:str, key:str}`  -> returns value of `key` in `store`.
- POST `/set {store:str, key:str, value:list}`  -> sets `value` of `key` in `store`.
- POST `/delete {store:str, key:str}`  -> deletes key-value pair in `store`.

---

- POST `/create_store {name:str}`  -> create a new store.
- POST `/delete_store {name:str}`  -> deletes the store.
- POST `/get_keys {name:str}`  -> get all the keys in the store.

And that's it! Lightweight, easy to use, and *super-fast*.