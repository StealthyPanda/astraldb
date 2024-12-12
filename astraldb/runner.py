import uvicorn

def run_server():
    uvicorn.run("server:server", host="127.0.0.1", port=4269, reload=True)
