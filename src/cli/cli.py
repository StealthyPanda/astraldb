
import os
import sys, json
from .splash import splash
from astraldb import start_server
from utils import logger, default_port, astral_dir



def parse_argv_flags() -> dict[str, str]:
    flags = dict()
    for i, each in enumerate(sys.argv):
        if each[:2] == '--':
            flags[each[2:]] = None
            if i < (len(sys.argv) - 1):
                if sys.argv[i + 1][:2] != '--':
                    flags[each[2:]] = sys.argv[i + 1]
    return flags




def main():
    print(splash)
    flags = parse_argv_flags()
    
    port = default_port
    if 'port' in flags: port = int(flags['port'])
    
    location = os.path.join(os.getcwd(), astral_dir)
    logger.warning(
        f'Starting 🚀✨AstralDB server @ http://localhost:{port} in {location}'
    )
    
    start_server({
        'port' : port,
        'debug' : 'debug' in flags
    })
