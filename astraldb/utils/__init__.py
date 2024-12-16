
from loguru import logger
import sys
from numpy import float64


astral_dir = '.astral'


max_shard_size = 2 ** 20 # bytes
max_update_rate = 1/2 # Hz

logger.remove()
# logger.add(sys.stdout, level="INFO")
logger.add(
    sink=sys.stdout,  # Output logs to the console
    # format="🚀✨ [<green>{time:YYYY-MM-DD HH:mm:ss}</green>][<level>{level: <8}</level>] <cyan>{message}</cyan>",
    format="🚀✨ [<green>{time:YYYY-MM-DD HH:mm:ss}</green>] <level>{message}</level>",
    level="INFO",  # Set the minimum log level
)


default_dtype = float64
default_port = 4269

