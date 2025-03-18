# Description: This file contains the rpc procedures that are used to communicate with other nodes in the network.
from typing import Optional, List

import aiomas
# from loguru import logger

from chord.helpers import gen_finger
import logging
logger = logging.getLogger(__name__)

######################
# RPC Procedures
######################


async def rpc_ask_for_succ(
    next_node: dict, numeric_id: int
) -> (bool, Optional[dict]):
    """
    Find the successor.
      Args:
          next_node (dict): The next node.
          numeric_id (int): The numeric id of the node.
          ssl_ctx (ssl.SSLContext): Used for tls.
      Returns:
          found (Boolean): Whether or not a successor exist.
          successor (dict): The successor node if it exists.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        found, rep = await rpc_con.remote.find_successor(numeric_id)
        await rpc_con.close()
        return found, rep
    except Exception as e:
        #logger.error(e, next_node, numeric_id)
        return False, None


async def rpc_ask_for_pred_and_succlist(addr: str) -> (dict, List):
    """
    Gets the predecessor and successor list of the current node.
      Args:
          ssl_ctx (ssl.SSLContext): Used for tls.
      Returns
          (dict): predecessor
          (array): The list of successors
    """
    host, port = addr.split(":")
    try:
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_pred_and_succlist()
        await rpc_con.close()
        return rep
    except Exception as e:
        #logger.error(e)
        return None, None


async def rpc_ping(addr: str) -> bool:
    """
    Pings a node with a given address.
      Args:
          addr: The address of target node to ping.
      Returns
          (dict): predecessor
          (array): The list of successors
    """
    try:
        host, port = addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.ping()
        await rpc_con.close()
        return rep == "pong"
    except Exception as e:
        #logger.error(e)
        return False


async def rpc_notify(succ_addr: str, my_addr: str, ring_sz: int, keysize: int) -> None:
    """
    Notifies nodes that the calling node  is now their predecessor.
    Args:
        succ_addr (string): The address of the successor node.
        my_addr (string): Address of the calling node.
#    """
    try:
        host, port = succ_addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        await rpc_con.remote.notify(gen_finger(my_addr, ring_sz=ring_sz, keysize=keysize))
        await rpc_con.close()
    except Exception as e:
        print(e)
        pass

async def rpc_get_job(next_node: dict, job_id: str, ttl: int) -> Optional[str]:
    """
    checks current node for the value or deligates to appropriate succsorsself.
    Returns the value if it is stored on the ring.
    Args:
        next_node (dict): the next node.
        key (string): The key under which a vlue shall be stored.
        value (string): The value / data being stored.
        ttl (int): Time to live for the message, after that the message is discared and no value is returned for that key.
        is_replica (Boolean): Whether or not the current node is a replica.
        ssl_ctx (ssl.SSLContext): Used for tls.
    Returns:
        Boolean: Whether or not the value is found
        String: Value if one is found.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_job(job_id, ttl)
        await rpc_con.close()
        return rep
    except Exception as e:
        #logger.error(e)
        return None


async def rpc_get_key(
    next_node: dict, key: str, ttl: int, is_replica: bool
) -> Optional[str]:
    """
    checks current node for the value or deligates to appropriate succsorsself.
    Returns the value if it is stored on the ring.
    Args:
        next_node (dict): the next node.
        key (string): The key under which a vlue shall be stored.
        value (string): The value / data being stored.
        ttl (int): Time to live for the message, after that the message is discared and no value is returned for that key.
        is_replica (Boolean): Whether or not the current node is a replica.
        ssl_ctx (ssl.SSLContext): Used for tls.
    Returns:
        Boolean: Whether or not the value is found
        String: Value if one is found.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.find_key(key, ttl, is_replica=is_replica)
        #print("response from node =>", rep)
        await rpc_con.close()
        return rep
    except Exception as e:
        #logger.error(e)
        return None


async def rpc_save_key(
    next_node: dict, key: str, value: str, ttl: int
) -> Optional[str]:
    """
    Stores key, val pair in the actual storage.
    Args:
        next_node (dict): The next node.
        key (string): The key under which a vlue shall be stored.
        value (string): The value / data being stored.
        ttl (int): time to live. How long this should remain in the network.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.save_key(key, value, ttl)
        await rpc_con.close()
        return rep
    except Exception as e:
        print(e)
        return None


async def rpc_put_key(next_node: dict, key: str, value: str) -> Optional[str]:
    """
    Generates multiple dht keys for each value for replication.
    Finds the node based on the key, where the value should be stored.
    Save it using save_key after a detination node is chosen.
    Args:
        next_node (dict): The next node.
        key (string): The key under which a vlue shall be stored.
        value (string): The value / data being stored.
        ttl (int): time to live. How long this should remain in the network.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.put_key(key, value)
        await rpc_con.close()
        return rep
    except Exception as e:
        logger.error(e)
        return None


async def rpc_get_all_keys(next_node: dict, node_id: int):
    """
    Gets all key, value pairs of the node with the given node_id
    Args:
        node_id (int): The id of the node.
    Returns:
        next_node (dict): The next node.
        keys (list): The keys on the node as a list of strings
        values (list): Valus stored on the node as a list of string.
        ssl_ctx (ssl.SSLContext): Used for tls.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_all(node_id)
        await rpc_con.close()
        return rep
    except Exception as e:
        #logger.error(e)
        return None
