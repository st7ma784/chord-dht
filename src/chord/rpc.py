"""
RPC Module
==========

This module contains the Remote Procedure Call (RPC) functions used for communication between nodes in the 
Chord Distributed Hash Table (DHT) system. These functions enable inter-node communication for tasks such as 
finding successors, retrieving and storing keys, managing node relationships, and handling distributed jobs.

Key Features
------------

- **Successor and Predecessor Management**:
  - `rpc_ask_for_succ`: Finds the successor for a given numeric ID.
  - `rpc_ask_for_pred_and_succlist`: Retrieves the predecessor and successor list of a node.

- **Node Communication**:
  - `rpc_ping`: Pings a node to check its availability.
  - `rpc_notify`: Notifies a node that the calling node is now its predecessor.

- **Key-Value Operations**:
  - `rpc_get_key`: Retrieves a value associated with a key from the DHT.
  - `rpc_save_key`: Stores a key-value pair in the DHT.
  - `rpc_put_key`: Replicates and stores a key-value pair in the DHT.
  - `rpc_get_all_keys`: Retrieves all key-value pairs stored on a specific node.

- **Job Management**:
  - `rpc_find_job`: Finds and retrieves job data from the DHT.

Dependencies
------------

- **aiomas**:
  Provides the RPC framework for asynchronous communication between nodes.

- **chord.helpers**:
  Utility functions for generating finger table entries.

- **logging**:
  Used for logging errors and debugging information.

"""

# Description: This file contains the RPC procedures that are used to communicate with other nodes in the network.
from typing import Optional, List

import aiomas
from chord.helpers import gen_finger
import logging

logger = logging.getLogger(__name__)

######################
# RPC Procedures
######################


async def rpc_ask_for_succ(next_node: dict, numeric_id: int) -> (bool, Optional[dict]):
    """
    Finds the successor for a given numeric ID.

    Args:
        next_node (dict): The next node.
        numeric_id (int): The numeric ID of the node.

    Returns:
        bool: Whether or not a successor exists.
        Optional[dict]: The successor node if it exists.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        found, rep = await rpc_con.remote.find_successor(numeric_id)
        await rpc_con.close()
        return found, rep
    except Exception as e:
        return False, None


async def rpc_ask_for_pred_and_succlist(addr: str) -> (Optional[dict], List):
    """
    Gets the predecessor and successor list of the current node.

    Args:
        addr (str): Address of the target node.

    Returns:
        Optional[dict]: The predecessor node.
        List: The list of successors.
    """
    host, port = addr.split(":")
    try:
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_pred_and_succlist()
        await rpc_con.close()
        return rep
    except Exception as e:
        return None, None


async def rpc_ping(addr: str) -> bool:
    """
    Pings a node with a given address.

    Args:
        addr (str): The address of the target node to ping.

    Returns:
        bool: True if the node responds with "pong", False otherwise.
    """
    try:
        host, port = addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.ping()
        await rpc_con.close()
        return rep == "pong"
    except Exception as e:
        return False


async def rpc_notify(succ_addr: str, my_addr: str, ring_sz: int, keysize: int) -> None:
    """
    Notifies a node that the calling node is now its predecessor.

    Args:
        succ_addr (str): The address of the successor node.
        my_addr (str): Address of the calling node.
        ring_sz (int): The size of the ring.
        keysize (int): The number of characters to extract from the hash.
    """
    try:
        host, port = succ_addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        await rpc_con.remote.notify(gen_finger(my_addr, ring_sz=ring_sz, keysize=keysize))
        await rpc_con.close()
    except Exception as e:
        print(e)


async def rpc_find_job(next_node: dict, job_id: str, ttl: int) -> Optional[str]:
    """
    Checks the current node for the value or delegates to appropriate successors.

    Args:
        next_node (dict): The next node.
        job_id (str): The job ID to find.
        ttl (int): Time to live for the message.

    Returns:
        Optional[str]: The job data if found, None otherwise.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.find_job(job_id, ttl)
        await rpc_con.close()
        return rep
    except Exception as e:
        return None


async def rpc_get_key(next_node: dict, key: str, ttl: int, is_replica: bool) -> Optional[str]:
    """
    Checks the current node for the value or delegates to appropriate successors.

    Args:
        next_node (dict): The next node.
        key (str): The key to find.
        ttl (int): Time to live for the message.
        is_replica (bool): Whether or not the current node is a replica.

    Returns:
        Optional[str]: The value if found, None otherwise.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.find_key(key, ttl, is_replica=is_replica)
        await rpc_con.close()
        return rep
    except Exception as e:
        return None


async def rpc_save_key(next_node: dict, key: str, value: str, ttl: int) -> Optional[str]:
    """
    Stores a key-value pair in the actual storage.

    Args:
        next_node (dict): The next node.
        key (str): The key under which the value shall be stored.
        value (str): The value/data being stored.
        ttl (int): Time to live for the message.

    Returns:
        Optional[str]: The response from the node.
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
    Generates multiple DHT keys for each value for replication and stores them.

    Args:
        next_node (dict): The next node.
        key (str): The key under which the value shall be stored.
        value (str): The value/data being stored.

    Returns:
        Optional[str]: The response from the node.
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
    Gets all key-value pairs of the node with the given node ID.

    Args:
        next_node (dict): The next node.
        node_id (int): The ID of the node.

    Returns:
        tuple: A tuple containing the keys and values stored on the node.
    """
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_all(node_id)
        await rpc_con.close()
        return rep
    except Exception as e:
        return None
