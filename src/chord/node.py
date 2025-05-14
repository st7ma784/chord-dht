"""
Node Module
===========

This module implements the `Node` class, which is responsible for managing a node in the Chord Distributed Hash Table (DHT) system. 
The Chord algorithm is implemented to enable decentralized and efficient management of key-value pairs across a distributed network of nodes.

Key Features
------------

- **Node Initialization**:
  - Initializes a node with its address, ID, and storage.
  - Configures the MinIO client for object storage integration.

- **Joining the Network**:
  - Allows a node to join an existing Chord ring by connecting to a bootstrap node.
  - Handles the initialization of the finger table, predecessor, and successor.

- **Stabilization**:
  - Periodically verifies and updates the successor and predecessor nodes.
  - Ensures the finger table is up-to-date for efficient routing.

- **Routing**:
  - Implements methods to find the closest preceding node and the successor for a given key.
  - Supports multi-hop routing to locate the appropriate node for a key.

- **Storage Integration**:
  - Integrates with the `Storage` class to manage key-value pairs locally.
  - Supports replication and retrieval of keys across the network.

Dependencies
------------

- **api.job**:
  Provides job management functionality for distributed tasks.

- **chord.helpers**:
  Utility functions for generating IDs, checking ranges, and printing tables.

- **chord.rpc**:
  Remote procedure call (RPC) functions for inter-node communication.

- **chord.storage**:
  Local storage management for key-value pairs.

- **minio**:
  MinIO client for object storage integration.

Classes
-------

- **Node**:
  Represents a node in the Chord DHT system and implements the core Chord algorithm.

"""

import asyncio
import os
from api.job import Job
from chord.helpers import generate_id, between, print_table
from chord.rpc import *
from chord.storage import Storage
from minio import Minio
from typing import Optional
import logging
from contextlib import suppress

logger = logging.getLogger(__name__)

class Node:
    """
    Class responsible for managing a Chord DHT node.

    The Chord algorithm is implemented in the following methods:
    1. join: Join the network by connecting to a known node. Initializes the node's predecessor and finger table.
    2. stabilize: Called periodically to verify the current node's successor and notify the successor about the current node.
    3. fix_fingers: Called periodically to update finger table entries.
    4. fix_successor: Called periodically to update the successor of the current node.
    """

    router = aiomas.rpc.Service()

    def __init__(self, host: str, port: str, **kwargs):
        """
        Initializes a Chord DHT node.

        Args:
            host (str): Host address of the node.
            port (str): Port number of the node.
            **kwargs: Additional keyword arguments.
        """
        self._addr = f"{host}:{port}"
        self.minio_url = kwargs.get("minio_url", os.environ.get("MINIO_URL", "localhost:9000"))
        print(f"Minio URL: {self.minio_url}")
        self.MinioClient = Minio(
            self.minio_url,
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False,
        )

        print("Known Buckets: {}".format(self.MinioClient.list_buckets()))
        self.ring_sz = 2 ** 16
        self.key_sz = 16 // 4
        self._id = generate_id(self._addr.encode("utf-8"), keysize=self.key_sz)
        self._numeric_id = int(self._id, 16) % self.ring_sz

        self._MAX_STEPS = 8
        self._MAX_SUCC = 6
        self._REPLICATION_COUNT = 1

        self._fingers = [{"addr": "", "id": "", "numeric_id": -1} for _ in range(16)]
        self._predecessor = None
        self._successor = None
        self._storage = Storage(node=self)

        self._successors = [None for _ in range(self._MAX_SUCC)]
        self._next = 0

    def _init_empty_fingers(self):
        """
        Generates an empty finger table with the node's address as fingers.
        """
        addr = self._successor["addr"] if self._successor else self._addr
        _id = generate_id(addr.encode("utf-8"), keysize=self.key_sz)
        for i in range(len(self._fingers)):
            self._fingers[i] = {"addr": addr, "id": _id, "numeric_id": int(_id, 16)}

        self._successor = self._fingers[0]
        self._successors = [self._successor.copy() for _ in range(len(self._successors))]

    async def join(self, bootstrap_node: Optional[str]):
        """
        Joins the Chord network by connecting to a known bootstrap node.

        Args:
            bootstrap_node (Optional[str]): Address of the bootstrap node.
        """
        if not bootstrap_node or bootstrap_node == self._addr or bootstrap_node == "None" or bootstrap_node == os.getenv("HOSTNAME", "localhost"):
            self._create()
        else:
            if self._successor is None:
                _, self._successor = await rpc_ask_for_succ(
                    gen_finger(bootstrap_node, self.ring_sz, self.key_sz), self._numeric_id
                )
                self._init_empty_fingers()
                keys, values = await rpc_get_all_keys(
                    next_node=self._successor, node_id=self._numeric_id,
                )
                self._storage.put_keys(keys, values)
            else:
                raise Exception("Attempting to join after joining before.")

        self.dump_me()

    def _create(self):
        """
        Creates a new Chord ring.
        """
        self._predecessor = None
        self._init_empty_fingers()

    def _closest_preceding_node(self, numeric_id: int):
        """
        Finds the closest preceding node for a given numeric ID.

        Args:
            numeric_id (int): The numeric ID of the node.

        Returns:
            dict: The closest preceding node.
        """
        for i in range(len(self._fingers) - 1, -1, -1):
            if self._fingers[i]["numeric_id"] != -1:
                if between(
                    self._fingers[i]["numeric_id"],
                    self._numeric_id,
                    numeric_id,
                    inclusive_left=False,
                    inclusive_right=False,
                    ring_sz=self.ring_sz,
                ):
                    return self._fingers[i]
        return self._successor

    def _find_successor(self, _numeric_id: int):
        """
        Finds the successor for a given numeric ID.

        Args:
            _numeric_id (int): The numeric ID of the node.

        Returns:
            tuple: A tuple containing a boolean indicating if the successor was found and the successor node.
        """
        is_bet = between(
            _numeric_id,
            self._numeric_id,
            self._successor["numeric_id"],
            inclusive_left=False,
            inclusive_right=True,
            ring_sz=self.ring_sz,
        )
        if is_bet:
            return True, self._successor
        return False, self._closest_preceding_node(_numeric_id)

    @aiomas.expose
    async def find_successor(self, numeric_id: int):
        """
        Finds the successor for a given numeric ID.

        Args:
            numeric_id (int): The numeric ID of the node.

        Returns:
            tuple: A tuple containing a boolean indicating if the successor was found and the successor node.
        """
        found, next_node = self._find_successor(numeric_id)
        i = 0
        while not found and i < self._MAX_STEPS:
            found, next_node = await rpc_ask_for_succ(next_node, numeric_id)
            i += 1
        if found:
            return True, next_node
        return False, None

    async def stabilize(self):
        """
        Periodically stabilizes the network by verifying and updating the successor and predecessor nodes.
        """
        _fix_interval = 1
        print_interval = 200
        time_since_last_print = print_interval
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            try:
                pred, succ_list = await rpc_ask_for_pred_and_succlist(self._successor["addr"])
                if pred is not None:
                    if between(
                        pred["numeric_id"],
                        self._numeric_id,
                        self._successor["numeric_id"],
                        inclusive_right=False,
                        inclusive_left=False,
                        ring_sz=self.ring_sz,
                    ):
                        self._successor = pred.copy()
                        self._fingers[0] = self._successor
                    if self._predecessor is None or between(
                        pred["numeric_id"],
                        self._predecessor["numeric_id"],
                        self._numeric_id,
                        inclusive_left=False,
                        inclusive_right=False,
                        ring_sz=self.ring_sz,
                    ):
                        self._predecessor = pred.copy()
                        await rpc_notify(self._successor["addr"], self._addr, self.ring_sz, self.key_sz)

                self._successors = [self._successor] + succ_list[:-1]
                for i in range(len(self._fingers)):
                    next_id = (self._numeric_id + (2 ** i)) % self.ring_sz
                    found, succ = await self.find_successor(next_id)
                    if found and self._fingers[i] != succ:
                        self._fingers[i] = succ
                await rpc_notify(self._successor["addr"], self._addr, self.ring_sz, self.key_sz)
            except Exception as e:
                self._successors = self._successors[1:]
                if len(self._successors) == 0:
                    self._successors.append(gen_finger(self._addr, self.key_sz))
                    self._successor = self._successors[0].copy()
                else:
                    self._successor = self._successors[0].copy()

            time_since_last_print -= _fix_interval
            if time_since_last_print <= 0:
                time_since_last_print = print_interval
                self.dump_me()

    def dump_me(self):
        """
        Prints a dump of all relevant node information for debugging purposes.
        """
        my_data = [{"addr": self._addr, "id": self._id, "numeric_id": self._numeric_id}]
        my_data += [self._successor]
        my_data += [self._predecessor]
        print_table(my_data)
        print_table(self._successors)
        print_table(self._fingers)
