import asyncio
import os
from api.job import Job
from chord.helpers import generate_id, between, print_table
from chord.rpc import *
from chord.storage import Storage
# from config.config import dht_config
from minio import Minio
from typing import Optional
import logging
logger = logging.getLogger(__name__)
class Node:
    """
    Class Responsible for Managing a Chord DHT Node.
    The chord algorithm is implemented in the next methods:
    1 - join : Join the network by connecting to a known node.
    Initialize node n: (the predecessor and the finger table).
    Notify other nodes to update their predecessors and finger tables.
    The new node takes over its responsible keys from its successor.
    The predecessor of n can be easily obtained from the predecessor of the
         successor(n) (in the previous circle). As for its finger table, there are various initialization methods. The simplest one is to execute find successor queries for all m entries, 
    2 - stabilize : Called periodically. verifies current node’s successor, and tells the successor about the current node.
    To ensure correct lookups, all successor pointers must be up to date. Therefore, a stabilization protocol is running periodically in the background which updates finger tables and successor pointers.
    The stabilization protocol works as follows:
    Stabilize(): n asks its successor for its predecessor p and decides whether p should be n's successor instead (this is the case if p recently joined the system).
    Notify(): notifies n's successor of its existence, so it can change its predecessor to n
    Fix_fingers(): updates finger tables
    3 - fix_fingers : Called periodically. Updates finger table entries.
    4 - fix_successor : Called periodically. Updates the successor of the current node.

    """

    router = aiomas.rpc.Service()

    def __init__(self, host: str, port: str, **kwargs):
        self._addr = f"{host}:{port}"
        self.minio_url=kwargs.get("minio_url",os.environ.get("MINIO_URL","localhost:9000"))
        print(f"Minio URL: {self.minio_url}")
        self.MinioClient = Minio(
            self.minio_url,
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False,
        )

        #print known buckets at spin up 
        print("Known Buckets: {}".format(self.MinioClient.list_buckets()))
        self.ring_sz = 2 ** (int(16))
        self.key_sz = 16 //4
        self._id = generate_id(self._addr.encode("utf-8"), keysize=self.key_sz)
        self._numeric_id = int(self._id, 16) % self.ring_sz

        self._MAX_STEPS = int(8)
        self._MAX_SUCC = int(6)
        self._REPLICATION_COUNT = 1

        self._fingers = [
            {"addr": "", "id": "", "numeric_id": -1} for _ in range(16)
        ]

        self._predecessor = None
        self._successor = None

        self._storage = Storage(node=self)

        # for stabilization
        self._successors = [None for _ in range(self._MAX_SUCC)]
        self._next = 0
        
        # tls_dir = os.environ.get("TLS_DIR", "node_1")

    
    ##################################
    # Node Initialization(s)
    ##################################

    def _init_empty_fingers(self):
        """
        Generate empty finger table with my address as fingers.
        """
        addr = self._successor["addr"] if self._successor else self._addr
        _id = generate_id(addr.encode("utf-8"), keysize=self.key_sz)
        for i in range(len(self._fingers)):
            self._fingers[i] = {"addr": addr, "id": _id, "numeric_id": int(_id, 16)}

        self._successor = self._fingers[0]
        self._successors = [self._successor.copy() for _ in range(len(self._successors))]

    async def join(self, bootstrap_node: Optional[str]):
        # Create a new ring if no bootstrap node is given
        if not bootstrap_node or bootstrap_node == self._addr or bootstrap_node == "None" or bootstrap_node == os.getenv("HOSTNAME", "localhost"):
            # logger.debug("Bootstrap node initialized...")
            self._create()
        else:
            if self._successor is None:
                _, self._successor = await rpc_ask_for_succ(
                    gen_finger(bootstrap_node,self.ring_sz,self.key_sz), self._numeric_id
                )
                self._init_empty_fingers()
                # get keys from succ
                keys, values = await rpc_get_all_keys(
                    next_node=self._successor, node_id=self._numeric_id,
                )
                self._storage.put_keys(keys, values)
            else:
                raise Exception("Attempting to join after joining before.")

        self.dump_me()

    def _create(self):
        self._predecessor = None
        self._init_empty_fingers()

    ##################################
    # Find Successor
    ##################################

    def _closest_preceding_node(self, numeric_id: int):
        """Find the closest preceding node.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              node (dict): the closest preceding node.
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
                    # print(
                    #     f"Using finger {i} => {self._fingers[i]} {numeric_id} is between ({self._fingers[i]['numeric_id']},{self._numeric_id}]")
                    return self._fingers[i]
        return self._successor

    def _find_successor(self, _numeric_id: int):
        """
        Find the successor for a given node id if it is within the interval of the
        node itself and its successor. If successor is not in range,
        checks the finger table.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              found (bool): Whether or not a successor exists.
              successor (dict): The successor if it exists.
        """
        is_bet = between(
            _numeric_id,
            self._numeric_id,
            self._successor["numeric_id"],
            inclusive_left=False,
            inclusive_right=True,
            ring_sz=self.ring_sz,
       )
        # logger.debug(f"Finding succ for: {_numeric_id} using node {self._numeric_id}: {is_bet}")
        if is_bet:
            return True, self._successor
        return False, self._closest_preceding_node(_numeric_id)

    @aiomas.expose
    async def find_successor(self, numeric_id: int):
        """
        Find the successor for a given node id.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              found (bool): Whether or not a successor exist.
              successor (dict): The successor if it exists.
        """
        found, next_node = self._find_successor(numeric_id)
        i = 0
        while not found and i < self._MAX_STEPS:
            found, next_node = await rpc_ask_for_succ(next_node, numeric_id)
            i += 1
        if found:
            return True, next_node
        return False, None

    ##################################
    # Network Stabilization
    ##################################

    async def check_predecessor(self):
        _fix_interval = 1
        while True:
            await asyncio.sleep(_fix_interval)
            if self._predecessor:
                res = await rpc_ping(self._predecessor["addr"])
                if not res:
                    self._predecessor = None

    @aiomas.expose
    def get_pred_and_succlist(self):
        """Gets the predecessor and successor list of the current node.
          Returns
              dict: predecessor
              array: The list of successors
        """
        return self._predecessor, self._successors

    async def stabilize(self):
        """
        Stabilize(): n asks its successor for its predecessor p and decides whether p should be n's successor instead (this is the case if p recently joined the system).

        """
        # if succ not yet set don't run stabilize
        _fix_interval = 1
        print_interval = 200
        time_since_last_print = print_interval
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            # print("Stabilizing the network")
            try:
                pred, succ_list = await rpc_ask_for_pred_and_succlist(
                    self._successor["addr"]
                )
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
                    #else it should be our predecessor, and we should update our predecessor
                    if self._predecessor is None or between(
                        pred["numeric_id"],
                        self._predecessor["numeric_id"],
                        self._numeric_id,
                        inclusive_left=False,
                        inclusive_right=False,
                        ring_sz=self.ring_sz,
                    ):
                        self._predecessor = pred.copy()
                        #and we should notify our successor that we are their predecessor
                        await rpc_notify(self._successor["addr"], self._addr, self.ring_sz, self.key_sz)
                    
                    
                self._successors = [self._successor] + succ_list[:-1]
                #use successors to update fingers
                for i in range(len(self._fingers)):
                    next_id = (self._numeric_id + (2 ** (i))) % (self.ring_sz)
                    found, succ = await self.find_successor(next_id)
                    if found and self._fingers[i] != succ:
                        # print(f"Finger {i} updated from {self._fingers[i]['addr']} to {succ}.")
                        self._fingers[i] = succ
                await rpc_notify(self._successor["addr"], self._addr, self.ring_sz, self.key_sz)
            except Exception as e:
                # logger.error(e)
                # logger.error("Succ is no longer working switch to next succ.")
                # print(self._successor)
                self._successors = self._successors[1:]
                if len(self._successors) == 0:
                    self._successors.append(gen_finger(self._addr,self.key_sz))
                    self._successor = self._successors[0].copy()
                else:
                    self._successor = self._successors[0].copy()

            # logger.debug("Dumping after stabilizing the network...")
            time_since_last_print -= _fix_interval
            if time_since_last_print <= 0:
                time_since_last_print = print_interval
                self.dump_me()
            # print(f"Sleeping for {SECS_TO_WAIT} secs before stabilizing again")

    async def fix_fingers(self):
        """
        Updates finger table to fix entries in case of change.
        """
        _fix_interval = 1
        while True:
            await asyncio.sleep(_fix_interval)
            self._next = (self._next + 1) % len(self._fingers)
            next_id = (self._numeric_id + int(2 ** (self._next))) % self.ring_sz
            # numeric_id = int(next_id, 16)
            found, succ = await self.find_successor(next_id)
            if found and self._fingers[self._next] != succ:
                print(f"Finger {self._next} updated from {self._fingers[self._next]['addr']} to {succ}.")
                self._fingers[self._next] = succ
            #        while True:
            # await asyncio.sleep(_fix_interval)
            # for i in range(len(self._fingers)):
            #     next_id = (self._numeric_id + (2 ** i)) % self.ring_sz
            #     found, succ = await self.find_successor(next_id)
            #     if found and self._fingers[i] != succ:
            #         self._fingers[i] = succ

    async def fix_successor(self):
        """
        Updates the successor of the current node.
        """
        _fix_interval = 1
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            found, succ = await rpc_ask_for_succ(
                self._successor["addr"], self._numeric_id
            )
            #check that self._successor is still the successor of the current node and update if not
            if found and self._successor != succ:
                if between(
                    succ["numeric_id"],
                    self._numeric_id,
                    self._successor["numeric_id"],
                    inclusive_right=False,
                    inclusive_left=False,
                    ring_sz=self.ring_sz,
                ):
                    self._successor = succ
                    self._fingers[1] = self._successor
                    await rpc_notify(self._successor["addr"], self._addr, self.ring_sz, self.key_sz)
                    # print(f"Successor updated to {succ}")
            # print_table(self._successors)
                # print(f"Successor updated to {succ}")
    async def fix_successor_list(self):    
            
        _fix_interval = 1
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            for i in range(len(self._successors)):
                if i == 0:
                    continue
                if not self._successors[i]:
                    continue
                if not between(
                    self._successors[i]["numeric_id"],
                    self._numeric_id,
                    self._successor["numeric_id"],
                    inclusive_right=False,
                    inclusive_left=False,
                    ring_sz=self.ring_sz,
                ):
                    self._successors[i] = self._successor
                    continue
                found, succ = await rpc_ask_for_succ(
                    self._successors[i]["addr"], self._numeric_id
                )
                if not found:
                    continue
                if succ != self._successors[i]:
                    self._successors[i] = succ
                    # print(f"Successor {i} updated to {succ}")
            # print_table(self._successors)


    @aiomas.expose
    def notify(self, n):
        """
        Notifies nodes which node n is now a predecessor of.
        Args:
            n (dict): The node notidying other nodes that it is their predecessor.
        """
        if not self._predecessor or between(
            n["numeric_id"],
            self._predecessor["numeric_id"],
            self._numeric_id,
            inclusive_left=False,
            inclusive_right=False,
            ring_sz=self.ring_sz,
        ):
            self._predecessor = n
            print(f"New predecessor {self._predecessor}")

    @aiomas.expose
    def save_key(self, key: str, value: str, ttl: int):
        """
        Stores key, val pair in the actual storage.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
        """
        print(f"Saving key {key} => {value} in my storage.")
        return self._storage.put_key(key, value, ttl=ttl)

    @aiomas.expose
    async def put_key(self, key: str, value: str, ttl: int = 4):
        """
        Generates multiple dht keys for each value for replication.
        Finds the node based on the key, where the value should be stored.
        Save it using save_key after a detination node is chosen.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
        Returns:
            keys (list): The update list of keys.
        """
        # generate multiple dht keys for each each
        keys = []
        for replica in range(self._REPLICATION_COUNT):
            numeric_id = int(key, 16)+replica
            # logger.warning(f"Putting Key: {key} - {dht_key} - {numeric_id}")
            found, next_node = await self.find_successor(numeric_id)
            if found:
                print(f"putting key {key} on node {next_node['addr']}")
                await rpc_save_key(
                    next_node=next_node, key=key, value=value,ttl=ttl)
            else:
                print(f"Key {key} not found")
            keys.append(key)
        return keys

    @aiomas.expose
    async def find_job(self, job_hash, ttl:int=8, is_replica: bool=False):
        """
        Finds a job in the network.
        Args:
            job_hash (string): The hash of the job.
            ttl (int): time to live. How long this should remain in the network.
            is_replica (Boolean): Whether or not the current node is a replica.
        Returns:
            Boolean: Whether or not the value is found
            String: Value if one is found.
        """
        # logger.debug(f"Finding key with TTL => {ttl} {key}")
        if ttl <= 0:
            return None
        search_cnt = 1 if is_replica else self._REPLICATION_COUNT + 1
        for idx in range(search_cnt):
            numeric_id = int(dht_key, 16)
            # logger.warning(f"Getting Key: {key} - {dht_key} - {numeric_id}")
            found, value = self._find_key(job_hash)
            if found:
                return value
            found, node = await self.find_successor(numeric_id)
            if not found:
                continue
            # logger.debug(f"Getting key from responsible node {node}")
            res = await rpc_find_job(
                next_node=node, key=job_hash, ttl=ttl - 1, is_replica=idx > 0)
            if res:
                return res
            

    @aiomas.expose
    async def find_key(self, key: str, ttl: int = 4, is_replica: bool = False):
        """
        checks current node for the value or deligates to appropriate succsorsself.
        Returns the value if it is stored on the ring.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
            is_replica (Boolean): Whether or not the current node is a replica.
        Returns:
            Boolean: Whether or not the value is found
            String: Value if one is found.
        """
        # logger.debug(f"Finding key with TTL => {ttl} {key}")
        if ttl <= 0:
            return None
        search_cnt = 1 if is_replica else self._REPLICATION_COUNT + 1
        for idx in range(search_cnt):
            dht_key = generate_id(key, keysize=self.key_sz)
            numeric_id = int(dht_key, 16)
            # logger.warning(f"Getting Key: {key} - {dht_key} - {numeric_id}")
            found, value = self._find_key(dht_key)
            if found:
                return value
            found, node = await self.find_successor(numeric_id)
            if not found:
                continue
            # logger.debug(f"Getting key from responsible node {node}")
            res = await rpc_get_key(
                next_node=node, key=key, ttl=ttl - 1, is_replica=idx > 0)
            if res:
                return res
            key = dht_key

    def _find_key(self, key: str):
        """
        A 'helper' function that returns the value if the key is store in the current node.
        Args:
            key (string): The key for whic the value is to be retrieved.
        Returns:
            Boolean: Whether or not the value is found
            String: Value if one is found.
        """
        value = self._storage.get_key(key)
        # print(f"finding key {key} => {value}")
        if value is not None:
            return True, value
        # get the succ responsible for the key
        return False, None

    @staticmethod
    @aiomas.expose
    def ping():
        return "pong"

    @aiomas.expose
    def get_all(self, node_id: int):
        """
        Gets all key, value pairs of the node with the given node_id
        Args:
            node_id (int): The id of the node.
        Returns:
            keys (list): The keys on the node as a list of strings
            values (list): Valus stored on the node as a list of string.
        """
        if not self._predecessor or not between(
            node_id,
            self._predecessor["numeric_id"],
            self._numeric_id,
            inclusive_right=False,
            inclusive_left=False,
            ring_sz=self.ring_sz,

        ):
            return [], []

        keys, values = self._storage.get_keys(self._predecessor["numeric_id"], node_id)
        self._storage.del_keys(keys)
        return keys, values

    def dump_me(self):
        """
        Used for debugging. prints a dump of all relevant node information.
        Prints the following for a node:
            *  my_data: node's `_addr`, `numeric_id`, `successor` and `predecessor`
            *  _successors: The successors of the current node.
            * _fingers: The finger table.
        """
        # logger.debug("My data, succ and pred")
        my_data = [{"addr": self._addr, "id": self._id, "numeric_id": self._numeric_id}]
        my_data += [self._successor]
        my_data += [self._predecessor]
        print_table(my_data)

        # logger.debug("My Successors")
        print_table(self._successors)

        # logger.debug("My Fingers")
        print_table(self._fingers)

    @staticmethod
    def completed():
        return "completed"


    @aiomas.expose
    async def put_job(self, job,ttl:int=6):
        """
        Stores a job in the DHT.
        Args:
            job (Job): The job to be stored.
            ttl (int): time to live. How long this should remain in the network.
        Returns:
            keys (list): The updated list of keys.
        """
        print(f"Putting job {job.job_id} in the DHT")
        key = job.hash[:self.key_sz]
        value = job.serialize()
        #convert value dict to string
        return await self.put_key(key, value, ttl=ttl)

    async def run_job(self, job):
        return await job.run(self)
        # Logic to return the result to the requester

    async def worker(self):
        """
        Worker to run through jobs stored in the DHT.
        """
        _fix_interval = 1
        print(f"Worker {self._addr} started")
    
        while True:
            try:
                await asyncio.sleep(_fix_interval)
                async for key, job_serial in self._storage.iterjobs():
                    job = Job.deserialize(job_serial)
                    
                    if job.status == "completed":
                        continue
                    elif job.status == "pending":
                        job.set_status("running")
                        self._storage.put_key(key, job.serialize())
                        print(f"Running job {job.job_id} from DHT on {self._addr}")
                        result=await self.run_job(job)
                        job.set_status("completed")
                        self._storage.put_key(key, job.serialize())
                        print(f"Job {job.job_id} completed on {self._addr}")
            except Exception as e:
                print(e)
                pass
