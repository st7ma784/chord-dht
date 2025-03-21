import hashlib
import hmac
import os
from typing import List

from diskcache import Cache

from chord.helpers import between


class Storage:
    """
    Hashes the message using a node's node_id, for authenticity and integrity.
    For secuirty purposes.
    successor about the current node.
    """

    def make_digest(self, message: bytes) -> str:
        secret_key = os.environ.get("SEC_KEY", self.node_id)
        return hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256,).hexdigest()

    def __init__(self, node):
        self._store = Cache("./chord_data")
        self.node= node
        self.node_id = self.node._id

    def get_key(self, key: str):
        """
        If they a key maps to a stored value, return the value.
          Args:
            key (string): The key that ideally maps to a desired value
          Returns:
            value (string): The value stored under the provided key. (if one exists)
        """
        value = None
        try:
            value, tag = self._store.get(f"{key}", tag=True)
            if value:
                _val_bytes = value.encode("utf-8")
                # logger.debug(f"Got {value} with digest {tag} - {self.make_digest(_val_bytes)}")
                if tag != self.make_digest(_val_bytes):
                    return None
        except (TimeoutError, AttributeError) as e:
            # logger.error(e)
            pass
        return value

    def put_key(self, key: str, value: str, ttl: int = 3600) -> bool:
        """
        Stores the `value` under the provided  `key`.
            Args:
                key (string): The key under which a vlue shall be stored.
                value (string): The value / data being stored.
                ttl (int): time to live. How long this should remain in the network.
        """
        #convert value to bytes 
        _byte_val = value.encode("utf-8")
        try:
            return self._store.set(key, value=value, expire=ttl, tag=self.make_digest(_byte_val))
        except Exception as e:
            print(e)
            print("Error in put_key")
            return False
    def _del_key(self, key):
        """
        Deletes the given `key` from storage.
            Args:
                key (string): The key to be deleted.
        """
        return self._store.delete(key)

    def del_keys(self, keys: List[str]):
        """
        Deletes multiple keys from storage.
        Args:
            keys (list): A list if the keys to be deleted.
        """
        for key in keys:
            self._del_key(key)

    def get_my_data(self):
        """
        Gets all keys and values of the current storage instance.
        """
        keys = []
        values = []
        for key in self._store.iterkeys():
            val = self.get_key(key)
            if val:
                keys.append(key)
                values.append(val)
        return keys, values

    async def iterjobs(self):
        ''' pops all jobs from the storage '''
        jobs = []
        for key in self._store.iterkeys():
            job_serial=await self._store.pop(key)
            yield key, job_serial
    
    def get_keys(self, left: int, right: int):
        """
        Gets all keys and values of the current storage instance within range:
        left (exclusive) to right (exclusive).
            Args:
                left (int): The start interval (exclusive).
                right (int): The end intervale (exclusive).
        """
        keys = []
        values = []
        # logger.debug(list(self._store.iterkeys()))
        for key in self._store.iterkeys():
            # logger.debug(
            #     f"{key} - ({left}, {right}) => {between(int(key, 16), left, right, inclusive_left=False, inclusive_right=False)}"
            # )
            if between(int(key, 16), left, right, inclusive_left=False, inclusive_right=False, ring_sz=self.node.ring_sz):
                val = self.get_key(key)
                if val:
                    keys.append(key)
                    values.append(val)

        # logger.debug(f"Got {keys} => {values}")
        return keys, values

    def put_keys(self, keys, values):
        """
        Stroes multiple key value pairs.
            Args:
                keys (list): The list of keys.
                values (list): The list of values.
        """
        for idx, key in enumerate(keys):
            self.put_key(key, values[idx])
