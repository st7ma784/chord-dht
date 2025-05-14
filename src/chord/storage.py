"""
Storage Class
=============

The `Storage` class provides functionality for securely storing and retrieving key-value pairs in a distributed 
Chord DHT system. It ensures data authenticity and integrity using HMAC-based hashing and supports operations 
such as adding, deleting, and retrieving keys and values.

Methods
-------

- **make_digest**:
  Generates an HMAC digest for a given message using the node's secret key.

- **get_key**:
  Retrieves the value associated with a given key, ensuring its integrity.

- **put_key**:
  Stores a value under a given key with an optional time-to-live (TTL).

- **_del_key**:
  Deletes a specific key from storage.

- **del_keys**:
  Deletes multiple keys from storage.

- **get_my_data**:
  Retrieves all keys and values stored in the current storage instance.

- **iterjobs**:
  Asynchronously iterates over and pops all jobs from storage.

- **get_keys**:
  Retrieves all keys and values within a specified range.

- **put_keys**:
  Stores multiple key-value pairs in storage.

Attributes
----------

- **_store**:
  A `diskcache.Cache` instance used for local storage.

- **node**:
  The node instance associated with this storage.

- **node_id**:
  The unique identifier of the node.

"""
import hashlib
import hmac
import os
from typing import List

from diskcache import Cache

from chord.helpers import between


class Storage:
    """
    Provides secure storage functionality for a Chord DHT node.

    This class handles key-value storage with HMAC-based integrity checks, ensuring that data is authentic and 
    has not been tampered with. It also supports operations for managing keys and values within the distributed 
    system.
    """

    def make_digest(self, message: bytes) -> str:
        """
        Generates an HMAC digest for a given message using the node's secret key.

        Args:
            message (bytes): The message to hash.

        Returns:
            str: The HMAC digest of the message.
        """
        secret_key = os.environ.get("SEC_KEY", self.node_id)
        return hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256).hexdigest()

    def __init__(self, node):
        """
        Initializes the Storage instance.

        Args:
            node: The node instance associated with this storage.
        """
        self._store = Cache("./chord_data")
        self.node = node
        self.node_id = self.node._id

    def get_key(self, key: str):
        """
        Retrieves the value associated with a given key, ensuring its integrity.

        Args:
            key (str): The key to retrieve.

        Returns:
            str: The value stored under the key, or None if the key does not exist or the integrity check fails.
        """
        value = None
        try:
            value, tag = self._store.get(f"{key}", tag=True)
            if value:
                _val_bytes = value.encode("utf-8")
                if tag != self.make_digest(_val_bytes):
                    return None
        except (TimeoutError, AttributeError):
            pass
        return value

    def put_key(self, key: str, value: str, ttl: int = 3600) -> bool:
        """
        Stores a value under a given key with an optional time-to-live (TTL).

        Args:
            key (str): The key under which the value will be stored.
            value (str): The value to store.
            ttl (int): The time-to-live for the key-value pair in seconds.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        _byte_val = value.encode("utf-8")
        try:
            return self._store.set(key, value=value, expire=ttl, tag=self.make_digest(_byte_val))
        except Exception:
            return False

    def _del_key(self, key):
        """
        Deletes a specific key from storage.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was successfully deleted, False otherwise.
        """
        return self._store.delete(key)

    def del_keys(self, keys: List[str]):
        """
        Deletes multiple keys from storage.

        Args:
            keys (List[str]): A list of keys to delete.
        """
        for key in keys:
            self._del_key(key)

    def get_my_data(self):
        """
        Retrieves all keys and values stored in the current storage instance.

        Returns:
            tuple: A tuple containing two lists - keys and their corresponding values.
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
        """
        Asynchronously iterates over and pops all jobs from storage.

        Yields:
            tuple: A tuple containing the key and serialized job data.
        """
        for key in self._store.iterkeys():
            job_serial = await self._store.pop(key)
            yield key, job_serial

    def get_keys(self, left: int, right: int):
        """
        Retrieves all keys and values within a specified range.

        Args:
            left (int): The start interval (exclusive).
            right (int): The end interval (exclusive).

        Returns:
            tuple: A tuple containing two lists - keys and their corresponding values.
        """
        keys = []
        values = []
        for key in self._store.iterkeys():
            if between(int(key, 16), left, right, inclusive_left=False, inclusive_right=False, ring_sz=self.node.ring_sz):
                val = self.get_key(key)
                if val:
                    keys.append(key)
                    values.append(val)
        return keys, values

    def put_keys(self, keys, values):
        """
        Stores multiple key-value pairs in storage.

        Args:
            keys (list): A list of keys.
            values (list): A list of values corresponding to the keys.
        """
        for idx, key in enumerate(keys):
            self.put_key(key, values[idx])
