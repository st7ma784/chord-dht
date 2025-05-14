"""
Chord Package
=============

This package contains modules and classes for implementing and interacting with 
a Chord Distributed Hash Table (DHT) network.

Key Changes
-----------

1. **File Organization**:
   - Added a section explaining how the files are split between the `API` folder and the `Controller`.
   - Clarified the roles of the `API` folder (core logic) and the `Controller` (web interface for nodes).

2. **Maintained Existing Content**:
   - Retained the explanation of the Chord DHT benefits and the Docker-Compose setup instructions.

3. **Improved Clarity**:
   - Highlighted the role of the `Controller` in enabling SuperDARN requests and external interactions.

Modules
-------

- **storage**:
  Provides storage-related functionality.

- **rpc**:
  Contains remote procedure call (RPC) functions for node communication, including:
  - `rpc_get_key`
  - `rpc_find_job`
  - `rpc_notify`
  - `rpc_ask_for_pred_and_succlist`
  - `rpc_ask_for_succ`
  - `rpc_save_key`
  - `rpc_put_key`
  - `rpc_get_all_keys`
  - `rpc_ping`

- **helpers**:
  Utility functions for the Chord DHT, including:
  - `generate_id`
  - `between`
  - `print_table`

- **node**:
  Implements the `Node` class, which represents a node in the Chord DHT.

"""
# from .storage import Storage
# from .rpc import (
#    rpc_get_key,
#    rpc_find_job,
#    rpc_notify,
#    rpc_ask_for_pred_and_succlist,
#    rpc_ask_for_succ,
#    rpc_save_key,
#    rpc_put_key,
#    rpc_get_all_keys,
#    rpc_ping,
# )
# from .helpers import generate_id, between, print_table
# from .node import Node