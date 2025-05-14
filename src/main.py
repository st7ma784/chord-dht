import argparse
import asyncio
import os
import aiohttp.web

import aiomas
import nest_asyncio

from api.controller import ApiController
from chord.node import Node
import threading
"""
Main Module
===========

This module serves as the entry point for starting a Chord Distributed Hash Table (DHT) node and its associated 
API server. It initializes the Chord node, joins it to the Chord ring, and starts the API server for handling 
external requests. The module also schedules background tasks for maintaining the Chord ring and managing 
distributed jobs.

Key Features
------------

- **Chord Node Initialization**:
  - Starts a Chord node with the specified DHT address and MinIO URL.
  - Joins the node to an existing Chord ring or starts a new ring if no bootstrap node is provided.

- **API Server**:
  - Starts an HTTP API server using `aiohttp` to interact with the Chord DHT.
  - Provides routes for job submission, querying the Chord ring, and interacting with MinIO object storage.

- **Background Tasks**:
  - Schedules tasks for stabilizing the Chord ring, fixing finger tables, checking predecessors, and maintaining 
    the successor list.
  - Runs a worker task for processing distributed jobs.

- **Command-Line Interface**:
  - Parses command-line arguments to configure the DHT node, API server, and MinIO integration.
  - Supports specifying the DHT address, API address, MinIO URL, and bootstrap node.

Dependencies
------------

- **aiohttp**:
  Provides the framework for handling HTTP requests and responses.

- **aiomas**:
  Enables RPC communication between Chord nodes.

- **asyncio**:
  Manages asynchronous operations and event loops.

- **nest_asyncio**:
  Allows nested event loops for compatibility with certain environments.

- **argparse**:
  Parses command-line arguments for configuring the DHT node and API server.

- **MinIO**:
  Used for interacting with MinIO object storage for distributed job management.

"""

async def _start_api_server(chord_node: Node):
    """
    Starts the API server for the Chord node.

    Args:
        chord_node (Node): The Chord node instance to associate with the API server.

    Returns:
        aiohttp.web.Application: The aiohttp application instance for the API server.
    """
    app = aiohttp.web.Application()
    api_controller = ApiController(chord_node)
    app.add_routes(api_controller.get_routes())
    return app


async def _stop_api_server(runner):
    """
    Stops the API server.

    Args:
        runner (aiohttp.web.AppRunner): The application runner for the API server.

    Returns:
        None
    """
    await runner.cleanup()


async def _start_chord_node(args):
    """
    Starts a Chord node.

    Args:
        args (argparse.Namespace): Command-line arguments containing the DHT address and MinIO URL.

    Returns:
        tuple: A tuple containing the host, port, and the Chord node instance.
    """
    dht_address = args.dht_address
    host, port = dht_address.split(":")
    args = {"minio_url": args.minio_url}
    return host, int(port), Node(host=host, port=port, **args)


async def _start(args: argparse.Namespace):
    """
    Starts the Chord node and its associated API server.

    This function initializes the Chord node, joins it to the Chord ring, and starts the API server
    for handling external requests. It also schedules background tasks for maintaining the Chord ring.

    Args:
        args (argparse.Namespace): Command-line arguments containing the DHT address, API address,
            MinIO URL, and bootstrap node information.

    Returns:
        None
    """
    nest_asyncio.apply()

    dht_host, dht_port, chord_node = await _start_chord_node(args)
    await chord_node.join(bootstrap_node=args.bootstrap_node)

    loop = asyncio.get_event_loop()
    stabilize_task = loop.create_task(chord_node.stabilize())
    fix_fingers_task = loop.create_task(chord_node.fix_fingers())
    check_pred_task = loop.create_task(chord_node.check_predecessor())
    fix_successor_task = loop.create_task(chord_node.fix_successor_list())
    do_work = loop.create_task(chord_node.worker())

    api_address = args.api_address
    api_host = api_address.split(":")[0]
    api_port = api_address.split(":")[1]
    chord_rpc_server = await aiomas.rpc.start_server((dht_host, int(dht_port)), chord_node)
    app = await _start_api_server(chord_node)

    # Ensure API server is running
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()

    run_site = loop.create_task(aiohttp.web.TCPSite(runner, api_host, int(api_port)).start())

    async with chord_rpc_server:
        return await asyncio.gather(
            loop.run_until_complete(chord_rpc_server.serve_forever()),
            loop.run_until_complete(fix_fingers_task),
            loop.run_until_complete(stabilize_task),
            loop.run_until_complete(check_pred_task),
            loop.run_until_complete(do_work),
            loop.run_until_complete(fix_successor_task),
            loop.run_until_complete(run_site)
        )


if __name__ == "__main__":
    """
    Entry point for starting the Chord DHT node and API server.

    Parses command-line arguments to configure the DHT node, API server, and MinIO integration.
    Initializes the Chord node and starts the event loop for handling requests and maintaining the ring.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--dht_address", help="Address to run the DHT Node on", default="{}:6501".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument("--api_address", help="Address to run the API server on", default="{}:8001".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument("--minio_url", help="Address to run the MinIO server on", default="{}:9000".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument(
        "--bootstrap_node", help="Start a new Chord Ring if argument not present", default=None,
    )
    arguments = parser.parse_args()
    asyncio.run(_start(arguments))
