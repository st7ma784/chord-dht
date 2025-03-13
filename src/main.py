import argparse
import asyncio
import os
import aiohttp.web

import aiomas
import nest_asyncio

from api.controller import ApiController
from chord.node import Node
import threading


async def _start_api_server(chord_node: Node):
    app = aiohttp.web.Application()
    api_controller = ApiController(chord_node)
    app.add_routes(api_controller.get_routes())
    return app



async def _stop_api_server(runner):
    await runner.cleanup()


async def _start_chord_node(args):
    """
    Start Chord Node
    """
    dht_address = args.dht_address
    host, port = dht_address.split(":")
    args={"minio_url":args.minio_url}
    return host, int(port), Node(host=host, port=port,**args)


async def _start(args: argparse.Namespace):
    nest_asyncio.apply()

    dht_host, dht_port, chord_node = await _start_chord_node(args)
    await chord_node.join(bootstrap_node=args.bootstrap_node)

    loop = asyncio.get_event_loop()
    stabilize_task = loop.create_task(chord_node.stabilize())
    fix_fingers_task = loop.create_task(chord_node.fix_fingers())
    check_pred_task = loop.create_task(chord_node.check_predecessor())
    fix_successor_task = loop.create_task(chord_node.fix_successor_list())
    do_work= loop.create_task(chord_node.worker())



    api_address = args.api_address
    api_host = api_address.split(":")[0]
    api_port = api_address.split(":")[1]
    chord_rpc_server = await aiomas.rpc.start_server((dht_host, int(dht_port)), chord_node)
    app = await _start_api_server(chord_node)
    # ensure api server is running
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()



    run_site=loop.create_task(aiohttp.web.TCPSite(runner, api_host, int(api_port)).start())

    async with chord_rpc_server:
        return await asyncio.gather(
            loop.run_until_complete(chord_rpc_server.serve_forever()),
            loop.run_until_complete(stabilize_task),
            loop.run_until_complete(fix_fingers_task),
            loop.run_until_complete(check_pred_task),
            loop.run_until_complete(do_work),
            loop.run_until_complete(fix_successor_task),
            loop.run_until_complete(run_site)
        )




if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    import os
    parser.add_argument("--dht_address", help="Address to run the DHT Node on", default="{}:6501".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument("--api_address", help="Address to run the DHT Node on", default="{}:8001".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument("--minio_url", help="Address to run the Minio Server on", default="{}:9000".format(os.getenv("HOSTNAME", "localhost")))
    parser.add_argument(
        "--bootstrap_node", help="Start a new Chord Ring if argument no present", default=None,
    )
    arguments = parser.parse_args()
    asyncio.run(_start(arguments))
