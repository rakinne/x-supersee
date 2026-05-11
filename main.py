from xrpl.clients import WebsocketClient
from xrpl.models import Subscribe, StreamParameter

WS_URL = "wss://xrplcluster.com/"

with WebsocketClient(WS_URL, timeout=10) as client:
    client.send(
        Subscribe(
            streams=[
                StreamParameter.TRANSACTIONS,
            ],
        )
    )
    for message in client:
        if message.get("type") != "transaction":
            continue
        tx = message.get("tx_json", {})
        print(
            {
                "validated": message.get("validated"),
                "hash": message.get("hash"),
                "tx_type": tx.get("TransactionType"),
                "account": tx.get("Account"),
                "destination": tx.get("Destination"),
                "engine_result": message.get("engine_result"),
                "ledger_index": message.get("ledger_index"),
            }
        )