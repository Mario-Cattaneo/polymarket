import os
import httpx
from web3 import Web3
from datetime import datetime, timezone

# ==============================================================================
# 1. DEFINE ABIs FOR ALL EVENTS
# ==============================================================================
ABIS = {
    # USDC Token (exists on both chains with same ABI)
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": [ # Polygon USDC.e
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}],
            "name": "Transfer", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "owner", "type": "address"}, {"indexed": True, "name": "spender", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}],
            "name": "Approval", "type": "event"
        }
    ],
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": [ # Mainnet USDC
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}],
            "name": "Transfer", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "owner", "type": "address"}, {"indexed": True, "name": "spender", "type": "address"}, {"indexed": False, "name": "value", "type": "uint256"}],
            "name": "Approval", "type": "event"
        }
    ],
    # UMA Protocol Contract
    "0xac60353a54873c446101216829a6a98cdbbc3f3d": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "requester", "type": "address"}, {"indexed": False, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "time", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": True, "name": "childRequestId", "type": "bytes32"}, {"indexed": True, "name": "parentRequestId", "type": "bytes32"}],
            "name": "PriceRequestBridged", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "time", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": True, "name": "requestHash", "type": "bytes32"}],
            "name": "PriceRequestAdded", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": False, "name": "message", "type": "bytes"}],
            "name": "MessageSent", "type": "event"
        }
    ],
    # UMA Protocol Contract
    "0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "requester", "type": "address"}, {"indexed": True, "name": "proposer", "type": "address"}, {"indexed": True, "name": "disputer", "type": "address"}, {"indexed": False, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "timestamp", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": False, "name": "proposedPrice", "type": "int256"}],
            "name": "DisputePrice", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "requester", "type": "address"}, {"indexed": False, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "timestamp", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": False, "name": "currency", "type": "address"}, {"indexed": False, "name": "reward", "type": "uint256"}, {"indexed": False, "name": "finalFee", "type": "uint256"}],
            "name": "RequestPrice", "type": "event"
        }
    ],
    # Polymarket Related Contract
    "0x65070be91477460d8a7aeeb94ef92fe056c2f2a7": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "questionID", "type": "bytes32"}],
            "name": "QuestionReset", "type": "event"
        }
    ],
    # Polygon: POL Token (for fees)
    "0x0000000000000000000000000000000000001010": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "token", "type": "address"}, {"indexed": True, "name": "from", "type": "address"}, {"indexed": True, "name": "to", "type": "address"}, {"indexed": False, "name": "amount", "type": "uint256"}, {"indexed": False, "name": "input1", "type": "uint256"}, {"indexed": False, "name": "input2", "type": "uint256"}, {"indexed": False, "name": "output1", "type": "uint256"}, {"indexed": False, "name": "output2", "type": "uint256"}],
            "name": "LogFeeTransfer", "type": "event"
        }
    ],
    # UMA Oracle
    "0x9b40e25ddd4518f36c50ce8aef53ee527419d55d": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "time", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": False, "name": "price", "type": "int256"}, {"indexed": True, "name": "requestHash", "type": "bytes32"}],
            "name": "PushedPrice", "type": "event"
        },
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "time", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": True, "name": "requestHash", "type": "bytes32"}],
            "name": "PriceRequestAdded", "type": "event"
        }
    ],
    # Polygon State Syncer
    "0x28e4f3a7f651294b9564800b2d01f35189a5bfbe": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "id", "type": "uint256"}, {"indexed": True, "name": "contractAddress", "type": "address"}, {"indexed": False, "name": "data", "type": "bytes"}],
            "name": "StateSynced", "type": "event"
        }
    ],
    # UMA Voting v2 on Ethereum
    "0x004395edb43efca9885cedad51ec9faf93bd34ac": [
        {
            "anonymous": False, "inputs": [{"indexed": True, "name": "requester", "type": "address"}, {"indexed": True, "name": "roundId", "type": "uint32"}, {"indexed": True, "name": "identifier", "type": "bytes32"}, {"indexed": False, "name": "time", "type": "uint256"}, {"indexed": False, "name": "ancillaryData", "type": "bytes"}, {"indexed": False, "name": "isGovernance", "type": "bool"}],
            "name": "RequestAdded", "type": "event"
        }
    ]
}

# ==============================================================================
# 2. HELPER FUNCTIONS FOR DECODING AND FORMATTING
# ==============================================================================

def clean_bytes_string(byte_string: bytes) -> str:
    return byte_string.decode('utf-8', errors='replace').replace('\x00', '')

def format_ancillary_data(data: bytes) -> str:
    decoded_string = clean_bytes_string(data)
    if ':' in decoded_string and ',' in decoded_string:
        parts = decoded_string.split(',')
        formatted_parts = []
        for part in parts:
            if ':' in part:
                key, value = part.split(':', 1)
                formatted_parts.append(f"\n      - {key.strip()}: {value.strip()}")
            else:
                formatted_parts.append(part)
        return "".join(formatted_parts)
    return decoded_string

def format_arg(arg_name: str, arg_value, contract_address: str):
    if isinstance(arg_value, bytes):
        if arg_name == 'identifier':
            return clean_bytes_string(arg_value)
        elif arg_name == 'ancillaryData':
            return format_ancillary_data(arg_value)
        else:
            return arg_value.hex()
    if arg_name in ['time', 'timestamp']:
        return f"{arg_value} ({datetime.fromtimestamp(arg_value, tz=timezone.utc).isoformat()})"
    # Handle token amounts with correct decimals
    if contract_address in ["0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"] and arg_name in ['value', 'reward', 'finalFee']:
        return f"{arg_value} ({arg_value / 1e6:.6f} USDC)"
    if contract_address == "0x0000000000000000000000000000000000001010" and arg_name == 'amount':
        return f"{arg_value} ({arg_value / 1e18:.18f} MATIC)"
    if arg_name in ['price', 'proposedPrice']:
        return f"{arg_value} (Scaled by 1e18: {arg_value / 1e18})"
    return arg_value

def decode_log(log, w3_instance):
    log_address = log['address'].lower()
    if log_address not in ABIS:
        return None, None
    
    log_to_process = log.copy()
    log_to_process['topics'] = [Web3.to_bytes(hexstr=t) for t in log_to_process['topics']]
    log_to_process['data'] = Web3.to_bytes(hexstr=log_to_process['data'])
    
    contract_abi = ABIS[log_address]
    contract = w3_instance.eth.contract(abi=contract_abi)
    log_topic_bytes = log_to_process['topics'][0]

    for event_abi in contract_abi:
        if event_abi['type'] == 'event':
            event_signature = f"{event_abi['name']}({','.join([inp['type'] for inp in event_abi['inputs']])})"
            event_topic_hash = w3_instance.keccak(text=event_signature)
            if event_topic_hash == log_topic_bytes:
                try:
                    decoded_log = contract.events[event_abi['name']]().process_log(log_to_process)
                    return event_abi['name'], decoded_log
                except Exception as e:
                    print(f"      [!] Could not decode event {event_abi['name']}: {e}")
                    return None, None
    return None, None

# ==============================================================================
# 3. MAIN SCRIPT EXECUTION
# ==============================================================================

def main():
    infura_key = os.getenv("INFURA_KEY")
    if not infura_key:
        print("Error: INFURA_KEY environment variable not set.")
        return

    ### ------------------------------------------------------------------ ###
    ### --- FIX: Correctly assigned transaction hashes to their networks --- ###
    ### ------------------------------------------------------------------ ###
    tx_map = {
        "polygon": [
            "0x90e1f6602c31571443801462eefd225de3690b87a6aaa59564a4c675303aba2c",
        ],
        "mainnet": [
            "0x8e94098a2d0d4d2f7d8e2d17380af53235cda94c7d93d4973710716422283d6e",
            "0xfe5b325cb60a9efb40cd11fa021e6f291cd63e87b35c1455229a3d465c4dc830",
        ]
    }

    w3 = Web3()

    with httpx.Client() as client:
        for network, hashes in tx_map.items():
            if network == 'mainnet':
                infura_url = f"https://mainnet.infura.io/v3/{infura_key}"
            else:
                infura_url = f"https://{network}-mainnet.infura.io/v3/{infura_key}"
            
            payload = [
                {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [tx_hash], "id": index}
                for index, tx_hash in enumerate(hashes)
            ]

            print(f"\n{'='*80}\nFetching {len(hashes)} receipt(s) from {network.capitalize()} ({infura_url})...\n{'='*80}")
            
            try:
                response = client.post(infura_url, json=payload, timeout=30)
                response.raise_for_status()
                results = response.json()
            except httpx.ConnectError as e:
                print(f"\n[FATAL NETWORK ERROR] Could not connect to {infura_url}")
                print(f"Original error: {e}")
                continue
            except httpx.HTTPStatusError as e:
                print(f"HTTP Error fetching from {network}: {e}\nResponse body: {e.response.text}")
                continue

            for result in sorted(results, key=lambda x: x['id']):
                tx_hash = hashes[result['id']]
                print(f"\n--- TX HASH: {tx_hash} ---\n")

                if 'error' in result:
                    print(f"  [!] Infura returned an error for this transaction:")
                    print(f"      Code: {result['error'].get('code')}")
                    print(f"      Message: {result['error'].get('message')}")
                    continue

                receipt = result.get('result')
                if not receipt or not receipt.get('logs'):
                    print("  No events found in this transaction.")
                    continue

                for log in receipt['logs']:
                    log_index = int(log['logIndex'], 16)
                    contract_address = log['address']
                    print(f"  -> Log Index: {log_index} | Contract: {contract_address}")

                    event_name, decoded_log = decode_log(log, w3)

                    if event_name and decoded_log:
                        print(f"    Event: {event_name}")
                        for arg_name, arg_value in decoded_log['args'].items():
                            formatted_value = format_arg(arg_name, arg_value, contract_address.lower())
                            print(f"    - {arg_name}: {formatted_value}")
                    else:
                        print("    [!] Could not decode this log (ABI for contract or event might be missing).")
                        print(f"      Topics: {log['topics']}")
                        print(f"      Data: {log['data']}")
                    print("-" * 40)

if __name__ == "__main__":
    main()