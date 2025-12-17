import os
from web3 import Web3
from web3.exceptions import MismatchedABI
import json

def fetch_implementation_change_events_debug():
    """
    Connects to Polygon and uses the low-level eth_getLogs method to fetch
    InterfaceImplementationChanged events, with debugging output.
    """
    # 1. Set up the connection
    infura_key = os.environ.get("INFURA_KEY")
    if not infura_key:
        raise ValueError("The INFURA_KEY environment variable is not set.")

    polygon_rpc_url = f"https://polygon-mainnet.infura.io/v3/{infura_key}"
    web3 = Web3(Web3.HTTPProvider(polygon_rpc_url))

    if not web3.is_connected():
        raise ConnectionError(f"Failed to connect to the Polygon RPC URL: {polygon_rpc_url}")
    print("✅ Successfully connected to Polygon network.")

    # 2. Define contract address and ABI for the event
    contract_address = "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64"
    # We need the full contract ABI to properly decode the event logs later
    full_contract_abi = json.loads("""
    [
        {
            "anonymous": false,
            "inputs": [
                {"indexed": true, "internalType": "bytes32", "name": "interfaceName", "type": "bytes32"},
                {"indexed": true, "internalType": "address", "name": "newImplementationAddress", "type": "address"}
            ],
            "name": "InterfaceImplementationChanged",
            "type": "event"
        },
        {
            "inputs": [
                {"internalType": "bytes32", "name": "interfaceName", "type": "bytes32"},
                {"internalType": "address", "name": "implementationAddress", "type": "address"}
            ],
            "name": "changeImplementationAddress",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ]
    """)
    
    # Create a contract object which we'll use for decoding
    contract = web3.eth.contract(address=contract_address, abi=full_contract_abi)
    
    try:
        # 3. Manually calculate the event signature topic hash
        # This is the keccak256 hash of the event signature string.
        event_signature = "InterfaceImplementationChanged(bytes32,address)"
        event_topic_hash = Web3.keccak(text=event_signature).hex()
        
        print(f"\n[DEBUG] Contract Address: {contract_address}")
        print(f"[DEBUG] Event Signature: {event_signature}")
        print(f"[DEBUG] Calculated Event Topic Hash: {event_topic_hash}")

        # 4. Construct the filter object for eth_getLogs
        # This structure directly matches the JSON-RPC specification.
        filter_params = {
            "fromBlock": 0,
            "toBlock": 'latest',
            "address": contract_address,
            "topics": [event_topic_hash]
        }
        print(f"\n[DEBUG] Constructed filter for eth_getLogs: {json.dumps(filter_params, indent=2)}")

        # 5. Make the low-level JSON-RPC call
        print("\n⏳ Fetching logs using web3.eth.get_logs...")
        logs = web3.eth.get_logs(filter_params)
        print(f"✅ Found {len(logs)} raw log entries.")

        if not logs:
            return
            
        print(f"\n[DEBUG] First raw log received: {logs[0]}")

        # 6. Decode and print each log entry
        print("\n--- Decoding Events ---")
        for log in logs:
            try:
                # Use the contract event object to process each raw log
                event_data = contract.events.InterfaceImplementationChanged().process_log(log)
                args = event_data['args']

                # Decode the bytes32 interfaceName, stripping trailing null bytes
                try:
                    interface_name_bytes = args['interfaceName'].rstrip(b'\x00')
                    interface_name = interface_name_bytes.decode('utf-8')
                except (UnicodeDecodeError, AttributeError):
                    interface_name = args['interfaceName'].hex() # Fallback to hex

                implementation_address = args['newImplementationAddress']

                print(f"\n  Interface Name: {interface_name}")
                print(f"  New Implementation Address: {implementation_address}")
                print(f"  Block Number: {event_data['blockNumber']}")
                print(f"  Transaction Hash: {event_data['transactionHash'].hex()}")
                print("-" * 25)

            except MismatchedABI as e:
                print(f"\n[ERROR] Could not decode a log due to ABI mismatch: {e}")
                print(f"[DEBUG] Raw log that failed: {log}")


    except Exception as e:
        print(f"\n[FATAL ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_implementation_change_events_debug()