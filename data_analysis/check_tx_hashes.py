#!/usr/bin/env python3
"""
Check transaction hash format and print explorer links for Polygon mainnet and Amoy testnet.
"""
import json
import re
import sys

# You can change this to your table or file source
# For demo, we'll just use a hardcoded list
hashes = [
    "0x65fccce5a289236d4ffcc85de871bcabf33211f10aa9c843db2620238114fa8c",
    "0x7a1cc6438201cbb42e9a952ca10b2c24c114c6bde0b0efadb9e65938f7f6b5e9",
    "0x2e54e6254aadccf37eed10af7486b724857296bfed272682c055604e9b2062e5"
]

# Regex for valid tx hash
hash_re = re.compile(r"^0x[a-fA-F0-9]{64}$")

for h in hashes:
    valid = bool(hash_re.match(h))
    print(f"{h} | valid: {valid}")
    if valid:
        print(f"  Polygon Mainnet: https://polygonscan.com/tx/{h}")
        print(f"  Polygon Amoy Testnet: https://amoy.polygonscan.com/tx/{h}")
    else:
        print("  INVALID FORMAT!")

# To use with your table, you can adapt this to read from your DB or a file.
