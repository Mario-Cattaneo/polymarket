from web3 import Web3
import asyncio
import orjson
import time
import traceback
import numpy as np
from typing import List, Tuple, Optional, Dict
import functools

from orderbook import OrderbookRegistry
from db_cli import DatabaseManager
from http_cli import HttpTaskConfig, HttpPosterCallbacks, HttpManager
from wss_cli import WebSocketTaskConfig, WebSocketTaskCallbacks, WebSocketManager

CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
CTF_ABI = [
    {
      "type": "constructor",
      "inputs": [
        {
          "name": "_collateral",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "_ctf",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "_proxyFactory",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "_safeFactory",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "addAdmin",
      "inputs": [
        {
          "name": "admin_",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "addOperator",
      "inputs": [
        {
          "name": "operator_",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "admins",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "cancelOrder",
      "inputs": [
        {
          "name": "order",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "cancelOrders",
      "inputs": [
        {
          "name": "orders",
          "type": "tuple[]",
          "internalType": "struct Order[]",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "domainSeparator",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "fillOrder",
      "inputs": [
        {
          "name": "order",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        },
        {
          "name": "fillAmount",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "fillOrders",
      "inputs": [
        {
          "name": "orders",
          "type": "tuple[]",
          "internalType": "struct Order[]",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        },
        {
          "name": "fillAmounts",
          "type": "uint256[]",
          "internalType": "uint256[]"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "getCollateral",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getComplement",
      "inputs": [
        {
          "name": "token",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getConditionId",
      "inputs": [
        {
          "name": "token",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getCtf",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getMaxFeeRate",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "pure"
    },
    {
      "type": "function",
      "name": "getOrderStatus",
      "inputs": [
        {
          "name": "orderHash",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "tuple",
          "internalType": "struct OrderStatus",
          "components": [
            {
              "name": "isFilledOrCancelled",
              "type": "bool",
              "internalType": "bool"
            },
            {
              "name": "remaining",
              "type": "uint256",
              "internalType": "uint256"
            }
          ]
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getPolyProxyFactoryImplementation",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getPolyProxyWalletAddress",
      "inputs": [
        {
          "name": "_addr",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getProxyFactory",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getSafeAddress",
      "inputs": [
        {
          "name": "_addr",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getSafeFactory",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "getSafeFactoryImplementation",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "hashOrder",
      "inputs": [
        {
          "name": "order",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "incrementNonce",
      "inputs": [],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "isAdmin",
      "inputs": [
        {
          "name": "usr",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "isOperator",
      "inputs": [
        {
          "name": "usr",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "isValidNonce",
      "inputs": [
        {
          "name": "usr",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "nonce",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "matchOrders",
      "inputs": [
        {
          "name": "takerOrder",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        },
        {
          "name": "makerOrders",
          "type": "tuple[]",
          "internalType": "struct Order[]",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        },
        {
          "name": "takerFillAmount",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "makerFillAmounts",
          "type": "uint256[]",
          "internalType": "uint256[]"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "nonces",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "onERC1155BatchReceived",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "",
          "type": "uint256[]",
          "internalType": "uint256[]"
        },
        {
          "name": "",
          "type": "uint256[]",
          "internalType": "uint256[]"
        },
        {
          "name": "",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bytes4",
          "internalType": "bytes4"
        }
      ],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "onERC1155Received",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        },
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "",
          "type": "bytes",
          "internalType": "bytes"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bytes4",
          "internalType": "bytes4"
        }
      ],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "operators",
      "inputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "orderStatus",
      "inputs": [
        {
          "name": "",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "outputs": [
        {
          "name": "isFilledOrCancelled",
          "type": "bool",
          "internalType": "bool"
        },
        {
          "name": "remaining",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "parentCollectionId",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "pauseTrading",
      "inputs": [],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "paused",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "proxyFactory",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "registerToken",
      "inputs": [
        {
          "name": "token",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "complement",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "conditionId",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "registry",
      "inputs": [
        {
          "name": "",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [
        {
          "name": "complement",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "conditionId",
          "type": "bytes32",
          "internalType": "bytes32"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "removeAdmin",
      "inputs": [
        {
          "name": "admin",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "removeOperator",
      "inputs": [
        {
          "name": "operator",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "renounceAdminRole",
      "inputs": [],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "renounceOperatorRole",
      "inputs": [],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "safeFactory",
      "inputs": [],
      "outputs": [
        {
          "name": "",
          "type": "address",
          "internalType": "address"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "setProxyFactory",
      "inputs": [
        {
          "name": "_newProxyFactory",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "setSafeFactory",
      "inputs": [
        {
          "name": "_newSafeFactory",
          "type": "address",
          "internalType": "address"
        }
      ],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "supportsInterface",
      "inputs": [
        {
          "name": "interfaceId",
          "type": "bytes4",
          "internalType": "bytes4"
        }
      ],
      "outputs": [
        {
          "name": "",
          "type": "bool",
          "internalType": "bool"
        }
      ],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "unpauseTrading",
      "inputs": [],
      "outputs": [],
      "stateMutability": "nonpayable"
    },
    {
      "type": "function",
      "name": "validateComplement",
      "inputs": [
        {
          "name": "token",
          "type": "uint256",
          "internalType": "uint256"
        },
        {
          "name": "complement",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "validateOrder",
      "inputs": [
        {
          "name": "order",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        }
      ],
      "outputs": [],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "validateOrderSignature",
      "inputs": [
        {
          "name": "orderHash",
          "type": "bytes32",
          "internalType": "bytes32"
        },
        {
          "name": "order",
          "type": "tuple",
          "internalType": "struct Order",
          "components": [
            {
              "name": "salt",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "maker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "signer",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "taker",
              "type": "address",
              "internalType": "address"
            },
            {
              "name": "tokenId",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "makerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "takerAmount",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "expiration",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "nonce",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "feeRateBps",
              "type": "uint256",
              "internalType": "uint256"
            },
            {
              "name": "side",
              "type": "uint8",
              "internalType": "enum Side"
            },
            {
              "name": "signatureType",
              "type": "uint8",
              "internalType": "enum SignatureType"
            },
            {
              "name": "signature",
              "type": "bytes",
              "internalType": "bytes"
            }
          ]
        }
      ],
      "outputs": [],
      "stateMutability": "view"
    },
    {
      "type": "function",
      "name": "validateTokenId",
      "inputs": [
        {
          "name": "tokenId",
          "type": "uint256",
          "internalType": "uint256"
        }
      ],
      "outputs": [],
      "stateMutability": "view"
    },
    {
      "type": "event",
      "name": "FeeCharged",
      "inputs": [
        {
          "name": "receiver",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "tokenId",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "amount",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "NewAdmin",
      "inputs": [
        {
          "name": "newAdminAddress",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "admin",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "NewOperator",
      "inputs": [
        {
          "name": "newOperatorAddress",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "admin",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "OrderCancelled",
      "inputs": [
        {
          "name": "orderHash",
          "type": "bytes32",
          "indexed": True,
          "internalType": "bytes32"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "OrderFilled",
      "inputs": [
        {
          "name": "orderHash",
          "type": "bytes32",
          "indexed": True,
          "internalType": "bytes32"
        },
        {
          "name": "maker",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "taker",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "makerAssetId",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "takerAssetId",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "makerAmountFilled",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "takerAmountFilled",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "fee",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "OrdersMatched",
      "inputs": [
        {
          "name": "takerOrderHash",
          "type": "bytes32",
          "indexed": True,
          "internalType": "bytes32"
        },
        {
          "name": "takerOrderMaker",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "makerAssetId",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "takerAssetId",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "makerAmountFilled",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        },
        {
          "name": "takerAmountFilled",
          "type": "uint256",
          "indexed": False,
          "internalType": "uint256"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "ProxyFactoryUpdated",
      "inputs": [
        {
          "name": "oldProxyFactory",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "newProxyFactory",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "RemovedAdmin",
      "inputs": [
        {
          "name": "removedAdmin",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "admin",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "RemovedOperator",
      "inputs": [
        {
          "name": "removedOperator",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "admin",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "SafeFactoryUpdated",
      "inputs": [
        {
          "name": "oldSafeFactory",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        },
        {
          "name": "newSafeFactory",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "TokenRegistered",
      "inputs": [
        {
          "name": "token0",
          "type": "uint256",
          "indexed": True,
          "internalType": "uint256"
        },
        {
          "name": "token1",
          "type": "uint256",
          "indexed": True,
          "internalType": "uint256"
        },
        {
          "name": "conditionId",
          "type": "bytes32",
          "indexed": True,
          "internalType": "bytes32"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "TradingPaused",
      "inputs": [
        {
          "name": "pauser",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "event",
      "name": "TradingUnpaused",
      "inputs": [
        {
          "name": "pauser",
          "type": "address",
          "indexed": True,
          "internalType": "address"
        }
      ],
      "anonymous": False
    },
    {
      "type": "error",
      "name": "AlreadyRegistered",
      "inputs": []
    },
    {
      "type": "error",
      "name": "FeeTooHigh",
      "inputs": []
    },
    {
      "type": "error",
      "name": "InvalidComplement",
      "inputs": []
    },
    {
      "type": "error",
      "name": "InvalidNonce",
      "inputs": []
    },
    {
      "type": "error",
      "name": "InvalidSignature",
      "inputs": []
    },
    {
      "type": "error",
      "name": "InvalidTokenId",
      "inputs": []
    },
    {
      "type": "error",
      "name": "MakingGtRemaining",
      "inputs": []
    },
    {
      "type": "error",
      "name": "MismatchedTokenIds",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotAdmin",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotCrossing",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotOperator",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotOwner",
      "inputs": []
    },
    {
      "type": "error",
      "name": "NotTaker",
      "inputs": []
    },
    {
      "type": "error",
      "name": "OrderExpired",
      "inputs": []
    },
    {
      "type": "error",
      "name": "OrderFilledOrCancelled",
      "inputs": []
    },
    {
      "type": "error",
      "name": "Paused",
      "inputs": []
    },
    {
      "type": "error",
      "name": "TooLittleTokensReceived",
      "inputs": []
    }
  ]


NegRiskCtfExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NegRiskCtfExchange_ABI = [
        {
            "type": "constructor",
            "inputs": [
                {
                    "name": "_collateral",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_ctf",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_negRiskAdapter",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_proxyFactory",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_safeFactory",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "addAdmin",
            "inputs": [
                {
                    "name": "admin_",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "addOperator",
            "inputs": [
                {
                    "name": "operator_",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "admins",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "cancelOrder",
            "inputs": [
                {
                    "name": "order",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "cancelOrders",
            "inputs": [
                {
                    "name": "orders",
                    "type": "tuple[]",
                    "internalType": "struct Order[]",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "domainSeparator",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "fillOrder",
            "inputs": [
                {
                    "name": "order",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                },
                {
                    "name": "fillAmount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "fillOrders",
            "inputs": [
                {
                    "name": "orders",
                    "type": "tuple[]",
                    "internalType": "struct Order[]",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                },
                {
                    "name": "fillAmounts",
                    "type": "uint256[]",
                    "internalType": "uint256[]"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "getCollateral",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getComplement",
            "inputs": [
                {
                    "name": "token",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getConditionId",
            "inputs": [
                {
                    "name": "token",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getCtf",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getMaxFeeRate",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "pure"
        },
        {
            "type": "function",
            "name": "getOrderStatus",
            "inputs": [
                {
                    "name": "orderHash",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "tuple",
                    "internalType": "struct OrderStatus",
                    "components": [
                        {
                            "name": "isFilledOrCancelled",
                            "type": "bool",
                            "internalType": "bool"
                        },
                        {
                            "name": "remaining",
                            "type": "uint256",
                            "internalType": "uint256"
                        }
                    ]
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getPolyProxyFactoryImplementation",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getPolyProxyWalletAddress",
            "inputs": [
                {
                    "name": "_addr",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getProxyFactory",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getSafeAddress",
            "inputs": [
                {
                    "name": "_addr",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getSafeFactory",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "getSafeFactoryImplementation",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "hashOrder",
            "inputs": [
                {
                    "name": "order",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "incrementNonce",
            "inputs": [],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "isAdmin",
            "inputs": [
                {
                    "name": "usr",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "isOperator",
            "inputs": [
                {
                    "name": "usr",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "isValidNonce",
            "inputs": [
                {
                    "name": "usr",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "nonce",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "matchOrders",
            "inputs": [
                {
                    "name": "takerOrder",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                },
                {
                    "name": "makerOrders",
                    "type": "tuple[]",
                    "internalType": "struct Order[]",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                },
                {
                    "name": "takerFillAmount",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "makerFillAmounts",
                    "type": "uint256[]",
                    "internalType": "uint256[]"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "nonces",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "onERC1155BatchReceived",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "",
                    "type": "uint256[]",
                    "internalType": "uint256[]"
                },
                {
                    "name": "",
                    "type": "uint256[]",
                    "internalType": "uint256[]"
                },
                {
                    "name": "",
                    "type": "bytes",
                    "internalType": "bytes"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes4",
                    "internalType": "bytes4"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "onERC1155Received",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "",
                    "type": "bytes",
                    "internalType": "bytes"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes4",
                    "internalType": "bytes4"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "operators",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "orderStatus",
            "inputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "outputs": [
                {
                    "name": "isFilledOrCancelled",
                    "type": "bool",
                    "internalType": "bool"
                },
                {
                    "name": "remaining",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "parentCollectionId",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "pauseTrading",
            "inputs": [],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "paused",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "proxyFactory",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "registerToken",
            "inputs": [
                {
                    "name": "token",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "complement",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "conditionId",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "registry",
            "inputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "complement",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "conditionId",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "removeAdmin",
            "inputs": [
                {
                    "name": "admin",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "removeOperator",
            "inputs": [
                {
                    "name": "operator",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "renounceAdminRole",
            "inputs": [],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "renounceOperatorRole",
            "inputs": [],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "safeFactory",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "setProxyFactory",
            "inputs": [
                {
                    "name": "_newProxyFactory",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "setSafeFactory",
            "inputs": [
                {
                    "name": "_newSafeFactory",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "supportsInterface",
            "inputs": [
                {
                    "name": "interfaceId",
                    "type": "bytes4",
                    "internalType": "bytes4"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "unpauseTrading",
            "inputs": [],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "validateComplement",
            "inputs": [
                {
                    "name": "token",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "complement",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "validateOrder",
            "inputs": [
                {
                    "name": "order",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                }
            ],
            "outputs": [],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "validateOrderSignature",
            "inputs": [
                {
                    "name": "orderHash",
                    "type": "bytes32",
                    "internalType": "bytes32"
                },
                {
                    "name": "order",
                    "type": "tuple",
                    "internalType": "struct Order",
                    "components": [
                        {
                            "name": "salt",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "maker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "signer",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "taker",
                            "type": "address",
                            "internalType": "address"
                        },
                        {
                            "name": "tokenId",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "makerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "takerAmount",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "expiration",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "nonce",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "feeRateBps",
                            "type": "uint256",
                            "internalType": "uint256"
                        },
                        {
                            "name": "side",
                            "type": "uint8",
                            "internalType": "enum Side"
                        },
                        {
                            "name": "signatureType",
                            "type": "uint8",
                            "internalType": "enum SignatureType"
                        },
                        {
                            "name": "signature",
                            "type": "bytes",
                            "internalType": "bytes"
                        }
                    ]
                }
            ],
            "outputs": [],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "validateTokenId",
            "inputs": [
                {
                    "name": "tokenId",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [],
            "stateMutability": "view"
        },
        {
            "type": "event",
            "name": "FeeCharged",
            "inputs": [
                {
                    "name": "receiver",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "tokenId",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "NewAdmin",
            "inputs": [
                {
                    "name": "newAdminAddress",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "admin",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "NewOperator",
            "inputs": [
                {
                    "name": "newOperatorAddress",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "admin",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "OrderCancelled",
            "inputs": [
                {
                    "name": "orderHash",
                    "type": "bytes32",
                    "indexed": True,
                    "internalType": "bytes32"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "OrderFilled",
            "inputs": [
                {
                    "name": "orderHash",
                    "type": "bytes32",
                    "indexed": True,
                    "internalType": "bytes32"
                },
                {
                    "name": "maker",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "taker",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "makerAssetId",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "takerAssetId",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "makerAmountFilled",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "takerAmountFilled",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "fee",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "OrdersMatched",
            "inputs": [
                {
                    "name": "takerOrderHash",
                    "type": "bytes32",
                    "indexed": True,
                    "internalType": "bytes32"
                },
                {
                    "name": "takerOrderMaker",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "makerAssetId",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "takerAssetId",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "makerAmountFilled",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                },
                {
                    "name": "takerAmountFilled",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "ProxyFactoryUpdated",
            "inputs": [
                {
                    "name": "oldProxyFactory",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "newProxyFactory",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "RemovedAdmin",
            "inputs": [
                {
                    "name": "removedAdmin",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "admin",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "RemovedOperator",
            "inputs": [
                {
                    "name": "removedOperator",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "admin",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "SafeFactoryUpdated",
            "inputs": [
                {
                    "name": "oldSafeFactory",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "newSafeFactory",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "TokenRegistered",
            "inputs": [
                {
                    "name": "token0",
                    "type": "uint256",
                    "indexed": True,
                    "internalType": "uint256"
                },
                {
                    "name": "token1",
                    "type": "uint256",
                    "indexed": True,
                    "internalType": "uint256"
                },
                {
                    "name": "conditionId",
                    "type": "bytes32",
                    "indexed": True,
                    "internalType": "bytes32"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "TradingPaused",
            "inputs": [
                {
                    "name": "pauser",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "TradingUnpaused",
            "inputs": [
                {
                    "name": "pauser",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                }
            ],
            "anonymous": False
        },
        {
            "type": "error",
            "name": "AlreadyRegistered",
            "inputs": []
        },
        {
            "type": "error",
            "name": "FeeTooHigh",
            "inputs": []
        },
        {
            "type": "error",
            "name": "InvalidComplement",
            "inputs": []
        },
        {
            "type": "error",
            "name": "InvalidNonce",
            "inputs": []
        },
        {
            "type": "error",
            "name": "InvalidSignature",
            "inputs": []
        },
        {
            "type": "error",
            "name": "InvalidTokenId",
            "inputs": []
        },
        {
            "type": "error",
            "name": "MakingGtRemaining",
            "inputs": []
        },
        {
            "type": "error",
            "name": "MismatchedTokenIds",
            "inputs": []
        },
        {
            "type": "error",
            "name": "NotAdmin",
            "inputs": []
        },
        {
            "type": "error",
            "name": "NotCrossing",
            "inputs": []
        },
        {
            "type": "error",
            "name": "NotOperator",
            "inputs": []
        },
        {
            "type": "error",
            "name": "NotOwner",
            "inputs": []
        },
        {
            "type": "error",
            "name": "NotTaker",
            "inputs": []
        },
        {
            "type": "error",
            "name": "OrderExpired",
            "inputs": []
        },
        {
            "type": "error",
            "name": "OrderFilledOrCancelled",
            "inputs": []
        },
        {
            "type": "error",
            "name": "Paused",
            "inputs": []
        },
        {
            "type": "error",
            "name": "TooLittleTokensReceived",
            "inputs": []
        }
    ]

Usdc = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
Usdc_ABI = [
        {
            "type": "constructor",
            "inputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "DOMAIN_SEPARATOR",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "allowance",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "approve",
            "inputs": [
                {
                    "name": "spender",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "balanceOf",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "burn",
            "inputs": [
                {
                    "name": "_from",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_amount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "decimals",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "uint8",
                    "internalType": "uint8"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "mint",
            "inputs": [
                {
                    "name": "_to",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "_amount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "name",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "string",
                    "internalType": "string"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "nonces",
            "inputs": [
                {
                    "name": "",
                    "type": "address",
                    "internalType": "address"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "permit",
            "inputs": [
                {
                    "name": "owner",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "spender",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "value",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "deadline",
                    "type": "uint256",
                    "internalType": "uint256"
                },
                {
                    "name": "v",
                    "type": "uint8",
                    "internalType": "uint8"
                },
                {
                    "name": "r",
                    "type": "bytes32",
                    "internalType": "bytes32"
                },
                {
                    "name": "s",
                    "type": "bytes32",
                    "internalType": "bytes32"
                }
            ],
            "outputs": [],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "symbol",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "string",
                    "internalType": "string"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "totalSupply",
            "inputs": [],
            "outputs": [
                {
                    "name": "",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "stateMutability": "view"
        },
        {
            "type": "function",
            "name": "transfer",
            "inputs": [
                {
                    "name": "to",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "function",
            "name": "transferFrom",
            "inputs": [
                {
                    "name": "from",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "to",
                    "type": "address",
                    "internalType": "address"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "internalType": "uint256"
                }
            ],
            "outputs": [
                {
                    "name": "",
                    "type": "bool",
                    "internalType": "bool"
                }
            ],
            "stateMutability": "nonpayable"
        },
        {
            "type": "event",
            "name": "Approval",
            "inputs": [
                {
                    "name": "owner",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "spender",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                }
            ],
            "anonymous": False
        },
        {
            "type": "event",
            "name": "Transfer",
            "inputs": [
                {
                    "name": "from",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "to",
                    "type": "address",
                    "indexed": True,
                    "internalType": "address"
                },
                {
                    "name": "amount",
                    "type": "uint256",
                    "indexed": False,
                    "internalType": "uint256"
                }
            ],
            "anonymous": False
        }
    ]

CTFE_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTFE_EXCHANGE_ABI = [
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "_collateral",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "_ctf",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "_proxyFactory",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "_safeFactory",
            "type": "address"
          }
        ],
        "stateMutability": "nonpayable",
        "type": "constructor"
      },
      {
        "inputs": [],
        "name": "AlreadyRegistered",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "FeeTooHigh",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "InvalidComplement",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "InvalidNonce",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "InvalidSignature",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "InvalidTokenId",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "MakingGtRemaining",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "MismatchedTokenIds",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "NotAdmin",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "NotCrossing",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "NotOperator",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "NotOwner",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "NotTaker",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "OrderExpired",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "OrderFilledOrCancelled",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "Paused",
        "type": "error"
      },
      {
        "inputs": [],
        "name": "TooLittleTokensReceived",
        "type": "error"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "receiver",
            "type": "address"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "tokenId",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "amount",
            "type": "uint256"
          }
        ],
        "name": "FeeCharged",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "newAdminAddress",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "admin",
            "type": "address"
          }
        ],
        "name": "NewAdmin",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "newOperatorAddress",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "admin",
            "type": "address"
          }
        ],
        "name": "NewOperator",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "bytes32",
            "name": "orderHash",
            "type": "bytes32"
          }
        ],
        "name": "OrderCancelled",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "bytes32",
            "name": "orderHash",
            "type": "bytes32"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "maker",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "taker",
            "type": "address"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "makerAssetId",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "takerAssetId",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "makerAmountFilled",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "takerAmountFilled",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "fee",
            "type": "uint256"
          }
        ],
        "name": "OrderFilled",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "bytes32",
            "name": "takerOrderHash",
            "type": "bytes32"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "takerOrderMaker",
            "type": "address"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "makerAssetId",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "takerAssetId",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "makerAmountFilled",
            "type": "uint256"
          },
          {
            "indexed": False,
            "internalType": "uint256",
            "name": "takerAmountFilled",
            "type": "uint256"
          }
        ],
        "name": "OrdersMatched",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "oldProxyFactory",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "newProxyFactory",
            "type": "address"
          }
        ],
        "name": "ProxyFactoryUpdated",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "removedAdmin",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "admin",
            "type": "address"
          }
        ],
        "name": "RemovedAdmin",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "removedOperator",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "admin",
            "type": "address"
          }
        ],
        "name": "RemovedOperator",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "oldSafeFactory",
            "type": "address"
          },
          {
            "indexed": True,
            "internalType": "address",
            "name": "newSafeFactory",
            "type": "address"
          }
        ],
        "name": "SafeFactoryUpdated",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "uint256",
            "name": "token0",
            "type": "uint256"
          },
          {
            "indexed": True,
            "internalType": "uint256",
            "name": "token1",
            "type": "uint256"
          },
          {
            "indexed": True,
            "internalType": "bytes32",
            "name": "conditionId",
            "type": "bytes32"
          }
        ],
        "name": "TokenRegistered",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "pauser",
            "type": "address"
          }
        ],
        "name": "TradingPaused",
        "type": "event"
      },
      {
        "anonymous": False,
        "inputs": [
          {
            "indexed": True,
            "internalType": "address",
            "name": "pauser",
            "type": "address"
          }
        ],
        "name": "TradingUnpaused",
        "type": "event"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "admin_",
            "type": "address"
          }
        ],
        "name": "addAdmin",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "operator_",
            "type": "address"
          }
        ],
        "name": "addOperator",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "name": "admins",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "order",
            "type": "tuple"
          }
        ],
        "name": "cancelOrder",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order[]",
            "name": "orders",
            "type": "tuple[]"
          }
        ],
        "name": "cancelOrders",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "domainSeparator",
        "outputs": [
          {
            "internalType": "bytes32",
            "name": "",
            "type": "bytes32"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "order",
            "type": "tuple"
          },
          {
            "internalType": "uint256",
            "name": "fillAmount",
            "type": "uint256"
          }
        ],
        "name": "fillOrder",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order[]",
            "name": "orders",
            "type": "tuple[]"
          },
          {
            "internalType": "uint256[]",
            "name": "fillAmounts",
            "type": "uint256[]"
          }
        ],
        "name": "fillOrders",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getCollateral",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "token",
            "type": "uint256"
          }
        ],
        "name": "getComplement",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "token",
            "type": "uint256"
          }
        ],
        "name": "getConditionId",
        "outputs": [
          {
            "internalType": "bytes32",
            "name": "",
            "type": "bytes32"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getCtf",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getMaxFeeRate",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "stateMutability": "pure",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "bytes32",
            "name": "orderHash",
            "type": "bytes32"
          }
        ],
        "name": "getOrderStatus",
        "outputs": [
          {
            "components": [
              {
                "internalType": "bool",
                "name": "isFilledOrCancelled",
                "type": "bool"
              },
              {
                "internalType": "uint256",
                "name": "remaining",
                "type": "uint256"
              }
            ],
            "internalType": "struct OrderStatus",
            "name": "",
            "type": "tuple"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getPolyProxyFactoryImplementation",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "_addr",
            "type": "address"
          }
        ],
        "name": "getPolyProxyWalletAddress",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getProxyFactory",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "_addr",
            "type": "address"
          }
        ],
        "name": "getSafeAddress",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getSafeFactory",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "getSafeFactoryImplementation",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "order",
            "type": "tuple"
          }
        ],
        "name": "hashOrder",
        "outputs": [
          {
            "internalType": "bytes32",
            "name": "",
            "type": "bytes32"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "incrementNonce",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "usr",
            "type": "address"
          }
        ],
        "name": "isAdmin",
        "outputs": [
          {
            "internalType": "bool",
            "name": "",
            "type": "bool"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "usr",
            "type": "address"
          }
        ],
        "name": "isOperator",
        "outputs": [
          {
            "internalType": "bool",
            "name": "",
            "type": "bool"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "usr",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "nonce",
            "type": "uint256"
          }
        ],
        "name": "isValidNonce",
        "outputs": [
          {
            "internalType": "bool",
            "name": "",
            "type": "bool"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "takerOrder",
            "type": "tuple"
          },
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order[]",
            "name": "makerOrders",
            "type": "tuple[]"
          },
          {
            "internalType": "uint256",
            "name": "takerFillAmount",
            "type": "uint256"
          },
          {
            "internalType": "uint256[]",
            "name": "makerFillAmounts",
            "type": "uint256[]"
          }
        ],
        "name": "matchOrders",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "name": "nonces",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          },
          {
            "internalType": "uint256[]",
            "name": "",
            "type": "uint256[]"
          },
          {
            "internalType": "uint256[]",
            "name": "",
            "type": "uint256[]"
          },
          {
            "internalType": "bytes",
            "name": "",
            "type": "bytes"
          }
        ],
        "name": "onERC1155BatchReceived",
        "outputs": [
          {
            "internalType": "bytes4",
            "name": "",
            "type": "bytes4"
          }
        ],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          },
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          },
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          },
          {
            "internalType": "bytes",
            "name": "",
            "type": "bytes"
          }
        ],
        "name": "onERC1155Received",
        "outputs": [
          {
            "internalType": "bytes4",
            "name": "",
            "type": "bytes4"
          }
        ],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "name": "operators",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "bytes32",
            "name": "",
            "type": "bytes32"
          }
        ],
        "name": "orderStatus",
        "outputs": [
          {
            "internalType": "bool",
            "name": "isFilledOrCancelled",
            "type": "bool"
          },
          {
            "internalType": "uint256",
            "name": "remaining",
            "type": "uint256"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "parentCollectionId",
        "outputs": [
          {
            "internalType": "bytes32",
            "name": "",
            "type": "bytes32"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "pauseTrading",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "paused",
        "outputs": [
          {
            "internalType": "bool",
            "name": "",
            "type": "bool"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "proxyFactory",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "token",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "complement",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "conditionId",
            "type": "bytes32"
          }
        ],
        "name": "registerToken",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "",
            "type": "uint256"
          }
        ],
        "name": "registry",
        "outputs": [
          {
            "internalType": "uint256",
            "name": "complement",
            "type": "uint256"
          },
          {
            "internalType": "bytes32",
            "name": "conditionId",
            "type": "bytes32"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "admin",
            "type": "address"
          }
        ],
        "name": "removeAdmin",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "operator",
            "type": "address"
          }
        ],
        "name": "removeOperator",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "renounceAdminRole",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "renounceOperatorRole",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "safeFactory",
        "outputs": [
          {
            "internalType": "address",
            "name": "",
            "type": "address"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "_newProxyFactory",
            "type": "address"
          }
        ],
        "name": "setProxyFactory",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "address",
            "name": "_newSafeFactory",
            "type": "address"
          }
        ],
        "name": "setSafeFactory",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "bytes4",
            "name": "interfaceId",
            "type": "bytes4"
          }
        ],
        "name": "supportsInterface",
        "outputs": [
          {
            "internalType": "bool",
            "name": "",
            "type": "bool"
          }
        ],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [],
        "name": "unpauseTrading",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "token",
            "type": "uint256"
          },
          {
            "internalType": "uint256",
            "name": "complement",
            "type": "uint256"
          }
        ],
        "name": "validateComplement",
        "outputs": [],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "order",
            "type": "tuple"
          }
        ],
        "name": "validateOrder",
        "outputs": [],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "bytes32",
            "name": "orderHash",
            "type": "bytes32"
          },
          {
            "components": [
              {
                "internalType": "uint256",
                "name": "salt",
                "type": "uint256"
              },
              {
                "internalType": "address",
                "name": "maker",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "signer",
                "type": "address"
              },
              {
                "internalType": "address",
                "name": "taker",
                "type": "address"
              },
              {
                "internalType": "uint256",
                "name": "tokenId",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "makerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "takerAmount",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "expiration",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "nonce",
                "type": "uint256"
              },
              {
                "internalType": "uint256",
                "name": "feeRateBps",
                "type": "uint256"
              },
              {
                "internalType": "enum Side",
                "name": "side",
                "type": "uint8"
              },
              {
                "internalType": "enum SignatureType",
                "name": "signatureType",
                "type": "uint8"
              },
              {
                "internalType": "bytes",
                "name": "signature",
                "type": "bytes"
              }
            ],
            "internalType": "struct Order",
            "name": "order",
            "type": "tuple"
          }
        ],
        "name": "validateOrderSignature",
        "outputs": [],
        "stateMutability": "view",
        "type": "function"
      },
      {
        "inputs": [
          {
            "internalType": "uint256",
            "name": "tokenId",
            "type": "uint256"
          }
        ],
        "name": "validateTokenId",
        "outputs": [],
        "stateMutability": "view",
        "type": "function"
      }
    ]

CTF_INSERT_STMT = "INSERT INTO ctf_events (timestamp_ms, from_address, function_name, block_number) VALUES ($1, $2, $3, $4);"
CTFE_INSERT_STMT = "INSERT INTO ctfe_events (timestamp_ms, from_address, function_name, block_number) VALUES ($1, $2, $3, $4);"
NEGRISK_INSERT_STMT = "INSERT INTO negrisk_events (timestamp_ms, from_address, function_name, block_number) VALUES ($1, $2, $3, $4);"
USDC_INSERT_STMT = "INSERT INTO usdc_events (timestamp_ms, from_address, function_name, block_number) VALUES ($1, $2, $3, $4);"

class InfuraRPC:
    def __init__(self, log, http_man : HttpManager, db_man: DatabaseManager, infura_key : str):
        self._log = log
        self._db_man = db_man
        self._infura_key = infura_key
        
        # Web3 Contract Setup
        _w3 = Web3()
        checksummed_addr = Web3.to_checksum_address(NegRiskCtfExchange.lower())
        self._negrisk = _w3.eth.contract(address=checksummed_addr, abi=NegRiskCtfExchange_ABI)
        
        checksummed_addr = Web3.to_checksum_address(CTF.lower())
        self._ctf = _w3.eth.contract(address=checksummed_addr, abi=CTF_ABI)

        checksummed_addr = Web3.to_checksum_address(CTFE_EXCHANGE.lower())
        self._ctfe = _w3.eth.contract(address=checksummed_addr, abi=CTFE_EXCHANGE_ABI)
        
        checksummed_addr = Web3.to_checksum_address(Usdc.lower())
        self._usdc = _w3.eth.contract(address=checksummed_addr, abi=Usdc_ABI)

        # --- Database Inserter State ---
        self._ctf_rows: List[Tuple] = []
        self._ctf_signal = asyncio.Event()
        self._ctf_inserter_task: Optional[asyncio.Task] = None

        self._ctfe_rows: List[Tuple] = []
        self._ctfe_signal = asyncio.Event()
        self._ctfe_inserter_task: Optional[asyncio.Task] = None

        self._negrisk_rows: List[Tuple] = []
        self._negrisk_signal = asyncio.Event()
        self._negrisk_inserter_task: Optional[asyncio.Task] = None

        self._usdc_rows: List[Tuple] = []
        self._usdc_signal = asyncio.Event()
        self._usdc_inserter_task: Optional[asyncio.Task] = None

        # --- HTTP Client State (for Concurrency) ---
        self._http_cli = http_man
        self._poster_id: int = 0
        self._max_posters: int = 5
        self._running_poster_tasks: Dict[str, asyncio.Task] = {}

        self._payload = {
            "jsonrpc": "2.0", "id": self._poster_id, "method": "eth_getBlockByNumber",
            "params": ["mario_is_goated", True]
        }
        
        self._http_callbacks = HttpPosterCallbacks(
                next_url=self._next_url,
                next_payload=self._next_payload,
                on_response=self._on_response,
                on_exception=self._on_exception
        )
        self._http_config = HttpTaskConfig(
            base_back_off_s=1.0, max_back_off_s=150.0, back_off_rate=2.0,
            request_break_s=0.2, max_concurrent_requests=1
        )

        # --- WebSocket Client State ---
        self._url = f"wss://polygon-mainnet.infura.io/ws/v3/{infura_key}"
        self._wss_cli = WebSocketManager(
            callbacks=WebSocketTaskCallbacks(
                on_read=self._on_read,
                on_acknowledgement=self._on_acknowledgement,
                on_read_failure=self._on_read_failure,
                on_connect=self._on_connect,
                on_connect_failure=self._on_connect_failure
            ),
            config=WebSocketTaskConfig()
        )
        
        # --- Lifecycle State ---
        self._started = False

    def task_summary(self) -> dict:
        """Returns a summary of the managed tasks."""
        http_stats = {name: self._http_cli.get_stats(name) for name in self._running_poster_tasks}
        return {
            "http_posters": http_stats,
            "websocket_listener": self._wss_cli.get_stats(0)
        }

    async def start(self):
        """Starts the database inserters and the WebSocket listener."""
        if self._started:
            self._log("InfuraRPC module is already running.", "WARNING")
            return

        self._log("Starting InfuraRPC module...", "INFO")

        self._ctf_inserter_task = self._db_man.exec_persistent("ctf_inserter", CTF_INSERT_STMT, self._ctf_rows, self._ctf_signal, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._ctfe_inserter_task = self._db_man.exec_persistent("ctfe_inserter", CTFE_INSERT_STMT, self._ctfe_rows, self._ctfe_signal, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._negrisk_inserter_task = self._db_man.exec_persistent("negrisk_inserter", NEGRISK_INSERT_STMT, self._negrisk_rows, self._negrisk_signal, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)
        self._usdc_inserter_task = self._db_man.exec_persistent("usdc_inserter", USDC_INSERT_STMT, self._usdc_rows, self._usdc_signal, on_success=self._on_insert_success, on_failure=self._on_inserter_failure)

        subscribe_payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}
        self._wss_cli.start_task(0, self._url, orjson.dumps(subscribe_payload))

        self._started = True
        self._log("InfuraRPC module started successfully.", "INFO")

    def stop(self):
        """Stops all running tasks in the module."""
        if not self._started:
            return
        self._log("Stopping InfuraRPC module...", "INFO")
        self._started = False
        self._clean_up()
        self._log("InfuraRPC module stopped.", "INFO")

    def _clean_up(self):
        """Terminates all managed asyncio tasks."""
        self._wss_cli.terminate_task(0)
        for task_name in list(self._running_poster_tasks.keys()):
            self._http_cli.terminate_task(task_name)
        
        self._db_man.terminate_task("ctf_inserter")
        self._db_man.terminate_task("ctfe_inserter")
        self._db_man.terminate_task("negrisk_inserter")
        self._db_man.terminate_task("usdc_inserter")

    def _next_url(self):
        return str(self._poster_id), f"https://polygon-mainnet.infura.io/v3/{self._infura_key}"
    
    def _next_payload(self) -> bytes:
        """Creates a unique payload object for a specific block number."""
        return orjson.dumps(self._payload)

    def _on_response(self, poster_name: str, response_content: bytes, status_code: int, rtt: float, url: str,
                     headers: Optional[str], request_content: Optional[bytes]) -> bool:
        self._log(f"poster_name {poster_name}, poster_id {self._poster_id}, in flight {len(self._running_poster_tasks)}")
        if status_code != 200:
            try:
                request_content_str = request_content.decode('utf-8')
            except:
                request_content_str = "N/A"

            try:
                response_content_str = response_content.decode('utf-8')
            except:
                response_content_str = "N/A"
            
            self._log(f"on_response: bad status {status_code}, url {url}, response {response_content_str[:200]}, request {request_content_str[:200]} and headers {headers}", "WARNING")
            
            return False

        try:
            msg_json = orjson.loads(response_content)
        except Exception as e:
            self._log(f"on_response: failed to parse json: {response_content[:200]} with error {e!r}", "ERROR")
            self._running_poster_tasks.pop(poster_name)
            self._http_cli.terminate_task(poster_name)
            return True

        result = msg_json.get("result")
        if not isinstance(result, dict):
            self._log(f"on_response: 'result' is not a dictionary in response: {str(msg_json)[:200]}", "ERROR")
            self._running_poster_tasks.pop(poster_name)
            self._http_cli.terminate_task(poster_name)
            return True

        block_number_hex = result.get("number")
        if not isinstance(block_number_hex, str):
            self._log(f"on_response: 'number' is not a string in block data: {str(result)[:200]}", "ERROR")
            self._running_poster_tasks.pop(poster_name)
            self._http_cli.terminate_task(poster_name)
            return True

        try:
            block_number_int = int(block_number_hex, 16)
            decimal_timestamp = int(result["timestamp"], 16) * 1000
        except (KeyError, TypeError, ValueError) as e:
            self._log(f"on_response: failed number/timestamp conversion for block {block_number_hex} with '{e}'", "ERROR")
            self._running_poster_tasks.pop(poster_name)
            self._http_cli.terminate_task(poster_name)
            return True

        transactions = result.get("transactions")
        if not isinstance(transactions, list):
            self._log(f"on_response: 'transactions' field is not a list in block {block_number_hex}", "ERROR")
            self._running_poster_tasks.pop(poster_name)
            self._http_cli.terminate_task(poster_name)
            return True

        # for debbuging:
        total_tx = 0
        ctf_tx = 0
        ctf_set = set()
        ctfe_tx = 0
        ctfe_set = set()
        neg_tx = 0
        neg_set = set()
        usdc_tx = 0
        usdc_set = set()

        for tx in transactions:
            if not isinstance(tx, dict):
                continue

            # --- REVERTED to your original, robust validation style ---
            from_addr = tx.get("from")
            if not isinstance(from_addr, str):
                self._log(f"on_response: non-string 'from' in tx for block {block_number_hex}", "WARNING")
                continue
            
            input_data = tx.get("input")
            if not isinstance(input_data, str):
                self._log(f"on_response: non-string 'input' in tx for block {block_number_hex}", "WARNING")
                continue

            to_addr = tx.get("to")
            if not isinstance(to_addr, str):
                # This can be None for contract creation, which is valid, but we can't decode it.
                continue
            
            # --- Individual try/except blocks for each decoder ---
            total_tx +1
            if to_addr == CTF:
                try:
                    func, args = self._ctf.decode_function_input(input_data)
                    self._ctf_rows.append((decimal_timestamp, from_addr, func.fn_name, block_number_int))
                    self._ctf_signal.set()
                    ctf_tx + 1
                    ctf_set.add(func)
                except Exception as e:
                    self._log(f"Failed to decode CTF input in block {block_number_hex}. Error: {e!r}", "WARNING")
            
            elif to_addr == CTFE_EXCHANGE:
                try:
                    func, args = self._ctfe.decode_function_input(input_data)
                    self._ctfe_rows.append((decimal_timestamp, from_addr, func.fn_name, block_number_int))
                    self._ctfe_signal.set()
                    ctfe_tx += 1
                    ctfe_set.add(func)
                except Exception as e:
                    self._log(f"Failed to decode CTFE_EXCHANGE input in block {block_number_hex}. Error: {e!r}", "WARNING")

            elif to_addr == NegRiskCtfExchange:
                try:
                    func, args = self._negrisk.decode_function_input(input_data)
                    self._negrisk_rows.append((decimal_timestamp, from_addr, func.fn_name, block_number_int))
                    self._negrisk_signal.set()
                    neg_tx += 1
                    neg_set.add(func)
                except Exception as e:
                    self._log(f"Failed to decode NegRiskCtfExchange input in block {block_number_hex}. Error: {e!r}", "WARNING")

            elif to_addr == Usdc:
                try:
                    func, args = self._usdc.decode_function_input(input_data)
                    self._usdc_rows.append((decimal_timestamp, from_addr, func.fn_name, block_number_int))
                    self._usdc_signal.set()
                    usdc_tx += 1
                    usdc_set.add(func)
                except Exception as e:
                    self._log(f"Failed to decode Usdc input in block {block_number_hex}. Error: {e!r}", "WARNING")
            elif not isinstance(to_addr, str):
                self._log("on_response: basd to_addr")
            
        self._log(f"on_response: {total_tx} total, {ctf_tx} ctf_tx, {len(ctf_set)} ctf_set, {ctfe_tx} ctfe_tx, {len(ctfe_set)} ctfe_set, {neg_tx} neg_tx, {len(neg_set)} neg_set, {usdc_tx} usdc_tx, {len(usdc_set)} usdc_set", "DEBUG")
        self._running_poster_tasks.pop(poster_name)
        self._http_cli.terminate_task(poster_name)
        return False

    def _on_exception(self, poster_name : str, exception: Exception, url: str, headers: Optional[str],
                      request_content: Optional[bytes], response_content: Optional[bytes]) -> bool:
        
        block_num_info = "unknown block"
        if request_content:
            try:
                payload = orjson.loads(request_content)
                block_num_info = f"block {payload['params'][0]}"
            except Exception:
                pass

        tb_str = traceback.format_exc()
        self._log(f"HTTP exception for {block_num_info}: {exception!r}\nURL: {url}\n{tb_str}", "ERROR")
        return True

    def _on_read_failure(self, task_offset: int, exception: Exception, url: str) -> bool:
        self._log(f"WSS read failure on task {task_offset}: {exception!r}", "ERROR")
        return True

    def _on_connect(self, task_offset: int) -> bool:
        self._log(f"Successfully connected to Infura WebSocket.", "INFO")
        return True

    def _on_connect_failure(self, task_offset: int, exception: Exception, url: str, payload: str) -> bool:
        self._log(f"Failed to connect to Infura WebSocket: {exception!r}", "WARNING")
        return True
    
    def _on_acknowledgement(self, offset: int, ack: str) -> bool:
        try:
            parsed_ack = orjson.loads(ack)
            if not isinstance(parsed_ack, dict):
                self._log(f"on_ack: non-dict ack {str(parsed_ack)[:200]}", "ERROR")

            
            if "error" in parsed_ack:
                self._log(f"Infura subscription failed. Ack: {str(parsed_ack)[:300]}", "ERROR")
            else:
                self._log(f"Infura subscription successful. Ack: {str(parsed_ack)[:300]}", "INFO")
                return True
        except Exception:
            self._log(f"Failed to parse Infura subscription ack: {ack[:200]}", "ERROR")
        
        return False

    def _on_read(self, task_offset: int, message: str) -> bool:
        try:
            msg_json = orjson.loads(message)
            params = msg_json.get("params")
            if not isinstance(params, dict):
                self._log(f"on_read: 'params' is not a dictionary in message: {str(msg_json)[:200]}", "WARNING")
                return True
            
            result = params.get("result")
            if not isinstance(result, dict):
                self._log(f"on_read: 'result' is not a dictionary in message: {str(params)[:200]}", "WARNING")
                return True

            block_num = result.get("number")
            if not isinstance(block_num, str):
                self._log(f"on_read: 'number' is not a string in message: {str(result)[:200]}", "WARNING")
                return True
            
            if len(self._running_poster_tasks) >= self._max_posters:
                to_evict = self._running_poster_tasks.keys()[0]
                self._running_poster_tasks.pop(to_evict)
                self._http_cli.terminate_task(to_evict)
                self._log(f"Max concurrent block fetchers ({self._max_posters}) running. evicted {to_evict}.", "WARNING")
                return True
            
            task_name = str(self._poster_id)
            self._payload["params"][0] = block_num

            task = self._http_cli.start_posting(
                name=task_name, 
                callbacks=self._http_callbacks, 
                config=self._http_config
            )
            
            self._running_poster_tasks[task_name] = task
            self._log(f"on_read: new block {block_num} announced now started poster {self._poster_id}", "DEBUG")
            self._poster_id += 1
        except Exception as e:
            self._log(f"on_read: failed '{e}' with {message[:200]}", "WARNING")
        
        return True

    def _on_insert_success(self, name: str, params: List[Tuple]):
        pass

    def _on_inserter_failure(self, name: str, exception: Exception, params: List[Tuple]):
        self._log(f"CRITICAL: Database inserter '{name}' failed permanently: {exception!r}. Stopping module.", "ERROR")
        self.stop()

        