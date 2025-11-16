import json
from web3 import Web3
from web3.logs import DISCARD 

CTF_EXCHANGE_ADDRESS = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

CTF_EXCHANGE_EVENTS_ABI = """
[
    {
  "type": "event",
  "name": "FeeCharged",
  "inputs": [
    {
      "name": "receiver",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "tokenId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewAdmin",
  "inputs": [
    {
      "name": "newAdminAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewOperator",
  "inputs": [
    {
      "name": "newOperatorAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrderCancelled",
  "inputs": [
    {
      "name": "orderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrderFilled",
  "inputs": [
    {
      "name": "orderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "maker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "taker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "makerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "makerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "fee",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrdersMatched",
  "inputs": [
    {
      "name": "takerOrderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "takerOrderMaker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "makerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "makerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "ProxyFactoryUpdated",
  "inputs": [
    {
      "name": "oldProxyFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newProxyFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedAdmin",
  "inputs": [
    {
      "name": "removedAdmin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedOperator",
  "inputs": [
    {
      "name": "removedOperator",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "SafeFactoryUpdated",
  "inputs": [
    {
      "name": "oldSafeFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newSafeFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TokenRegistered",
  "inputs": [
    {
      "name": "token0",
      "type": "uint256",
      "indexed": true,
      "internalType": "uint256"
    },
    {
      "name": "token1",
      "type": "uint256",
      "indexed": true,
      "internalType": "uint256"
    },
    {
      "name": "conditionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TradingPaused",
  "inputs": [
    {
      "name": "pauser",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TradingUnpaused",
  "inputs": [
    {
      "name": "pauser",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}]
"""


negRiskCtfExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

NEG_EXCHANGE_EVENTS_ABI = """[
{
  "type": "event",
  "name": "FeeCharged",
  "inputs": [
    {
      "name": "receiver",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "tokenId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewAdmin",
  "inputs": [
    {
      "name": "newAdminAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewOperator",
  "inputs": [
    {
      "name": "newOperatorAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrderCancelled",
  "inputs": [
    {
      "name": "orderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrderFilled",
  "inputs": [
    {
      "name": "orderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "maker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "taker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "makerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "makerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "fee",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OrdersMatched",
  "inputs": [
    {
      "name": "takerOrderHash",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "takerOrderMaker",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "makerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAssetId",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "makerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "takerAmountFilled",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "ProxyFactoryUpdated",
  "inputs": [
    {
      "name": "oldProxyFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newProxyFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedAdmin",
  "inputs": [
    {
      "name": "removedAdmin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedOperator",
  "inputs": [
    {
      "name": "removedOperator",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "SafeFactoryUpdated",
  "inputs": [
    {
      "name": "oldSafeFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newSafeFactory",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TokenRegistered",
  "inputs": [
    {
      "name": "token0",
      "type": "uint256",
      "indexed": true,
      "internalType": "uint256"
    },
    {
      "name": "token1",
      "type": "uint256",
      "indexed": true,
      "internalType": "uint256"
    },
    {
      "name": "conditionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TradingPaused",
  "inputs": [
    {
      "name": "pauser",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "TradingUnpaused",
  "inputs": [
    {
      "name": "pauser",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}]
"""


negRiskOperator =  "0x71523d0f655B41E805Cec45b17163f528B59B820"

NEG_OPERATOR_EVENTS_ABI = """[
{
  "type": "event",
  "name": "MarketPrepared",
  "inputs": [
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "feeBips",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "data",
      "type": "bytes",
      "indexed": false,
      "internalType": "bytes"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newAdminAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionEmergencyResolved",
  "inputs": [
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "result",
      "type": "bool",
      "indexed": false,
      "internalType": "bool"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionFlagged",
  "inputs": [
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionPrepared",
  "inputs": [
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "requestId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "questionIndex",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "data",
      "type": "bytes",
      "indexed": false,
      "internalType": "bytes"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionReported",
  "inputs": [
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "requestId",
      "type": "bytes32",
      "indexed": false,
      "internalType": "bytes32"
    },
    {
      "name": "result",
      "type": "bool",
      "indexed": false,
      "internalType": "bool"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionResolved",
  "inputs": [
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "result",
      "type": "bool",
      "indexed": false,
      "internalType": "bool"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionUnflagged",
  "inputs": [
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "removedAdmin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}]
"""


negRiskAdapter = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

neg_adapter_events_abi = """[
{
  "type": "event",
  "name": "MarketPrepared",
  "inputs": [
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "oracle",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "feeBips",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "data",
      "type": "bytes",
      "indexed": false,
      "internalType": "bytes"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newAdminAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "OutcomeReported",
  "inputs": [
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "outcome",
      "type": "bool",
      "indexed": false,
      "internalType": "bool"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "PayoutRedemption",
  "inputs": [
    {
      "name": "redeemer",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "conditionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "amounts",
      "type": "uint256[]",
      "indexed": false,
      "internalType": "uint256[]"
    },
    {
      "name": "payout",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "PositionSplit",
  "inputs": [
    {
      "name": "stakeholder",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "conditionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "PositionsConverted",
  "inputs": [
    {
      "name": "stakeholder",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "indexSet",
      "type": "uint256",
      "indexed": true,
      "internalType": "uint256"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "PositionsMerge",
  "inputs": [
    {
      "name": "stakeholder",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "conditionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "QuestionPrepared",
  "inputs": [
    {
      "name": "marketId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "questionId",
      "type": "bytes32",
      "indexed": true,
      "internalType": "bytes32"
    },
    {
      "name": "index",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "data",
      "type": "bytes",
      "indexed": false,
      "internalType": "bytes"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "removedAdmin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
]
"""


negRiskFeeModule = "0x78769D50Be1763ed1CA0D5E878D93f05aabff29e"

negriskfee_abi = """[
{
  "type": "event",
  "name": "FeeRefunded",
  "inputs": [
    {
      "name": "token",
      "type": "address",
      "indexed": false,
      "internalType": "address"
    },
    {
      "name": "to",
      "type": "address",
      "indexed": false,
      "internalType": "address"
    },
    {
      "name": "id",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "FeeWithdrawn",
  "inputs": [
    {
      "name": "token",
      "type": "address",
      "indexed": false,
      "internalType": "address"
    },
    {
      "name": "to",
      "type": "address",
      "indexed": false,
      "internalType": "address"
    },
    {
      "name": "id",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "NewAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "newAdminAddress",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "RemovedAdmin",
  "inputs": [
    {
      "name": "admin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "removedAdmin",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    }
  ],
  "anonymous": false
}
]
"""

negRiskWrappedCollateral = "0x3A3BD7bb9528E159577F7C2e685CC81A765002E2"

negrisk_wc = """[{
  "type": "event",
  "name": "Approval",
  "inputs": [
    {
      "name": "owner",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "spender",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
{
  "type": "event",
  "name": "Transfer",
  "inputs": [
    {
      "name": "from",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "to",
      "type": "address",
      "indexed": true,
      "internalType": "address"
    },
    {
      "name": "amount",
      "type": "uint256",
      "indexed": false,
      "internalType": "uint256"
    }
  ],
  "anonymous": false
}
]"""


negRiskUmaCtfAdapter = "0x2F5e3684cb1F318ec51b00Edba38d79Ac2c0aA9d"

negrisk_uma = """[
]"""

