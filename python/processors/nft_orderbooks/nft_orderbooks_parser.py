from typing import List
from aptos.util.timestamp import timestamp_pb2
from aptos.transaction.testing1.v1 import transaction_pb2
from create_table import Event
import datetime
import re
from enum import Enum
from topaz_parser import parseTopazMarketplaceEvents
from dataclasses import dataclass

INDEXER_NAME = "nft_orderbooks"


class MarketplaceName(Enum):
    TOPAZ = "topaz"
    SOUFFLE = "souffle"
    BLUEMOVE = "bluemove"
    OKX = "okx"
    OZOZOZ = "ozozoz"
    ITSRARE = "itsrare"
    APTOMINGOS_AUCTION = "aptomingos_auction"


MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS = {
    MarketplaceName.TOPAZ: "^0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2.*$",
    MarketplaceName.SOUFFLE: "^0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4.*FixedPriceMarket.*$",
    MarketplaceName.BLUEMOVE: "^0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e.*$",
    MarketplaceName.OKX: "^0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43.*$",
    MarketplaceName.OZOZOZ: "^0xded0c1249b522cecb11276d2fad03e6635507438fef042abeea3097846090bcd.*OzozozMarketplace.*$",
    MarketplaceName.ITSRARE: "^0x143f6a7a07c76eae1fc9ea030dbd9be3c2d46e538c7ad61fe64529218ff44bc4.*$",
    MarketplaceName.APTOMINGOS_AUCTION: "^0x98937acca8bc2c164dff158156ab06f9c99bbbb050129d7a514a37ccb1b8e49e.*$",
}


@dataclass
class MarketplaceEvent:
    # transactionVersion: int
    # eventIndex: int
    # eventType: str
    # standardEventType: str
    # creatorAddress: str
    # collection: str
    # tokenName: str
    # tokenDataID: str
    # collectionID: str
    # price: int
    # amount: int
    # buyer: str
    # seller: str
    # jsonData: str
    # marketplace: str
    # contractAddress: str
    # entryFunctionIDStr: str
    # transactionTimestamp:

    sequenceNumber: int
    creationNumber: int
    accountAddress: str
    # Transaction unique identifier
    transactionVersion: int
    transactionBlockHeight: int
    type: str
    jsonData: str
    eventIndex: int
    contractAddr: str
    func: str


@dataclass
class ParsedMarketplaceEvent:
    jsonData: str
    marketplace: str
    contractAddr: str
    func: str
    transactionVersion: int
    transactionBlockHeight: int
    type2: str
    creator: str
    collection: str
    token: str
    tokenID: str
    bidID: str
    listingID: str
    price: float
    amount: float
    buyer: str
    seller: str


def getMarketplaceEvents(
    transaction: transaction_pb2.Transaction, marketplaceName: MarketplaceName
) -> List[MarketplaceEvent]:
    # Filter out all non-user transactions
    if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
        return []

    userTransaction = transaction.user
    userTransactionRequest = userTransaction.request
    transactionPayload = userTransactionRequest.payload
    entryFunctionPayload = transactionPayload.entry_function_payload
    entryFunctionIDStr = entryFunctionPayload.entry_function_id_str

    contractAddrMatch = re.search("([^:]*)::.*?", entryFunctionIDStr)
    if contractAddrMatch == None:
        contractAddr = ""
    else:
        contractAddr = contractAddrMatch.group(1)

    funcMatch = re.search("[^:]*?::([^:]*::[^:]*)", entryFunctionIDStr)
    if funcMatch == None:
        func = ""
    else:
        func = funcMatch.group(1)

    marketplaceEvents = []
    for event_index, event in enumerate(userTransaction.events):
        eventType = event.type_str
        eventTypeMatch = re.search(
            MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS[marketplaceName], eventType
        )
        if eventTypeMatch == None:
            continue
        marketplaceEvent = MarketplaceEvent(
            sequenceNumber=event.sequence_number,
            creationNumber=event.key.creation_number,
            accountAddress=standardize_address(event.key.account_address),
            transactionVersion=transaction.version,
            transactionBlockHeight=transaction.block_height,
            type=eventType,
            jsonData=event.data,
            eventIndex=event_index,
            contractAddr=contractAddr,
            func=func,
        )
        marketplaceEvents.append(marketplaceEvent)

    return marketplaceEvents


def parse(transaction: transaction_pb2.Transaction) -> List[ParsedMarketplaceEvent]:
    nftActivities = []
    nftListings = []
    currentNFTListings = []
    nftBids = []
    currentNFTBids = []
    nftCollectionBids = []
    currentNFTCollectionBids = []

    parsedMarketplaceEvents = []
    parsedMarketplaceEvents += parseTopazMarketplaceEvents(transaction)

    return parsedMarketplaceEvents


def standardize_address(address: str):
    return "0x" + address.zfill(64)
