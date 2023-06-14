from processors.nft_orderbooks.nft_marketplace_enums import MarketplaceName

MARKETPLACE_SMART_CONTRACT_ADDRESSES = {
    MarketplaceName.TOPAZ: set(
        [
            "0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2",
            "0xd4c70815e9d245a097646af821ecf87a073039e71e93c8cd04a0da082134d296",
        ]
    ),
    MarketplaceName.SOUFFLE: set(
        ["0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4"]
    ),
    MarketplaceName.BLUEMOVE: set(
        ["0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e"]
    ),
    MarketplaceName.OKX: set(
        ["0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43"]
    ),
    MarketplaceName.OZOZOZ: set(
        ["0xded0c1249b522cecb11276d2fad03e6635507438fef042abeea3097846090bcd"]
    ),
    MarketplaceName.ITSRARE: set(
        ["0x143f6a7a07c76eae1fc9ea030dbd9be3c2d46e538c7ad61fe64529218ff44bc4"]
    ),
    MarketplaceName.APTOMINGOS_AUCTION: set(
        ["0x98937acca8bc2c164dff158156ab06f9c99bbbb050129d7a514a37ccb1b8e49e"]
    ),
}

MARKETPLACE_SMART_CONTRACT_ADDRESSES_INV = {
    vv: k for k, v in MARKETPLACE_SMART_CONTRACT_ADDRESSES.items() for vv in v
}

MARKETPLACE_ENTRY_FUNCTIONS = {
    MarketplaceName.TOPAZ: set(
        [
            "bid_any::bid",
            "bid_any::cancel_bid",
            "bid_any::sell",
            "collection_marketplace::bid",
            "collection_marketplace::cancel",
            "collection_marketplace::fill",
            "collection_marketplace::fill_many",
            "marketplace_v2::buy",
            "marketplace_v2::buy_many",
            "marketplace_v2::delist",
            "marketplace_v2::list",
            "marketplace_v2::list_many",
            "edit_listing::edit",
        ]
    ),
    MarketplaceName.SOUFFLE: set(
        [
            "FixedPriceMarket::batch_buy_script",
            "FixedPriceMarket::batch_cancel_list_script",
            "FixedPriceMarket::batch_change_price_script",
            "FixedPriceMarket::batch_list_script",
        ]
    ),
    MarketplaceName.BLUEMOVE: set(
        [
            "marketplaceV2::accept_offer",
            "marketplaceV2::accept_offer_collection",
            "marketplaceV2::batch_buy_script",
            "marketplaceV2::batch_delist_script",
            "marketplaceV2::batch_list_script",
            "marketplaceV2::cancel_offer_collection",
            "marketplaceV2::cancel_offer_token",
            "marketplaceV2::change_price_token",
            "marketplaceV2::inittialize_offer",
            "marketplaceV2::inittialize_offer_collection",
        ]
    ),
    MarketplaceName.OKX: set(
        [
            "okx_fixed_price::create_direct_listing",
            "okx_fixed_price::buy_direct_listing",
            "Aggregator::batch_delist_script_V2",
            "markets::list_tokens_v2",
            "markets::buy_tokens_v2",
            "okx_fixed_price::cancel_direct_listing",
            "Aggregator::batch_change_price_script_V2",
        ]
    ),
    MarketplaceName.OZOZOZ: set(
        [
            "OzozozMarketplace::buyNFT",
            "OzozozMarketplace::delist",
            "OzozozMarketplace::buy",
            "OzozozMarketplace::updatePrice",
            "OzozozMarketplace::list",
        ]
    ),
    MarketplaceName.ITSRARE: set(
        [
            "MarketPlace::buy",
            "MarketPlace::delist",
            "MarketPlace::list",
        ]
    ),
}


MARKETPLACE_ADDRESS_MATCH_REGEX_STRINGS = {
    MarketplaceName.TOPAZ: "^0x2c7bccf7b31baf770fdbcc768d9e9cb3d87805e255355df5db32ac9a669010a2.*$",
    MarketplaceName.SOUFFLE: "^0xf6994988bd40261af9431cd6dd3fcf765569719e66322c7a05cc78a89cd366d4.*FixedPriceMarket.*$",
    MarketplaceName.BLUEMOVE: "^0xd1fd99c1944b84d1670a2536417e997864ad12303d19eac725891691b04d614e.*$",
    MarketplaceName.OKX: "^0x1e6009ce9d288f3d5031c06ca0b19a334214ead798a0cb38808485bd6d997a43.*$",
    MarketplaceName.OZOZOZ: "^0xded0c1249b522cecb11276d2fad03e6635507438fef042abeea3097846090bcd.*OzozozMarketplace.*$",
    MarketplaceName.ITSRARE: "^0x143f6a7a07c76eae1fc9ea030dbd9be3c2d46e538c7ad61fe64529218ff44bc4.*$",
    MarketplaceName.APTOMINGOS_AUCTION: "^0x98937acca8bc2c164dff158156ab06f9c99bbbb050129d7a514a37ccb1b8e49e.*$",
}

# Topaz
TOPAZ_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        # trade-related events are by nature also orderbook-related events
        "events::BuyEvent",
        "events::SellEvent",
        "events::FillCollectionBidEvent",
        # these are orderbook-related events
        "events::ListEvent",
        "events::DelistEvent",
        "events::BidEvent",
        "events::CancelBidEvent",
        "events::CollectionBidEvent",
        "events::CancelCollectionBidEvent"
        # unrelated to order book; these are filtered out (included here in comments for documentation)
        # "token_coin_swap::TokenListingEvent" -- redundant with events::ListEvent
        # "token_coin_swap::TokenSwapEvent"    -- redundant with events::BuyEvent
        # "events::SendEvent" -- transfer events do not affect order book
        # "events::ClaimEvent" -- transfer events do not affect order book
    ]
)
TOPAZ_LISTINGS_TABLE_HANDLE = (
    "0xe32a79149395a3cb3611fd30f748be07c49adb10c36e4ff7cda52708e7ac025a"
)
TOPAZ_BID_COIN_STORE_TABLE_HANDLE = (
    "0x094ec7243415588f97de9765562a7057a528180110e10513951e64d27d5f6612"
)
TOPAZ_BIDS_TABLE_HANDLE = (
    "0x9e13f27559a044dcdcbc262e9c9f32a3e21d2bcb15ca1952d8f4d374e3210bdf"
)
TOPAZ_COLLECTION_BIDS_TABLE_HANDLE = (
    "0x8f89a3d01d95119fbbb49e416a04394da369792b620536fcc9aa797589b18e5b"
)

# Souffle
SOUFFLE_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        # trade-related events are by nature also orderbook-related events
        "FixedPriceMarket::BuyTokenEvent",
        # these are orderbook-related events
        "FixedPriceMarket::ListTokenEvent",
        "FixedPriceMarket::CancelListTokenEvent",
    ]
)
SOUFFLE_LISTINGS_TABLE_HANDLE = (
    "0xa8426491baaed45f0d13bdf687e9f55959bdf297b685cbf61c6e968c81380d5f"
)

# Bluemove
BLUEMOVE_MARKETPLACE_EVENT_TYPES = set(
    [
        # these are trade-related events
        "marketplaceV2::BuyEvent",  # purchase
        "offer_lib::AcceptOfferCollectionEvent",  # accept global bid
        "offer_lib::AcceptOfferEvent",  # accept bid
        # these are orderbook related events
        "offer_lib::OfferCollectionEvent",  # global bid
        "offer_lib::CancelOfferCollectionEvent",  # cancelled global bid
        "offer_lib::OfferEvent",  # bid
        "offer_lib::CancelOfferEvent",  # cancelled bid
        "marketplaceV2::ListEvent",  # listing
        "marketplaceV2::ChangePriceEvent",  # change price of listing
        "marketplaceV2::DelistEvent"  # cancel listing
        # # all auctions on bluemove look unsuccessful
        # ,'marketplaceV2::AuctionEvent'
        # ,'marketplaceV2::BidEvent' # bids on unsuccessful auctions
        # # unrelated to order book; these are filtered out (included here in comments for documentation)
        # ,'marketplaceV2::ClaimTokenEvent'
        # ,'marketplaceV2::ClaimCoinsEvent'
        # ,'offer_lib::ClaimTokenOffer'
        # ,'offer_lib::ClaimTokenOfferCollectionEvent'
    ]
)

BLUEMOVE_LISTINGS_TABLE_HANDLE = (
    "0x7ccc0aa7c30736fbc1f4ba34f4e8fab4a5cad3d1bd520af716716087affb67f9"
)

BLUEMOVE_BIDS_TABLE_HANDLE = (
    "0x6e5cefeb3c74d6fbfb559cda6b173024de2a8e3e0598e96c3e090e675a306fd6"
)

BLUEMOVE_COLLECTION_BIDS_TABLE_HANDLE = (
    "0x892b432784f8147bfd1ed4cea039783d48a5a4d317ee14b334518ec6d899e3a5"
)

# Marketplace table handles
MARKETPLACE_TABLE_HANDLES = {
    MarketplaceName.TOPAZ: set(
        [
            TOPAZ_LISTINGS_TABLE_HANDLE,
            TOPAZ_BID_COIN_STORE_TABLE_HANDLE,
            TOPAZ_BIDS_TABLE_HANDLE,
            TOPAZ_COLLECTION_BIDS_TABLE_HANDLE,
        ]
    ),
    MarketplaceName.SOUFFLE: set([SOUFFLE_LISTINGS_TABLE_HANDLE]),
    MarketplaceName.BLUEMOVE: set(
        [
            BLUEMOVE_LISTINGS_TABLE_HANDLE,
            BLUEMOVE_BIDS_TABLE_HANDLE,
            BLUEMOVE_COLLECTION_BIDS_TABLE_HANDLE,
        ]
    ),
}

MARKETPLACE_TABLE_HANDLES_INV = {
    vv: k for k, v in MARKETPLACE_TABLE_HANDLES.items() for vv in v
}
