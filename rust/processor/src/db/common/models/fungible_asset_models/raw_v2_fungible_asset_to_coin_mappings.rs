// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::raw_v2_fungible_metadata::RawFungibleAssetMetadataModel;
use crate::{
    db::common::models::fungible_asset_models::raw_v2_fungible_asset_balances::get_paired_metadata_address,
    schema::fungible_asset_to_coin_mappings, utils::database::DbPoolConnection,
};
use ahash::AHashMap;
use diesel::query_dsl::methods::SelectDsl;
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

pub type FungibleAssetToCoinMappings = AHashMap<String, String>;
pub type FungibleAssetToCoinMappingsForDB = AHashMap<String, RawFungibleAssetToCoinMapping>;

lazy_static!(
    pub static ref FA_TO_COIN_MAPPING: AHashMap<&'static str, &'static str> = vec![
   ("0x000000000000000000000000000000000000000000000000000000000000000a", "0x1::aptos_coin::AptosCoin"),
   ("0x377adc4848552eb2ea17259be928001923efe12271fef1667e2b784f04a7cf3a", "0x7fd500c11216f0fe3095d0c4b8aa4d64a4e2e04f83758462f2b127255643615::thl_coin::THL"),
   ("0x0009da434d9b873b5159e8eeed70202ad22dc075867a7793234fbc981b63e119", "0xe4ccb6d39136469f376242c31b34d10515c8eaaa38092f804db8e08a8f53c5b2::assets_v1::EchoCoin002"),
   ("0xe568e9322107a5c9ba4cbd05a630a5586aa73e744ada246c3efb0f4ce3e295f3", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT"),
   ("0xd4c0be6af89a42d78fb728dd57096eda717d7044c2dd635a01417662c33614fc", "0x55987edfab9a57f69bac759674f139ae473b5e09a9283848c1f87faf6fc1e789::shrimp::ShrimpCoin"),
   ("0xa259be733b6a759909f92815927fa213904df6540519568692caf0b068fe8e62", "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::amapt_token::AmnisApt"),
   ("0xb614bfdf9edc39b330bbf9c3c5bcd0473eee2f6d4e21748629cc367869ece627", "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::stapt_token::StakedApt"),
   ("0x2b3be0a97a73c87ff62cbdd36837a9fb5bbd1d7f06a73b7ed62ec15c5326c1b8", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC"),
   ("0xb27b0c6b60772f0fc804ec1cd3339f552badf9bd1e125a7dd700d8eb11248ef1", "0x73eb84966be67e4697fc5ae75173ca6c35089e802650f75422ab49a8729704ec::coin::DooDoo"),
   ("0x5915ae0eae3701833fa02e28bf530bc01ca96a5f010ac8deecb14c7a92661368", "0x4fbed3f8a3fd8a11081c8b6392152a8b0cb14d70d0414586f0c9b858fcd2d6a7::UPTOS::UPTOS"),
   ("0x6dba1728c73363be1bdd4d504844c40fbb893e368ccbeff1d1bd83497dbc756d", "0xe50684a338db732d8fb8a3ac71c4b8633878bd0193bca5de2ebc852a83b35099::propbase_coin::PROPS"),
   ("0xad18575b0e51dd056e1e082223c0e014cbfe4b13bc55e92f450585884f4cf951", "0x159df6b7689437016108a019fd5bef736bac692b6d4a1f10c941f6fbb9a74ca6::oft::CakeOFT"),
   ("0xae02f68520afd221a5cd6fda6f5500afedab8d0a2e19a916d6d8bc2b36e758db", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WETH"),
   ("0x0a9ce1bddf93b074697ec5e483bc5050bc64cff2acd31e1ccfd8ac8cae5e4abe", "0xfaf4e633ae9eb31366c9ca24214231760926576c7b625313b3688b5e900731f6::staking::StakedThalaAPT"),
   ("0xd1bec63fa44dc3f3f5742c3f3a4afc3baed00505efbe955dfe6e5f9d306c67a5", "0xd0b4efb4be7c3508d9a26a9b5405cf9f860d0b9e5fe2f498b90e68b8d2cedd3e::aptos_launch_token::AptosLaunchToken"),
   ("0xb81588af2f7d291e8e57f673ec74d4a38f0654633ad6bbeb1cfd4bb0550ae0df", "0x4def3d3dee27308886f0a3611dd161ce34f977a9a5de4e80b237225923492a2a::coin::T"),
   ("0x02370cc1d995f3aadd337c1c6c63834ad8d2bd0cdc70bc8dff81de463e18b159", "0x53a30a6e5936c0a4c5140daed34de39d17ca7fcae08f947c02e979cef98a3719::coin::LSD"),
   ("0xbe34691f0388bbca83bafab87399aeb756284e11a2872f1ae74218451cb3899f", "0xdd89c0e695df0692205912fb69fc290418bed0dbe6e4573d744a6d5e6bab6c13::coin::T"),
   ("0x96d1ccca420ebc20fc8af6cacb864e44856ca879c6436d4e9be2b0a4b99bf852", "0x27fafcc4e39daac97556af8a803dbb52bcb03f0821898dc845ac54225b9793eb::move_coin::MoveCoin"),
   ("0x94ed76d3d66cb0b6e7a3ab81acf830e3a50b8ae3cfb9edc0abea635a11185ff4", "0x6f986d146e4a90b828d8c12c14b6f4e003fdff11a8eecceceb63744363eaac01::mod_coin::MOD"),
   ("0xa743351ed4889845737082ab9fcd42c21270647e2c6f342c509320e974035ed2", "0xd11107bdf0d6d7040c6c0bfbdecb6545191fdf13e8d8d259952f53e1713f61b5::staked_coin::StakedAptos"),
   ("0x41944cf1d4dac152d692644944e2cc49ee81fafdfb37abd541d06388ec3f7eda", "0xe88ae9670071da40a9a6b1d97aab8f6f1898fdc3b8f1c1038b492dfad738448b::coin::Donk"),
   ("0x4c3efb98d8d3662352f331b3465c6df263d1a7e84f002844348519614a5fea30", "0x63be1898a424616367e19bbd881f456a78470e123e2770b5b5dcdceb61279c54::movegpt_token::MovegptCoin"),
   ("0x5eea0061714d46da0ccfd088df4fb1a2ea26c2421e592ade9dacc21cdb60e056", "0x1000000fa32d122c18a6a31c009ce5e71674f22d06a581bb0a15575e6addadcc::usda::USDA"),
   ("0x54fc0d5fa5ad975ede1bf8b1c892ae018745a1afd4a4da9b70bb6e5448509fc0", "0x5e156f1207d0ebfa19a9eeff00d62a282278fb8719f4fab3a586a0a2c0fffbea::coin::T"),
   ("0xc448a48da1ed6f6f378bb82ece996be8b5fc8dd1ea851ea2c023de17714dd747", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::UsdtCoin"),
//    ("0x878370592f9129e14b76558689a4b570ad22678111df775befbfcbc9fb3d90ab", "0x5ae6789dd2fec1a9ec9cccfb3acaf12e93d432f0a3a42c92fe1a9d490b7bbc06::mkl_token::MKL"),
   ("0x1912eb1a5f8f0d4c72c1687eb199b7f9d2df34da5931ec96830c557f7abaa0ad", "0x84d7aeef42d38a5ffc3ccef853e1b82e4958659d16a7de736a29c55fbbeb0114::staked_aptos_coin::StakedAptosCoin"),
   ("0xb5f9a9ff6f815150af83b96b15e4f85e4e3b9e92e3fd17a414cd755c4aa49513", "0x7c0322595a73b3fc53bb166f5783470afeb1ed9f46d1176db62139991505dc61::abel_coin::AbelCoin"),
   ("0x6704464238d73a679486420aab91a8a2a01feb9d700e8ba3332aa3e41d3eab62", "0xa2eda21a58856fda86451436513b867c97eecb4ba099da5775520e0f7492e852::coin::T"),
   ("0xd47b65c45f5260c4f3c5de3f32ddaeabf7eab56c9493a7a955dff7f70ba8afaf", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::BusdCoin"),
   ("0x92410a41654236295001f06375afbb1786dbd14bc1c42a33bfcf50204c248bb7", "0xcc8a89c8dce9693d354449f1f73e60e14e347417854f029db5bc8e7454008abb::coin::T"),
   ("0x59c3a92ab1565a14d4133eb2a3418604341d37da47698a0e853c7bb22c342c55", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::DaiCoin"),
   ("0x1fe81b3886ff129d42064f7ee934013de7ef968cb8f47adb5f7210192bcd4a23", "0xc26a8eda1c3ab69a157815183ddda88c89d6758ee491dd1647a70af2907ce074::coin::Chewy"),
   ("0xa64d2d6f5e26daf6a3552f51d4110343b1a8c8046d0a9e72fa4086a337f3236c", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WBTC"),
   ("0x2329a8351b28aa3672329217a949a9ab39d7f24534324c2eeb8b18f69e7a6fb1", "0x4ed27736e724e403f9b4645ffef0ae86fd149503f45b37c428ffabd7e46e5b05::core::RenegadeCoin"),
   ("0xa79e44c5cdf8a0eb835a265a20adab56c8cdf169a2a25daa3b1d71c557b9ec59", "0xd6d6372c8bde72a7ab825c00b9edd35e643fb94a61c55d9d94a9db3010098548::USDC::Coin"),
   ("0xde368b120e750bbb0d8799356ea23511306ff19edd28eed15fe7b6cc72b04388", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDD"),
   ("0x41dfe1fb3d33d4d9d0b460f03ce1c0a6af6520dd8bdc0f204583c4987faf81de", "0x268d4a7a2ad93274edf6116f9f20ad8455223a7ab5fc73154f687e7dbc3e3ec6::LOON::LOON"),
   ("0x4a884be56ef4c11a27162bf30b32e8e3615dcb3df0fc1777c8eb69c1991f34d0", "0xae478ff7d83ed072dbc5e264250e67ef58f57c99d89b447efd8a0a2e8b2be76e::coin::T"),
   ("0xdc5e054538ba5e183d5aa197b01f327cf84aace749dc8fa2fe87bb5ec9bfe35a", "0xe1bfc010d2bdd576036f4c1f3ea7d547f19188f5b78075dd961420d843ee914c::brew_coin::BrewCoin"),
   ("0xd2f2fd4a3df494042cf24c3b8c1316be8bab7ebac228be77cc0f19fcd885c666", "0x1fc2f33ab6b624e3e632ba861b755fd8e61d2c2e6cf8292e415880b4c198224d::apt20::APTS"),
   ("0x9f0de082b2d4506de8b546308d4fd58bdd5ef5981097abeff40635e979874c9e", "0x65957cb717d1ec5e13c003cbad0d20d8f7f95236ea0f8bb8c419e533eda73890::TOAST::TOAST"),
   ("0xc8e09f0daa8f0143318c965b43cecad55eb1a4f26ea57677fcf44c36975fe28c", "0x665d06fcd9c94430099f82973f2a5e5f13142e42fa172e72ce14f51a64bd8ad9::coin_mbx::MBX"),
   ("0x5b5d60e20f3684ce19d3fd3a99ed2b2a8722b043fd67cea80ea4bc0a4749e883", "0x7c2aaaaf3f019bbf7f02beed21fc4ec352cc38272f93cb11e61ec7c89bfe5f4b::xbtc::XBTC"),
   ("0x69ef9f94420a1d287892fb42450ca5777984c1c22cc886407726482480276ec1", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::WbtcCoin"),
   ("0x78d37cff9f42109ce68cd73edb9ef24bb03aad697d7b6449a544701e425befbf", "0x967adbf2e05fe665ab86a3bf2c4acfa39fbf62097963474ef70a0786dae8cfa2::NRUH::NRUH"),
   ("0xa36e2774e4db37934a3e27e2df7b39be5e6fcb4b7840319336fb98ffdf3d613a", "0xc82974034820c34f065f948f1bee473d481bf99fde2d23e981043e5038cb36be::WOOF::Woof"),
   ("0x37bdfd533a28c38ba6f2963e3f2ab881b3826d952ea3d4ca03020e0d2735348d", "0xccc9620d38c4f3991fa68a03ad98ef3735f18d04717cb75d7a1300dd8a7eed75::coin::T"),
   ("0xeb73df9d3ba3fbc2538d2e7f4a2dac9718b48b07f65596a9c7cc84d978e3d6cd", "0x2dcbc03740a6fa2efee926b9df329184cce357d0573bdab09930f4d48e61a4c8::STOS::STOS"),
   ("0xcd70630fb90cab716ab01a7884821f86dceb1bbb09a89683b5c22c5462503f51", "0xdcfa079344261bfde45e7f6281df091743b8d3098bf9e26e1c0212fc5b070621::zaaptos_token::ZaaptosCoin"),
   ("0x1b976ce1a6bf4a37057166f52646c879f37cb0712eb2fd4005e54b9c929b7171", "0x380b3422c7d2c28f29776cf1a234b98bc514b6d8c1cf16e1b9123cb1acb6203::plant_coin::PlantCoin"),
   ("0xfd1f22b455e2d095a1ec497ecc0a11db86173d0271557ca6c16baa9d66466a75", "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::LPToken<0x1::aptos_coin::AptosCoin, 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC>"),
   ("0x0943bf6e5329167f459a4ae7efa93336f49ef08a5aff65f8c70133f0a0725ef2", "0x91b54cb4441c88fa21b7ca5b8b860e8b6fe726c23866bed91999823e65c1026d::GEMKRW::GEMKRW"),
   ("0xf92047adba5ec4a21ad076b19a0c8806b195435696d30dc3f43781d1e6d91563", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::UsdcCoin"),
   ("0xf0876baf6f8c37723f0e9d9c1bbad1ccb49324c228bcc906e2f1f5a9e139eda1", "0xcfea864b32833f157f042618bd845145256b1bf4c0da34a7013b76e42daa53cc::usdy::USDY"),
   ("0xf7833d21f83a19548c81e8fd17d8bde4a6e8cc3fbb1ffb97973e06e261c75dee", "0x5c738a5dfa343bee927c39ebe85b0ceb95fdb5ee5b323c95559614f5a77c47cf::Aptoge::Aptoge"),
   ("0xa4607412abfc37ec0b6fd6e102f5f0b7989f59fd44ff5d374cbe360ffbecdfff", "0xf891d2e004973430cc2bbbee69f3d0f4adb9c7ae03137b4579f7bb9979283ee6::APTOS_FOMO::APTOS_FOMO"),
   ("0x109492c6323a413d605f5768127b11ef28d5805a818b355b0c9ebcb1995fcf81", "0x881ac202b1f1e6ad4efcff7a1d0579411533f2502417a19211cfc49751ddb5f4::coin::MOJO"),
   ("0xfbd6406c12cab2aef728c917a365cdb73883213f74af5e8a46c8fbd77b623ee7", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::WethCoin"),
   ("0xc40443d625f94ddec95a76bcf2534eda394bf67713b93f08eb202026e2aaa66a", "0xdf3d5eb83df80dfde8ceb1edaa24d8dbc46da6a89ae134a858338e1b86a29e38::coin::Returd"),
   ("0x08bbc1e07f934be0862be6df1477dbab54d6ccdf594f1569a64fa2941cbfe368", "0x198e4a77b72cbcac7465e774e99d2ba552053ca57b0759ea3c008433742b4e4f::PEPE::Pepe"),
   ("0x290c792f89a47cd7280e0b9035fa8b2876ab4298f0135d4a2c88e77257681ea1", "0x2778b277644d375721766abfff0df2adca795f6cbae9f02ff1c95ce9adb6ee28::aptos_shiba_coin::AptosShibaCoin"),
   ("0xc73b3f454576b00d4d05393ff427537eda42f791350f30ce1f566448b5798644", "0x66302f3c648890f70ca3fafc42c919483945f3ba155101bc2e149e38a8b93afc::toss_coin::TossCoin"),
   ("0x3fb0cd30095fc85c77eb5cb9edcdbead3cef34e449b1a6f07729282120bc6383", "0x8d87a65ba30e09357fa2edea2c80dbac296e5dec2b18287113500b902942929d::celer_coin_manager::BnbCoin"),
   ("0xa0d9d647c5737a5aed08d2cfeb39c31cf901d44bc4aa024eaa7e5e68b804e011", "0xfaf4e633ae9eb31366c9ca24214231760926576c7b625313b3688b5e900731f6::staking::ThalaAPT"),
   ("0x0dcee4819a7b45113c6e44a157a11866f3366a7c93f79ba5acdf27f6fb8ce301", "0xbe3c4b493632b4d776d56e19d91dcfb34f591f759f6b57f8632d455360da503c::dumdum_coin::DumdumCoin"),
   ("0x23813a24e98215ab541051432b734baecaa3737019a4891c37034f88d9944960", "0x407a220699982ebb514568d007938d2447d33667e4418372ffec1ddb24491b6c::coin::T"),
   ("0xb56df862320ff2dc317e147c870ad09f12711a5e02c6245438f827e6c54188b4", "0x5c738a5dfa343bee927c39ebe85b0ceb95fdb5ee5b323c95559614f5a77c47cf::AptSwap::AptSwapGovernance"),
   ("0xf5d23515c4454652c38219aec5f1a0720207dc1f5d2e5140b94608f9ce821a36", "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"),
   ("0xcc71496adf8086dcc62bbe4d975718f09de8cc42629aacd8df84df514cbac154", "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::LPToken<0x1::aptos_coin::AptosCoin, 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT>"),
   ("0xfad230e7d9df2baf83a68b6f50217ed3c06da593e766970a885965b43b894a04", "0xada35ada7e43e2ee1c39633ffccec38b76ce702b4efc2e60b50f63fbe4f710d8::apetos_token::ApetosCoin"),
   ("0x8d55e255fcf4142a8008b1b5ca0ec9efb964f8a914807c0bf279f6c3af3ef955", "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::LPToken<0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::amapt_token::AmnisApt, 0x1::aptos_coin::AptosCoin>"),
   ("0x66037303c053e2bd0d5af444a9a4792e5a2b56633849e57259e575349d871d04", "0x1eb35b3b9e911ca2093970ae5abfb970dbec54597d43954bb53b09d2e8356cc3::BMTAmm::BMTAmm"),
   ("0xcab64ed0d956462e9b8ba7c340fdb8b9ab52da1503f37b522288bc0c5bf944de", "0x389dbbc6884a1d5b1ab4e1df2913a8c1b01251e50aed377554372b2842c5e3ef::EONcoin::EONCoin"),
   ("0x5486d29c4fceec48c55e88a700eddfdf5be8663a2a873ac0d2baac21cd78b390", "0xacd014e8bdf395fa8497b6d585b164547a9d45269377bdf67c96c541b7fec9ed::coin::T"),
   ("0x9660042a7c01d776938184278381d24c7009ca385d9a59cf9b22691f97615960", "0x16fe2df00ea7dde4a63409201f7f4e536bde7bb7335526a35d05111e68aa322c::AnimeCoin::ANI"),
   ("0x4ddd6e6dfff083e2e4981cf959384e6aec18a9c62cc4694e8aab950c07296208", "0xcc78307c77f1c2c0fdfee17269bfca7876a0b35438c3442417480c0d5c370fbc::AptopadCoin::APD"),
   ("0x8ddd74585cfe3249d71532a8c96fbfc3a1d1134357ec1eaf0411db953c6b942b", "0x31a6675cbe84365bf2b0cbce617ece6c47023ef70826533bde5203d32171dc3c::swap::LPToken<0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::WETH, 0x1::aptos_coin::AptosCoin>"),
   ("0x70003a36f53ed8142a7a530415982d466bc16cdcf2be2599c08211105bd9364d", "0x26f03cd414cdcae387961058ab5523a4e64559f4b0853c46c9b3c4e01fd8af9b::usdy_staging::USDY_STAGING"),
   ("0x64196428c8f492b189a1f69234e12b161adf729fc42679cb30219a59f8114f8c", "0x26f03cd414cdcae387961058ab5523a4e64559f4b0853c46c9b3c4e01fd8af9b::usdy::USDY"),
   ("0x56998a39007a9b431fa9d98bcc57de14f2f357846723a1ad94dfb4c3c965a3a3", "0x14b0ef0ec69f346bea3dfa0c5a8c3942fb05c08760059948f9f24c02cd0d4fd8::mover_token::Mover"),
].iter().cloned().collect();
   );

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(coin_type))]
#[diesel(table_name = fungible_asset_to_coin_mappings)]
pub struct RawFungibleAssetToCoinMapping {
    pub fungible_asset_metadata_address: String,
    pub coin_type: String,
    pub last_transaction_version: i64,
}

impl RawFungibleAssetToCoinMapping {
    pub fn from_raw_fungible_asset_metadata(metadata: &RawFungibleAssetMetadataModel) -> Self {
        let fungible_asset_metadata_address = get_paired_metadata_address(&metadata.asset_type);
        Self {
            fungible_asset_metadata_address,
            coin_type: metadata.asset_type.clone(),
            last_transaction_version: metadata.last_transaction_version,
        }
    }

    /// Get the asset_type_v1 (coin type) from either the constant or the dynamic mapping
    pub fn get_asset_type_v1(
        asset_type_v2: &str,
        fa_to_coin_mapping: Option<&FungibleAssetToCoinMappings>,
    ) -> Option<String> {
        // First check if asset type exists in static mapping
        if let Some(&coin_type) = FA_TO_COIN_MAPPING.get(asset_type_v2) {
            return Some(coin_type.to_string());
        }
        // If not found in static mapping, check dynamic mapping if provided
        fa_to_coin_mapping.and_then(|mapping| mapping.get(asset_type_v2).cloned())
    }

    /// This should be triggered on startup only. After that we will use the mapping in memory.
    pub async fn get_all_mappings(conn: &mut DbPoolConnection<'_>) -> FungibleAssetToCoinMappings {
        match Self::query_all(conn).await {
            Ok(mappings) => {
                let mut result = AHashMap::new();
                for (metadata_address, coin_type) in mappings {
                    result.insert(metadata_address, coin_type);
                }
                result
            },
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    "Failed to query fungible asset to coin mappings"
                );
                panic!("Failed to query fungible asset to coin mappings: {:?}", e);
            },
        }
    }

    async fn query_all(
        conn: &mut DbPoolConnection<'_>,
    ) -> diesel::QueryResult<Vec<(String, String)>> {
        fungible_asset_to_coin_mappings::table
            .select((
                fungible_asset_to_coin_mappings::fungible_asset_metadata_address,
                fungible_asset_to_coin_mappings::coin_type,
            ))
            .load::<(String, String)>(conn)
            .await
    }
}

pub trait FungibleAssetToCoinMappingConvertible {
    fn from_raw(raw: RawFungibleAssetToCoinMapping) -> Self;
}
