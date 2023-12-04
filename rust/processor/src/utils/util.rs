// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::models::property_map::{PropertyMap, TokenObjectPropertyMap};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    // assume the format of {“map”: {“data”: [{“key”: “Yuri”, “value”: {“type”: “String”, “value”: “0x42656e”}}, {“key”: “Tarded”, “value”: {“type”: “String”, “value”: “0x446f766572"}}]}}
    // if successfully parsing we return the decoded property_map string otherwise return the original string
    Ok(convert_bcs_propertymap(s.clone()).unwrap_or(s))
}

/// convert the bcs encoded inner value of property_map to its original value in string format
pub fn deserialize_token_object_property_map_from_bcs_hexstring<'de, D>(
    deserializer: D,
) -> core::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let s = serde_json::Value::deserialize(deserializer)?;
    // iterate the json string to convert key-value pair
    Ok(convert_bcs_token_object_propertymap(s.clone()).unwrap_or(s))
}

/// Convert the json serialized PropertyMap's inner BCS fields to their original value in string format
pub fn convert_bcs_propertymap(s: Value) -> Option<Value> {
    match PropertyMap::from_bcs_encode_str(s) {
        Some(e) => match serde_json::to_value(&e) {
            Ok(val) => Some(val),
            Err(_) => None,
        },
        None => None,
    }
}

pub fn convert_bcs_token_object_propertymap(s: Value) -> Option<Value> {
    match TokenObjectPropertyMap::from_bcs_encode_str(s) {
        Some(e) => match serde_json::to_value(&e) {
            Ok(val) => Some(val),
            Err(_) => None,
        },
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_processor_sdk::utils::deserialize_string_from_hexstring;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, Debug)]
    struct TypeInfoMock {
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub module_name: String,
        #[serde(deserialize_with = "deserialize_string_from_hexstring")]
        pub struct_name: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenDataMock {
        #[serde(deserialize_with = "deserialize_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct TokenObjectDataMock {
        #[serde(deserialize_with = "deserialize_token_object_property_map_from_bcs_hexstring")]
        pub default_properties: serde_json::Value,
    }

    #[test]
    fn test_deserialize_property_map() {
        let test_property_json = r#"
        {
            "map":{
               "data":[
                  {
                     "key":"type",
                     "value":{
                        "type":"0x1::string::String",
                        "value":"0x06646f6d61696e"
                     }
                  },
                  {
                     "key":"creation_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x140f4f6300000000"
                     }
                  },
                  {
                     "key":"expiration_time_sec",
                     "value":{
                        "type":"u64",
                        "value":"0x9442306500000000"
                     }
                  }
               ]
            }
        }"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["type"], "domain");
        assert_eq!(d.default_properties["creation_time_sec"], "1666125588");
        assert_eq!(d.default_properties["expiration_time_sec"], "1697661588");
    }

    #[test]
    fn test_empty_property_map() {
        let test_property_json = r#"{"map": {"data": []}}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }

    #[test]
    fn test_deserialize_token_object_property_map() {
        let test_property_json = r#"
        {
            "data": [{
                    "key": "Rank",
                    "value": {
                        "type": 9,
                        "value": "0x0642726f6e7a65"
                    }
                },
                {
                    "key": "address_property",
                    "value": {
                        "type": 7,
                        "value": "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
                    }
                },
                {
                    "key": "bytes_property",
                    "value": {
                        "type": 8,
                        "value": "0x0401020304"
                    }
                },
                {
                    "key": "u64_property",
                    "value": {
                        "type": 4,
                        "value": "0x0000000000000001"
                    }
                }
            ]
        }
        "#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties["Rank"], "Bronze");
        assert_eq!(
            d.default_properties["address_property"],
            "0x2b4d540735a4e128fda896f988415910a45cab41c9ddd802b32dd16e8f9ca3cd"
        );
        assert_eq!(d.default_properties["bytes_property"], "0x01020304");
        assert_eq!(d.default_properties["u64_property"], "72057594037927936");
    }

    #[test]
    fn test_empty_token_object_property_map() {
        let test_property_json = r#"{"data": []}"#;
        let test_property_json: serde_json::Value =
            serde_json::from_str(test_property_json).unwrap();
        let test_struct = TokenObjectDataMock {
            default_properties: test_property_json,
        };
        let val = serde_json::to_string(&test_struct).unwrap();
        let d: TokenObjectDataMock = serde_json::from_str(val.as_str()).unwrap();
        assert_eq!(d.default_properties, Value::Object(serde_json::Map::new()));
    }
}
