use crate::{errors::FilterError, traits::Filterable};
use memchr::memmem::Finder;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/**
Find multiple needles in a haystack. JSON parsing is relatively expensive, so we
    instead treat the JSON as a string and do simple matching.
    This means false positives are possible.

Currently, we use the `memchr` crate, which is a SIMD-accelerated library for finding bytes in bytes.
    This is faster than the naive `str::find`, however it does repeat work for each needle.
    There are alternatives, such as Aho-Corasick algorithm (https://github.com/BurntSushi/aho-corasick),
    which uses a trie-based for finding multiple needles in a haystack in a single pass.
    This is a good candidate for future work.

Given we serialize some number types as integers, and some others as strings, we have to search for both options.
This approach means:
    1. It's impossible to specify _where_ in the JSON the key/value pair is. I.e a search for a key of "address"
        and value of "0x5" will match both `{"address": "0x5"}`, and `{"inner": {"address": "0x5"}}`.
    2. The above means false positives are clearly possible. Depending on ecosystem feedback and overall performance,
        we may change this, and offer full json support. There are SIMD accelerated JSON parsers available, such as
        https://github.com/cloudwego/sonic-rs or https://github.com/simd-lite/simd-json . Benchmarks will be needed to
        determine if this is worth it.
*/

pub const MIN_KEY_TERM_LENGTH: usize = 3;

#[derive(Error, Debug)]
pub enum JSONSearchError {
    #[error("The json type `{0}` is not supported in searches")]
    UnsupportedJsonType(&'static str),
    #[error(
        "The key name is too short, must be at least {} characters",
        MIN_KEY_TERM_LENGTH
    )]
    KeyNameTooShort,
    #[error("Invalid JSON key term")]
    InvalidKeyTerm,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SerializedJsonSearchTerm {
    pub key: String,
    pub value: serde_json::Value,
}

impl TryInto<JsonSearchTerm> for SerializedJsonSearchTerm {
    type Error = JSONSearchError;

    fn try_into(self) -> Result<JsonSearchTerm, Self::Error> {
        JsonSearchTerm::new(self.key, self.value)
    }
}

impl From<JsonSearchTerm> for SerializedJsonSearchTerm {
    fn from(term: JsonSearchTerm) -> Self {
        term.original_term
    }
}

// Custom serde serialization/deserialization that uses above tryinto and from implementations
impl Serialize for JsonSearchTerm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        SerializedJsonSearchTerm::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JsonSearchTerm {
    fn deserialize<D>(deserializer: D) -> Result<JsonSearchTerm, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let term = SerializedJsonSearchTerm::deserialize(deserializer)?;
        term.try_into().map_err(serde::de::Error::custom)
    }
}

#[derive(Clone)]
pub struct JsonSearchTerm {
    original_term: SerializedJsonSearchTerm,
    // We use an owned finder; while this clones the string once, this is only done once at instantiation
    // Because we serialize some numbers as strings, and some as integers, we need to search for both
    key_finders: Vec<Finder<'static>>,
}

impl PartialEq for JsonSearchTerm {
    fn eq(&self, other: &Self) -> bool {
        self.original_term == other.original_term
    }
}

impl std::fmt::Debug for JsonSearchTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = &self
            .key_finders
            .iter()
            .map(|f| String::from_utf8_lossy(f.needle()));
        f.debug_struct("JsonSearchTerm")
            .field("key_finders", keys)
            .field("original_term", &self.original_term)
            .finish()
    }
}

impl JsonSearchTerm {
    pub fn new(key: String, value: serde_json::Value) -> Result<Self, JSONSearchError> {
        if key.len() < MIN_KEY_TERM_LENGTH {
            Err(JSONSearchError::KeyNameTooShort)?;
        }
        let values = match value.clone() {
            // We use to_string here to we get the quoted JSON value
            serde_json::Value::String(s) => vec![double_encode_json_string(s)],
            // For numbers, we need to search for both the string and the number.
            // TODO: There is probably a better way to do this
            serde_json::Value::Number(n) => vec![n.to_string(), format!("\\\"{}\\\"", n)],
            serde_json::Value::Bool(b) => vec![b.to_string()],
            serde_json::Value::Null => vec!["null".to_string()],
            // Maybe we'll support these in the future, but it's more complicated, so for now we don't
            // TODO: reconsider supporting arrays and/or other types
            serde_json::Value::Array(_) => Err(JSONSearchError::UnsupportedJsonType("Array"))?,
            serde_json::Value::Object(_) => Err(JSONSearchError::UnsupportedJsonType("Object"))?,
        };

        // We need to account for the fact that the key is quoted in the JSON: so we double quote.
        let encoded_key = double_encode_json_string(key.clone());
        // And we append the `:` to the key, and the value
        let key_finders = values
            .iter()
            .map(|v| Finder::new(&format!("{}:{}", encoded_key, v)).into_owned())
            .collect();
        Ok(Self {
            key_finders,
            original_term: SerializedJsonSearchTerm { key, value },
        })
    }

    pub fn from_json(json: serde_json::Value) -> Result<Self, JSONSearchError> {
        let key = json["key"]
            .as_str()
            .ok_or(JSONSearchError::InvalidKeyTerm)?;
        let value = json["value"].clone();
        Self::new(key.to_string(), value)
    }

    pub fn find(&self, haystack: &str) -> bool {
        let haystack_bytes = haystack.as_bytes();
        self.key_finders
            .iter()
            .any(|finder| finder.find(haystack_bytes).is_some())
    }
}

impl Filterable<String> for JsonSearchTerm {
    fn validate_state(&self) -> Result<(), FilterError> {
        // Validation is performed elsewhere
        Ok(())
    }

    fn is_allowed(&self, item: &String) -> bool {
        self.find(item)
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum JsonOrStringSearch {
    Json(JsonSearchTerm),
    String(String),
}

impl Filterable<String> for JsonOrStringSearch {
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            JsonOrStringSearch::Json(json) => json.is_valid(),
            JsonOrStringSearch::String(_) => Ok(()),
        }
    }

    fn is_allowed(&self, item: &String) -> bool {
        match self {
            JsonOrStringSearch::Json(json) => json.is_allowed(item),
            JsonOrStringSearch::String(s) => item == s,
        }
    }
}

pub fn double_encode_json_string(s: String) -> String {
    let s = serde_json::Value::String(s.to_string()).to_string();
    let s = serde_json::Value::String(s).to_string();
    // Then we remove the leading and trailing quotes
    s[1..s.len() - 1].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{borrow::Cow, str::FromStr};

    #[test]
    fn test_double_encode_json_string() {
        let s = "hello";
        let expected = "\\\"hello\\\"";
        assert_eq!(expected, double_encode_json_string(s.to_string()));
    }

    fn print_search_debug(needle: &JsonSearchTerm) {
        let needles_str = needle
            .key_finders
            .iter()
            .map(|f| String::from_utf8_lossy(f.needle()))
            .collect::<Vec<Cow<str>>>()
            .join(", ");
        println!("Needles: {}", needles_str);
    }

    fn search_test_case(test_json: &serde_json::Value, key: &str, value: serde_json::Value) {
        let test_json = serde_json::Value::String(test_json.to_string()).to_string();

        println!(
            "\n==== Searching for `{}: {}` in `{}` ====",
            key, value, test_json
        );

        let needle = JsonSearchTerm::new(key.into(), value).unwrap();
        print_search_debug(&needle);
        println!("Haystack: {}", test_json);
        assert!(needle.find(&test_json), "Failed to find needle in haystack");
    }

    #[test]
    fn test_json_search() {
        let test_json = json!(
            {
              // String and nested string
              "address": "0x3",
              "inner": {
                "b": "c",
                "address": "0x5"
              },

              // Null
              "nullval": null,

              // Numbers
              "somenum": 5,
              "bignum": "101",

               // Bools
              "trueval": true,
              "falseval": false,
            }
        );

        // String and nested string
        search_test_case(
            &test_json,
            "address",
            serde_json::Value::String("0x3".into()),
        );

        search_test_case(
            &test_json,
            "address",
            serde_json::Value::String("0x5".into()),
        );

        // Null
        search_test_case(&test_json, "nullval", serde_json::Value::Null);

        // Numbers
        search_test_case(&test_json, "somenum", serde_json::Value::Number(5.into()));
        search_test_case(
            &test_json,
            "bignum",
            serde_json::Value::String("101".into()),
        );

        // Bools
        search_test_case(&test_json, "trueval", serde_json::Value::Bool(true));
        search_test_case(&test_json, "falseval", serde_json::Value::Bool(false));
    }

    /**
    For searching `inner.address: 0x5` in a singly nested json, the results are:
    ```text
    BENCH: Memchr took 19.75µs for 1000 iters (19ns each)
    BENCH: Serde Search took 1.82875ms for 1000 iters (1.828µs each)
    Memchr is 96x faster than Serde
    ```

    If we double that json- i.e add an inner2 with the contents of test_json, we get:
    ```text
    BENCH: Memchr took 54.334µs for 1000 iters (54ns each)
    BENCH: Serde Search took 14.213292ms for 1000 iters (14.213µs each)
    Memchr is 263x faster than Serde
    ```

    This is excluding memory allocation, for which serde Value is [not great](https://github.com/serde-rs/json/issues/635)

    if we look at something like graffio txns, we’d be looking at more than three orders of magnitude in difference
    The main problem/optimization is that memchr does a tiny bit of work on startup (few ns), but is then re-used forever;
    as long as the stream remains, the per json search is relatively constant, because it’s just so fast, and our jsons are relatively small

    Serde however is not: it scales pretty linearly (and then some), and so the larger the json, the bigger the delta

    Whether we care about an extra 25-50ms per batch is a different story and this is, of course, with serde;
    it’s possible using one of the more efficient json parsers I looked into we could shave, maybe, [20-30% off that time](https://github.com/serde-rs/json-benchmark)
    **/
    #[test]
    fn test_bench_json_search_vs_serde() {
        let mut test_json = json!(
            {
              // String and nested string
              "address": "0x3",
              "inner": {
                "b": "c",
                "address": "0x5"
              },

              // Null
              "nullval": null,

              // Numbers
              "somenum": 5,
              "bignum": "101",

               // Bools
              "trueval": true,
              "falseval": false,
            }
        );

        let test_json_clone = test_json.clone();
        test_json
            .get_mut("inner")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("inner2".to_string(), test_json_clone);

        let test_json_encoded = serde_json::Value::String(test_json.to_string()).to_string();

        let needle =
            JsonSearchTerm::new("address".into(), serde_json::Value::String("0x5".into())).unwrap();

        let start = std::time::Instant::now();

        const ITERATIONS: usize = 1000;
        for _ in 0..ITERATIONS {
            needle.find(&test_json_encoded);
        }

        let elapsed = start.elapsed();
        let memchr_average = elapsed / ITERATIONS as u32;
        println!(
            "BENCH: Memchr took {:?} for {} iters ({:?} each)",
            elapsed, ITERATIONS, memchr_average
        );

        let json_search_term = ["inner".to_string(), "address".to_string()];
        let json_search_value = serde_json::Value::String("0x5".into());
        for _ in 0..ITERATIONS {
            let test_json_serval = serde_json::Value::from_str(&test_json_encoded).unwrap();
            let test_json_serval =
                serde_json::Value::from_str(test_json_serval.as_str().unwrap()).unwrap();

            if !test_json_serval.is_object() {
                panic!("Expected object");
            }
            let mut current = &test_json_serval;
            for key in json_search_term.iter() {
                if let Some(next) = current.get(key) {
                    current = next;
                } else {
                    break;
                }
            }

            // Ensure we found the value
            if current != &json_search_value {
                panic!(
                    "Failed to find needle in haystack: \n{:} \n{:} \n<<<",
                    current, json_search_value
                );
            }
        }
        let elapsed = start.elapsed();
        let serde_average = elapsed / ITERATIONS as u32;
        println!(
            "BENCH: Serde Search took {:?} for {} iters ({:?} each)",
            elapsed, ITERATIONS, serde_average
        );

        println!(
            "Memchr is {:?}x faster than Serde",
            serde_average.as_nanos() / memchr_average.as_nanos()
        );
    }
}
