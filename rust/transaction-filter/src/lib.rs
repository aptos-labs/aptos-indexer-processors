pub mod filter_operator;
pub mod filters;
pub mod json_search;
pub mod traits;

mod errors;
#[cfg(test)]
pub mod test_lib;

/**
The goal of transaction filtering is to be able to save resources downstream of wherever filtering is used.
For this to be true, the filtering itself must be fast, and so we do a few things:
    1. JSON fields are not parsed. Instead, we treat it as a string, and do simple matching. *This means
        false positives are possible*. This may change in the future, but JSON parsing is not cheap.
    2. We avoid clones, copies, etc as much as possible
    3. We do a single pass over the transaction data

There are four different parts of a transaction that are queryable:
    1. The "root" level. This includes:
        - Transaction type
        - Success
    2. Arbitrary Transaction-type-specific filters. We currently only support the "user" transaction type.
        - Sender
        - Payload: we only support the entry function payload
            - Entry function (address, module, name)
            - Entry function ID string
            - Arbitrary JSON data
    3. Events. Each event has:
        - Key
        - Type
        - Arbitrary JSON data
    4. WriteSet Changes. Each change may have:
        - Type
        - Address
        - Arbitrary JSON data
**/

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
