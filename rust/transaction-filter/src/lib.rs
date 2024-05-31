pub mod filters;
pub mod traits;

/**
The goal of transaction filtering is to be able to save resources downstream of wherever filtering is used.
For this to be true, the filtering itself must be fast, and so we do a few things:
    1. We avoid clones, copies, etc as much as possible
    2. We do a single pass over the transaction data

There are four different parts of a transaction that are queryable:
    1. The "root" level. This includes:
        - Transaction type
        - Success
    2. Arbitrary Transaction-type-specific filters. We currently only support the "user" transaction type.
        - Sender
        - Payload: we only support the entry function payload
            - Entry function (address, module, name)
            - Entry function ID string
    3. Events. Each event has:
        - Key
        - Type
    4. WriteSet Changes. Each change may have:
        - Type
        - Address
**/

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
