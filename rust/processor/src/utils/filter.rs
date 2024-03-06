use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct EventFilter {
    pub accounts: HashSet<String>,
    pub types: HashSet<String>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            accounts: HashSet::new(),
            types: HashSet::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.types.is_empty()
    }
}
