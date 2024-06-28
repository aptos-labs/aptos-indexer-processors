use dashmap::DashSet;

#[derive(Clone, Debug, Default)]
pub struct EventFilter {
    pub accounts: DashSet<String>,
    pub types: DashSet<String>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            accounts: DashSet::new(),
            types: DashSet::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.types.is_empty()
    }
}
