use crate::traits::Filterable;
use anyhow::{anyhow, Error};
use aptos_protos::transaction::v1::{MoveModuleId, MoveStructTag};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MoveModuleFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Filterable<MoveModuleId> for MoveModuleFilter {
    #[inline]
    fn is_valid(&self) -> Result<(), Error> {
        if self.address.is_none() && self.name.is_none() {
            return Err(anyhow!("At least one of address or name must be set"));
        };
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, module_id: &MoveModuleId) -> bool {
        self.address.is_allowed(&module_id.address) && self.name.is_allowed(&module_id.name)
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MoveStructTagFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Filterable<MoveStructTag> for MoveStructTagFilter {
    #[inline]
    fn is_valid(&self) -> Result<(), Error> {
        if self.address.is_none() && self.module.is_none() && self.name.is_none() {
            return Err(anyhow!(
                "At least one of address, module or name must be set"
            ));
        };
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, struct_tag: &MoveStructTag) -> bool {
        self.address.is_allowed(&struct_tag.address)
            && self.module.is_allowed(&struct_tag.module)
            && self.name.is_allowed(&struct_tag.name)
    }
}
