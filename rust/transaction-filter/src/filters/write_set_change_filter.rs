use crate::{
    errors::FilterError,
    filters::MoveStructTagFilter,
    json_search::{JsonOrStringSearch, JsonSearchTerm},
    traits::Filterable,
};
use anyhow::Error;
use aptos_protos::transaction::v1::{
    write_set_change::Change, DeleteModule, DeleteResource, DeleteTableItem, WriteModule,
    WriteResource, WriteSetChange, WriteTableItem,
};
use serde::{Deserialize, Serialize};

/// This is a wrapper around ChangeItemFilter, which differs because:
/// While `ChangeItemFilter` will return false if the Event does not match the filter,
/// `ChangeItemFilter` will return true- i.e `WriteSetChangeFilter` *only* tries to match if the
/// change type matches its internal change type
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteSetChangeFilter {
    // TODO: handle actual changes!!!
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change: Option<ChangeItemFilter>,
}

impl Filterable<WriteSetChange> for WriteSetChangeFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), FilterError> {
        if self.change.is_none() {
            return Err(Error::msg("field change must be set").into());
        };
        self.change.is_valid()?;
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &WriteSetChange) -> bool {
        if let Some(change_filter) = &self.change {
            if let Some(change) = item.change.as_ref() {
                match change {
                    Change::DeleteModule(dm) => {
                        if let ChangeItemFilter::ModuleChange(mcf) = change_filter {
                            if !mcf.is_allowed(&ModuleChange::DeleteModule(dm)) {
                                return false;
                            }
                        }
                    },
                    Change::WriteModule(wm) => {
                        if let ChangeItemFilter::ModuleChange(mcf) = change_filter {
                            if !mcf.is_allowed(&ModuleChange::WriteModule(wm)) {
                                return false;
                            }
                        }
                    },
                    Change::DeleteResource(dr) => {
                        if let ChangeItemFilter::ResourceChange(rcf) = change_filter {
                            if !rcf.is_allowed(&ResourceChange::DeleteResource(dr)) {
                                return false;
                            }
                        }
                    },
                    Change::WriteResource(wr) => {
                        if let ChangeItemFilter::ResourceChange(rcf) = change_filter {
                            if !rcf.is_allowed(&ResourceChange::WriteResource(wr)) {
                                return false;
                            }
                        }
                    },
                    Change::DeleteTableItem(dti) => {
                        if let ChangeItemFilter::TableChange(tcf) = change_filter {
                            if !tcf.is_allowed(&TableChange::DeleteTableItem(dti)) {
                                return false;
                            }
                        }
                    },
                    Change::WriteTableItem(wti) => {
                        if let ChangeItemFilter::TableChange(tcf) = change_filter {
                            if !tcf.is_allowed(&TableChange::WriteTableItem(wti)) {
                                return false;
                            }
                        }
                    },
                }
            } else {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum ChangeItemFilter {
    ResourceChange(ResourceChangeFilter),
    ModuleChange(ModuleChangeFilter),
    TableChange(TableChangeFilter),
}

impl Filterable<Change> for ChangeItemFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), FilterError> {
        match self {
            ChangeItemFilter::ResourceChange(rcf) => rcf.is_valid(),
            ChangeItemFilter::ModuleChange(mcf) => mcf.is_valid(),
            ChangeItemFilter::TableChange(tcf) => tcf.is_valid(),
        }
    }

    #[inline]
    fn is_allowed(&self, item: &Change) -> bool {
        match item {
            Change::DeleteModule(dm) => {
                if let ChangeItemFilter::ModuleChange(mcf) = self {
                    return mcf.is_allowed(&ModuleChange::DeleteModule(dm));
                }
                false
            },
            Change::WriteModule(wm) => {
                if let ChangeItemFilter::ModuleChange(mcf) = self {
                    return mcf.is_allowed(&ModuleChange::WriteModule(wm));
                }
                false
            },
            Change::DeleteResource(dr) => {
                if let ChangeItemFilter::ResourceChange(rcf) = self {
                    return rcf.is_allowed(&ResourceChange::DeleteResource(dr));
                }
                false
            },
            Change::WriteResource(wr) => {
                if let ChangeItemFilter::ResourceChange(rcf) = self {
                    return rcf.is_allowed(&ResourceChange::WriteResource(wr));
                }
                false
            },
            Change::DeleteTableItem(dti) => {
                if let ChangeItemFilter::TableChange(tcf) = self {
                    return tcf.is_allowed(&TableChange::DeleteTableItem(dti));
                }
                false
            },
            Change::WriteTableItem(wti) => {
                if let ChangeItemFilter::TableChange(tcf) = self {
                    return tcf.is_allowed(&TableChange::WriteTableItem(wti));
                }
                false
            },
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceChangeFilter {
    // todo: handle `generic_type_params` as well
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<MoveStructTagFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    // This is only applicable to WriteResource, but I'm lazy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonSearchTerm>,
}

pub enum ResourceChange<'a> {
    DeleteResource(&'a DeleteResource),
    WriteResource(&'a WriteResource),
}

impl Filterable<ResourceChange<'_>> for ResourceChangeFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), FilterError> {
        if self.resource_type.is_none() && self.address.is_none() {
            return Err(
                Error::msg("At least one of resource_type, address, or data must be set").into(),
            );
        };
        self.resource_type.is_valid()?;
        self.data.is_valid()?;
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &ResourceChange) -> bool {
        match &item {
            ResourceChange::DeleteResource(dr) => {
                if let Some(address) = &self.address {
                    if address != &dr.address {
                        return false;
                    }
                }
                if let Some(resource_type) = &self.resource_type {
                    if !resource_type.is_allowed_opt(&dr.r#type) {
                        return false;
                    }
                }
                if self.data.is_some() {
                    return false;
                }
            },
            ResourceChange::WriteResource(wr) => {
                if let Some(address) = &self.address {
                    if address != &wr.address {
                        return false;
                    }
                }
                if let Some(resource_type) = &self.resource_type {
                    if !resource_type.is_allowed_opt(&wr.r#type) {
                        return false;
                    }
                }
                if let Some(data) = &self.data {
                    if !data.find(&wr.data) {
                        return false;
                    }
                }
            },
        }
        true
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleChangeFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}
pub enum ModuleChange<'a> {
    DeleteModule(&'a DeleteModule),
    WriteModule(&'a WriteModule),
}

impl Filterable<ModuleChange<'_>> for ModuleChangeFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), FilterError> {
        if self.address.is_none() {
            return Err(Error::msg("At least one of address must be set").into());
        };
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &ModuleChange) -> bool {
        if let Some(address) = &self.address {
            return match &item {
                ModuleChange::DeleteModule(dm) => address == &dm.address,
                ModuleChange::WriteModule(wm) => address == &wm.address,
            };
        }
        true
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableChangeFilter {
    pub handle: Option<String>,
    pub key: Option<JsonOrStringSearch>,
    pub key_type_str: Option<String>,
}

pub enum TableChange<'a> {
    DeleteTableItem(&'a DeleteTableItem),
    WriteTableItem(&'a WriteTableItem),
}
impl Filterable<TableChange<'_>> for TableChangeFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), FilterError> {
        if self.handle.is_none() && self.key.is_none() && self.key_type_str.is_none() {
            return Err(
                Error::msg("At least one of handle, key, or key_type_str must be set").into(),
            );
        };
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &TableChange) -> bool {
        match &item {
            TableChange::DeleteTableItem(dti) => {
                if let Some(handle) = &self.handle {
                    return handle == &dti.handle;
                }
                if let Some(key_type) = &self.key_type_str {
                    if !dti
                        .data
                        .as_ref()
                        .map_or(false, |dtd| key_type == &dtd.key_type)
                    {
                        return false;
                    }
                }
                if let Some(key) = &self.key {
                    if !dti
                        .data
                        .as_ref()
                        .map_or(false, |dtd| key.is_allowed(&dtd.key))
                    {
                        return false;
                    }
                }
            },
            TableChange::WriteTableItem(wti) => {
                if let Some(handle) = &self.handle {
                    if handle != &wti.handle {
                        return false;
                    }
                }
                if let Some(key_type) = &self.key_type_str {
                    if !wti
                        .data
                        .as_ref()
                        .map_or(false, |wtd| key_type == &wtd.key_type)
                    {
                        return false;
                    }
                }
                self.key.is_allowed(&wti.key);
            },
        }
        true
    }
}
