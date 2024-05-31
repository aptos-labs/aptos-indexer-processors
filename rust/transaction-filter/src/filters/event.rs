use crate::{filters::MoveStructTagFilter, json_search::JsonSearchTerm, traits::Filterable};
use anyhow::Error;
use aptos_protos::transaction::v1::{move_type::Content, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonSearchTerm>,
    // Only for events that have a struct as their generic
    #[serde(skip_serializing_if = "Option::is_none")]
    pub struct_type: Option<MoveStructTagFilter>,
}

impl Filterable<Event> for EventFilter {
    #[inline]
    fn validate_state(&self) -> Result<(), Error> {
        if self.data.is_none() && self.struct_type.is_none() {
            return Err(Error::msg(
                "At least one of data or struct_type must be set",
            ));
        };

        self.data.is_valid()?;
        self.struct_type.is_valid()?;
        Ok(())
    }

    #[inline]
    fn is_allowed(&self, item: &Event) -> bool {
        if let Some(struct_type_filter) = &self.struct_type {
            if let Some(Content::Struct(struct_tag)) =
                &item.r#type.as_ref().and_then(|t| t.content.as_ref())
            {
                if !struct_type_filter.is_allowed(struct_tag) {
                    return false;
                }
            } else {
                return false;
            }
        }

        if !self.data.is_allowed(&item.data) {
            return false;
        }

        true
    }
}
