use crate::{filters::MoveStructTagFilter, traits::Filterable};
use anyhow::Error;
use aptos_protos::transaction::v1::{move_type::Content, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventFilter {
    // Only for events that have a struct as their generic
    #[serde(skip_serializing_if = "Option::is_none")]
    pub struct_type: Option<MoveStructTagFilter>,
}

impl Filterable<Event> for EventFilter {
    fn is_valid(&self) -> Result<(), Error> {
        if self.struct_type.is_none() {
            return Err(Error::msg("At least one of struct_type must be set"));
        };

        self.struct_type.is_valid()?;
        Ok(())
    }

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

        true
    }
}
