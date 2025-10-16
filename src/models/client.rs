use serde::{Deserialize, Serialize};

/// The [ClientId] is a newtype wrapping the client id expressed by an [u16].
/// This is used for referential integrity, while ensuring the exposed API surface (see auto derives)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ClientId(u16);

impl ClientId {
    pub fn new(id: u16) -> Self {
        Self(id)
    }
}
