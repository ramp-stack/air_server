use air::{Id, Identity, Request, Response, Secret, Signature};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub enum ClientMessage {
    Resume { token: Id, known: bool, seen: u64, challenge: Id },
    Request { index: u64, request: Request },
    Ack { seen: u64 },
    Cancel { indexes: Vec<u64> },
}

impl ClientMessage {
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        postcard::to_allocvec(self).map_err(|error| error.to_string())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        match postcard::take_from_bytes(bytes) {
            Ok((message, [])) => Ok(message),
            _ => Err("invalid client session message".into()),
        }
    }
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub enum ServerMessage {
    Challenge { challenge: Id },
    Ready { received: u64 },
    Received { received: u64 },
    Response { sequence: u64, index: u64, hash: Id, response: Response },
    Cancelled { indexes: Vec<u64> },
    Rejected { reason: String },
}

impl ServerMessage {
    pub fn encode(&self, secret: &Secret, context: Id) -> Result<Vec<u8>, String> {
        let signature = secret.sign(Id::hash(&("air-session-v1", context, self)));
        postcard::to_allocvec(&(signature, self)).map_err(|error| error.to_string())
    }

    pub fn decode(bytes: &[u8], identity: &Identity, context: Id) -> Result<Self, String> {
        let (signature, message): (Signature, Self) = match postcard::take_from_bytes(bytes) {
            Ok((message, [])) => message,
            _ => return Err("invalid server session message".into()),
        };
        signature
            .verify(identity, &[], Id::hash(&("air-session-v1", context, &message)))
            .map_err(|error| error.to_string())?;
        Ok(message)
    }
}
