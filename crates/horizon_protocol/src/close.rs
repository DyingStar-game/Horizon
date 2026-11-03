//! Close codes the server ends a client connection with.
//!
//! They sit in the 4000-4999 range, which WebSocket leaves to applications, so a client
//! can tell why it was disconnected and whether reconnecting makes sense. WebTransport
//! sessions close with the same numbers.

/// Why the server closed a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseCode {
    /// The client speaks a protocol version the server does not support.
    ProtocolMismatch,
    /// The client's frames cannot be read with the listener's format or compression.
    SettingsMismatch,
    /// The client's compression dictionary differs from the server's.
    DictionaryMismatch,
    /// Too many frames from the client could not be decoded.
    TooManyDecodeErrors,
    /// A frame from the client was larger than the server accepts.
    MessageTooLarge,
    /// A plugin kicked the player.
    Kicked,
    /// No traffic from the client for too long.
    IdleTimeout,
    /// The server has no room for another connection.
    ServerFull,
    /// The client did not read its messages fast enough.
    SlowClient,
}

impl CloseCode {
    /// Every close code, in numeric order.
    pub const ALL: [CloseCode; 9] = [
        CloseCode::ProtocolMismatch,
        CloseCode::SettingsMismatch,
        CloseCode::DictionaryMismatch,
        CloseCode::TooManyDecodeErrors,
        CloseCode::MessageTooLarge,
        CloseCode::Kicked,
        CloseCode::IdleTimeout,
        CloseCode::ServerFull,
        CloseCode::SlowClient,
    ];

    /// The numeric code sent to the client.
    pub fn code(self) -> u16 {
        match self {
            CloseCode::ProtocolMismatch => 4000,
            CloseCode::SettingsMismatch => 4001,
            CloseCode::DictionaryMismatch => 4002,
            CloseCode::TooManyDecodeErrors => 4003,
            CloseCode::MessageTooLarge => 4004,
            CloseCode::Kicked => 4005,
            CloseCode::IdleTimeout => 4006,
            CloseCode::ServerFull => 4007,
            CloseCode::SlowClient => 4008,
        }
    }

    /// The close code with this number, if it is one of the server's.
    pub fn from_code(code: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.code() == code)
    }

    /// The default reason text sent along with the code.
    pub fn default_reason(self) -> &'static str {
        match self {
            CloseCode::ProtocolMismatch => "protocol version not supported",
            CloseCode::SettingsMismatch => "frames do not match the listener's settings",
            CloseCode::DictionaryMismatch => "compression dictionary mismatch",
            CloseCode::TooManyDecodeErrors => "too many invalid frames",
            CloseCode::MessageTooLarge => "message too large",
            CloseCode::Kicked => "kicked by server",
            CloseCode::IdleTimeout => "idle timeout",
            CloseCode::ServerFull => "server full",
            CloseCode::SlowClient => "client not reading fast enough",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_distinct_and_in_the_application_range() {
        let mut codes: Vec<u16> = CloseCode::ALL.iter().map(|c| c.code()).collect();
        assert!(codes.iter().all(|c| (4000..5000).contains(c)));
        codes.dedup();
        assert_eq!(codes.len(), CloseCode::ALL.len());
        for code in CloseCode::ALL {
            assert_eq!(CloseCode::from_code(code.code()), Some(code));
        }
        assert_eq!(CloseCode::from_code(1000), None);
    }
}
