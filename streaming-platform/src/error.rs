#[derive(Debug)]
pub enum Error {    
	Io(std::io::Error),
	SerdeJson(serde_json::Error),
    ChannelReceive(crossbeam::channel::RecvError),
	EmptyCorrelationIdPassed
}

impl From<std::io::Error> for Error {
	fn from(err: std::io::Error) -> Error {
		Error::Io(err)
	}
}

impl From<serde_json::Error> for Error {
	fn from(err: serde_json::Error) -> Error {
		Error::SerdeJson(err)
	}
}

impl From<crossbeam::channel::RecvError> for Error {
	fn from(err: crossbeam::channel::RecvError) -> Error {
		Error::ChannelReceive(err)
	}
}
