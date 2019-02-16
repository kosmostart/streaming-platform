#[derive(Debug)]
pub enum Error {    
	Io(std::io::Error),
    ChannelReceive(crossbeam::channel::RecvError)
}

impl From<std::io::Error> for Error {
	fn from(err: std::io::Error) -> Error {
		Error::Io(err)
	}
}

impl From<crossbeam::channel::RecvError> for Error {
	fn from(err: crossbeam::channel::RecvError) -> Error {
		Error::ChannelReceive(err)
	}
}
