#[derive(Debug)]
pub enum Error {
	None,
	CustomError(String),
	Io(std::io::Error),
	SerdeJson(serde_json::Error)
}

impl Error {
	pub fn custom(e: &str) -> Error {
		Error::CustomError(e.to_owned())
	}
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {		
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        "I'm the superhero of errors"
    }    
}

impl From<String> for Error {
	fn from(e: String) -> Error {
		Error::CustomError(e)
	}
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Error {
		Error::Io(e)
	}
}

impl From<serde_json::Error> for Error {
	fn from(e: serde_json::Error) -> Error {
		Error::SerdeJson(e)
	}
}