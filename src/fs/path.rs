use once_cell::sync::Lazy;
use regex::Regex;
use url::Url;

pub const SEPARATOR: &'static str = "/";
pub const SEPARATOR_CHAR: &'static str = "/";
pub const CUR_DIR: &'static str = ".";
pub static WINDOWS: Lazy<bool> = Lazy::new(|| if cfg!(windows) { true } else { false });

static has_uri_scheme: Lazy<Regex> = Lazy::new(|| Regex::new("[a-zA-Z][a-zA-Z0-9+-.]+:").unwrap());
static has_drive_letter_specifier: Lazy<Regex> = Lazy::new(|| Regex::new("^/?[a-zA-Z]:").unwrap());

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct Path {
    url: Url,
}

impl Path {
    pub fn from_url(url: Url) -> Self {
        Path { url }
    }

    pub fn to_url(&self) -> &Url {
        &self.url
    }

    pub fn is_url_path_absolute(&self) -> bool {
        let start = self.start_position_without_windows_drive(self.url.path());
        self.url.path().get(start..).unwrap().starts_with(SEPARATOR)
    }

    pub fn is_absolute(&self) -> bool {
        self.is_url_path_absolute()
    }

    fn start_position_without_windows_drive(&self, path: &str) -> usize {
        if self.has_windows_drive(path) {
            if path.chars().next().unwrap() == SEPARATOR {
                3
            } else {
                2
            }
        } else {
            0
        }
    }

    fn has_windows_drive(&self, path: &str) -> bool {
        *WINDOWS && has_drive_letter_specifier.find(path).is_some()
    }

    pub fn get_name(&self) -> Option<&str> {
        let path = self.url.path();
        let slash = path.rfind(SEPARATOR)?;
        path.get(slash + 1..)
    }
}
