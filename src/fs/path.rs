use crate::error::Result;
use once_cell::sync::Lazy;
use regex::Regex;
use url::Url;

pub const SEPARATOR: char = '/';
pub const SEPARATOR_CHAR: char = '/';
pub const CUR_DIR: &'static str = ".";
pub static WINDOWS: Lazy<bool> = Lazy::new(|| if cfg!(windows) { true } else { false });

static HAS_URI_SCHEME: Lazy<Regex> = Lazy::new(|| Regex::new("[a-zA-Z][a-zA-Z0-9+-.]+:").unwrap());
static HAS_DRIVE_LETTER_SPECIFIER: Lazy<Regex> = Lazy::new(|| Regex::new("^/?[a-zA-Z]:").unwrap());

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Path {
    url: Url,
}

impl Path {
    pub fn from_url(url: Url) -> Self {
        Path { url }
    }

    pub fn from_path_string(path_string: &str) -> Self {
        // TODO can't directly parse as string might not be escaped
        let url = Url::parse(path_string).unwrap();
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
        *WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.find(path).is_some()
    }

    pub fn get_name(&self) -> Option<&str> {
        let path = self.url.path();
        let slash = path.rfind(SEPARATOR)?;
        path.get(slash + 1..)
    }

    pub fn get_parent(&self) -> Option<Path> {
        let path = self.url.path();
        let last_slash = path.rfind(SEPARATOR);
        let start = self.start_position_without_windows_drive(path);
        if (path.len() == start) || (last_slash? == start && path.len() == start + 1) {
            return None;
        }
        let parent_path = if last_slash.is_none() {
            CUR_DIR
        } else {
            path.get(
                0..if last_slash? == start {
                    start + 1
                } else {
                    last_slash?
                },
            )?
        };
        let mut parent = self.url.clone();
        parent.set_path(parent_path);
        Some(Path::from_url(parent))
    }

    pub fn is_root(&self) -> bool {
        self.get_parent().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_name() {
        assert_eq!("", Path::from_path_string("/").get_name().unwrap());
        assert_eq!("foo", Path::from_path_string("foo").get_name().unwrap());
        assert_eq!("foo", Path::from_path_string("/foo").get_name().unwrap());
        assert_eq!("foo", Path::from_path_string("/foo/").get_name().unwrap());
        assert_eq!(
            "bar",
            Path::from_path_string("/foo/bar").get_name().unwrap()
        );
        assert_eq!(
            "bar",
            Path::from_path_string("hdfs://host/foo/bar")
                .get_name()
                .unwrap()
        );
    }

    #[test]
    fn is_absolute() {
        assert!(Path::from_path_string("/").is_absolute());
        assert!(Path::from_path_string("/foo").is_absolute());
        assert!(!Path::from_path_string("foo").is_absolute());
        assert!(!Path::from_path_string("foo/bar").is_absolute());
        assert!(!Path::from_path_string(".").is_absolute());
        assert!(Path::from_path_string("scheme:///foo/bar").is_absolute());

        if *WINDOWS {
            assert!(Path::from_path_string("C:/a/b").is_absolute());
            assert!(Path::from_path_string("C:a/b").is_absolute());
        }
    }

    #[test]
    fn parent() {
        assert_eq!(
            Path::from_path_string("/foo"),
            Path::from_path_string("/foo/bar").get_parent().unwrap()
        );
        assert_eq!(
            Path::from_path_string("foo"),
            Path::from_path_string("foo/bar").get_parent().unwrap()
        );
        assert_eq!(
            Path::from_path_string("/"),
            Path::from_path_string("/foo").get_parent().unwrap()
        );
        assert!(Path::from_path_string("/").get_parent().is_none());

        if *WINDOWS {
            assert_eq!(
                Path::from_path_string("c:/"),
                Path::from_path_string("c:/foo").get_parent().unwrap()
            );
        }
    }
}
