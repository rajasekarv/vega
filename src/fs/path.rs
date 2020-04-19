use once_cell::sync::Lazy;
use regex::Regex;
use uriparse::{Authority, Fragment, Query, URI};

pub const SEPARATOR: char = '/';
pub const SEPARATOR_CHAR: char = '/';
pub const CUR_DIR: &str = ".";
pub static WINDOWS: Lazy<bool> = Lazy::new(|| cfg!(windows));

static HAS_URI_SCHEME: Lazy<Regex> = Lazy::new(|| Regex::new("[a-zA-Z][a-zA-Z0-9+-.]+:").unwrap());
static HAS_DRIVE_LETTER_SPECIFIER: Lazy<Regex> = Lazy::new(|| Regex::new("^/?[a-zA-Z]:").unwrap());

#[derive(Eq, PartialEq, Debug)]
pub struct Path {
    url: URI<'static>,
}

impl Path {
    pub fn from_url(url: URI<'static>) -> Self {
        Path { url }
    }

    pub fn from_path_string(path_string: &str) -> Self {
        // TODO: can't directly parse as string might not be escaped
        let mut path_string: String = path_string.to_string();
        if path_string.is_empty() {
            panic!("can not create a Path from an empty string");
        }
        if Self::has_windows_drive(&path_string) && !path_string.starts_with('/') {
            path_string = format!("/{:?}", path_string);
        }
        let mut scheme = None;
        let mut _authority = None;
        let mut index = 0;

        // parse uri scheme if any present
        let colon = path_string.find(':');
        let slash = path_string.find('/');
        if colon.is_some() && (slash.is_none() || colon.unwrap() < slash.unwrap()) {
            scheme = Some(path_string.get(0..colon.unwrap()).unwrap());
            index = colon.unwrap() + 1;
        }

        if path_string.get(index..).unwrap().starts_with("//") && path_string.len() - index > 2 {
            let next_slash = path_string[index + 2..]
                .find('/')
                .map(|fi| index + 2 + fi)
                .unwrap();
            let auth_end = if next_slash > 0 {
                next_slash
            } else {
                path_string.len()
            };
            _authority = Some(path_string.get(index + 2..auth_end).unwrap());
            index = auth_end;
        }

        let path = path_string.get(index..path_string.len());

        if scheme.is_none() {
            scheme = Some("file");
        }
        let path = Self::normalize_path(scheme.unwrap().to_string(), path.unwrap().to_string());
        let mut url = URI::from_parts(
            scheme.unwrap(),
            None::<Authority>,
            path.as_str(),
            None::<Query>,
            None::<Fragment>,
        )
        .unwrap();
        url.normalize();

        Path {
            url: url.into_owned(),
        }
    }

    pub fn from_scheme_auth_path(scheme: &str, auth: Option<&str>, path: &str) -> Self {
        let mut path = path.to_string();
        if path.is_empty() {
            panic!("cannot creeate path from empty string");
        }
        if Self::has_windows_drive(&path) && !path.starts_with('/') {
            path = format!("/{}", path);
        }

        if !*WINDOWS && !path.starts_with('/') {
            path = format!("./{}", path);
        }

        path = Self::normalize_path(scheme.to_string(), path);
        let url =
            URI::from_parts(scheme, auth, path.as_str(), None::<Query>, None::<Fragment>).unwrap();
        Path::from_url(url.into_owned())
    }

    pub fn to_url(&self) -> &URI {
        &self.url
    }

    pub fn merge_paths(path1: Path, path2: Path) -> Path {
        let path2 = path2.to_url().path().to_string();
        let _path2: String = path2
            .get(Self::start_position_without_windows_drive(&path2)..)
            .unwrap()
            .into();
        let scheme = path1.to_url().scheme().to_string();
        let auth = path1.to_url().authority().map(|x| x.to_string());
        let path = path1.to_url().path().to_string();
        Path::from_scheme_auth_path(scheme.as_str(), auth.as_deref(), path.as_str())
    }

    pub fn is_url_path_absolute(&self) -> bool {
        let start = Self::start_position_without_windows_drive(&self.url.path().to_string());
        self.url
            .path()
            .to_string()
            .get(start..)
            .unwrap()
            .starts_with(SEPARATOR)
    }

    pub fn is_absolute(&self) -> bool {
        self.is_url_path_absolute()
    }

    fn start_position_without_windows_drive(path: &str) -> usize {
        if Self::has_windows_drive(path) {
            if path.chars().next().unwrap() == SEPARATOR {
                3
            } else {
                2
            }
        } else {
            0
        }
    }

    fn has_windows_drive(path: &str) -> bool {
        *WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.find(path).is_some()
    }

    pub fn get_name(&self) -> Option<String> {
        let path = self.url.path();
        let slash = path.to_string().rfind(SEPARATOR).map_or(0, |i| i + 1);
        path.to_string().get(slash..).map(|x| x.to_owned())
    }

    pub fn get_parent(&self) -> Option<Path> {
        let path = self.url.path();
        let last_slash = path.to_string().rfind(SEPARATOR);
        let start = Self::start_position_without_windows_drive(&path.to_string());
        if (path.to_string().len() == start)
            || (last_slash? == start && path.to_string().len() == start + 1)
        {
            return None;
        }
        let parent_path = if last_slash.is_none() {
            CUR_DIR.to_string()
        } else {
            path.to_string()
                .get(
                    0..if last_slash? == start {
                        start + 1
                    } else {
                        last_slash?
                    },
                )
                .map(|x| x.to_string())?
        };
        let mut parent = self.url.clone();
        parent.set_path(parent_path.as_str()).unwrap();
        Some(Path::from_url(parent.into_owned()))
    }

    pub fn is_root(&self) -> bool {
        self.get_parent().is_none()
    }

    fn normalize_path(scheme: String, mut path: String) -> String {
        path = path.replace("//", "/");
        if *WINDOWS && (Self::has_windows_drive(&path) || scheme.eq("file")) {
            path = path.replace("\\", "/");
        }

        let min_len = Self::start_position_without_windows_drive(&path) + 1;
        if (path.len() > min_len) && path.ends_with(SEPARATOR) {
            path = path.get(0..path.len() - 1).unwrap().to_string();
        }
        path
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
            Path::from_path_string("file:///foo/bar")
                .get_parent()
                .unwrap()
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
