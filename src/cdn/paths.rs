// Modified by Aurelia; see README.
//! Keep manifest entries inside root.

use std::path::{Component, Path, PathBuf};

use crate::Error;

/// Longest accepted symlink target.
pub(crate) const MAX_LINK_TARGET: u64 = 4096;

fn unsafe_path(what: &str, path: &str) -> Error {
    Error::Unexpected(format!("unsafe {what} in manifest: {path:?}"))
}

/// Manifest path to safe relative path.
pub(crate) fn relative_entry(path: &str) -> Result<PathBuf, Error> {
    if path.contains('\0') {
        return Err(unsafe_path("path", path));
    }
    let mut out = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_path("path", path));
            }
        }
    }
    // Colons mean streams on Windows.
    if out.as_os_str().is_empty() || (cfg!(windows) && out.to_string_lossy().contains(':')) {
        return Err(unsafe_path("path", path));
    }
    Ok(out)
}

/// Whether a link target stays inside.
pub(crate) fn link_stays_inside(link: &Path, target: &str) -> bool {
    let target = Path::new(target);
    if target.as_os_str().is_empty() || target.has_root() {
        return false;
    }
    let mut depth: Vec<&std::ffi::OsStr> = link
        .parent()
        .into_iter()
        .flat_map(Path::components)
        .filter_map(|c| match c {
            Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();
    for component in target.components() {
        match component {
            Component::Normal(part) => depth.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                if depth.pop().is_none() {
                    return false;
                }
            }
            Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

/// Create and canonicalize the root.
pub(crate) fn prepare_root(root: &Path) -> Result<PathBuf, Error> {
    std::fs::create_dir_all(root).map_err(Error::Io)?;
    root.canonicalize().map_err(Error::Io)
}

/// Nearest existing ancestor must resolve inside.
pub(crate) fn ensure_inside(root: &Path, path: &Path) -> Result<(), Error> {
    let mut probe = path;
    loop {
        if std::fs::symlink_metadata(probe).is_ok() {
            let resolved = probe.canonicalize().map_err(Error::Io)?;
            if resolved.starts_with(root) {
                return Ok(());
            }
            return Err(unsafe_path("location", &path.to_string_lossy()));
        }
        match probe.parent() {
            Some(parent) => probe = parent,
            None => return Err(unsafe_path("location", &path.to_string_lossy())),
        }
    }
}

/// Drop a symlink before writing there.
pub(crate) fn unlink_symlink(path: &Path) -> Result<(), Error> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => std::fs::remove_file(path).map_err(Error::Io),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cdn-paths-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.canonicalize().unwrap()
    }

    #[test]
    fn plain_paths_are_kept() {
        assert_eq!(relative_entry("bin/game").unwrap(), PathBuf::from("bin/game"));
        assert_eq!(relative_entry("./a/./b").unwrap(), PathBuf::from("a/b"));
    }

    #[test]
    fn escaping_paths_are_rejected() {
        for bad in ["", ".", "../x", "a/../../x", "a/..", "/etc/passwd", "a\0b"] {
            assert!(relative_entry(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn link_targets_must_stay_inside() {
        let link = Path::new("lib/libfoo.so");
        assert!(link_stays_inside(link, "libfoo.so.1"));
        assert!(link_stays_inside(link, "../bin/tool"));
        assert!(!link_stays_inside(link, "../../outside"));
        assert!(!link_stays_inside(link, "/usr/lib/libfoo.so"));
        assert!(!link_stays_inside(Path::new("top"), ".."));
        assert!(!link_stays_inside(link, ""));
    }

    #[test]
    fn existing_paths_inside_root_pass() {
        let root = scratch("inside");
        std::fs::create_dir_all(root.join("a")).unwrap();
        ensure_inside(&root, &root.join("a/new/file")).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_directory_out_of_root_is_rejected() {
        let root = scratch("escape");
        let outside = scratch("escape-target");
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        assert!(ensure_inside(&root, &root.join("link/file")).is_err());
        assert!(!outside.join("file").exists());
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_at_file_path_is_removed_not_followed() {
        let root = scratch("unlink");
        let outside = scratch("unlink-target");
        std::fs::write(outside.join("victim"), b"keep").unwrap();
        let file = root.join("file");
        std::os::unix::fs::symlink(outside.join("victim"), &file).unwrap();
        unlink_symlink(&file).unwrap();
        assert!(std::fs::symlink_metadata(&file).is_err());
        assert_eq!(std::fs::read(outside.join("victim")).unwrap(), b"keep");
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }
}
