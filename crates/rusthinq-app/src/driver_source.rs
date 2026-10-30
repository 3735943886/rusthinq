//! Where the configured driver directory came from, for display only. Reads a few
//! small files; runs no Git commands and never changes the directory.
use serde_json::{Value, json};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

const LIMIT: u64 = 65536;

fn small(path: &Path) -> Option<String> {
    let mut text = String::new();
    File::open(path)
        .ok()?
        .take(LIMIT)
        .read_to_string(&mut text)
        .ok()?;
    Some(text)
}
fn commit(text: &str) -> Option<&str> {
    Some(text).filter(|text| text.len() == 40 && text.bytes().all(|b| b.is_ascii_hexdigit()))
}
/// `.git` is a directory, or a file naming one (worktrees, submodules).
fn git_dir(root: &Path) -> Option<PathBuf> {
    let dot = root.join(".git");
    if dot.is_dir() {
        return Some(dot);
    }
    let pointer = small(&dot)?;
    let target = Path::new(pointer.strip_prefix("gitdir:")?.trim());
    Some(root.join(target))
}
fn resolve(git: &Path, name: &str) -> Option<String> {
    // A worktree keeps its own HEAD; branches live in the common directory.
    let common = small(&git.join("commondir"))
        .map(|dir| git.join(dir.trim()))
        .unwrap_or_else(|| git.to_owned());
    for dir in [git, common.as_path()] {
        if let Some(found) =
            small(&dir.join(name)).and_then(|t| commit(t.trim()).map(str::to_owned))
        {
            return Some(found);
        }
    }
    small(&common.join("packed-refs"))?
        .lines()
        .find_map(|line| {
            let (hash, reference) = line.split_once(' ')?;
            (reference == name).then(|| commit(hash).map(str::to_owned))?
        })
}
fn git(root: &Path) -> Option<Value> {
    let git = git_dir(root)?;
    let head = small(&git.join("HEAD"))?;
    let head = head.trim();
    let (commit, branch) = match head.strip_prefix("ref:") {
        Some(reference) => {
            let reference = reference.trim();
            (
                resolve(&git, reference),
                reference.strip_prefix("refs/heads/").map(str::to_owned),
            )
        }
        None => (commit(head).map(str::to_owned), None),
    };
    Some(json!({"kind":"git","commit":commit,"branch":branch}))
}
/// rusthinq-scripts' VERSION, which `git archive` fills in as `<commit> <yyyy-mm-dd>`.
fn archive(root: &Path) -> Option<Value> {
    let text = small(&root.join("VERSION"))?;
    let (hash, date) = text.trim().split_once(' ')?;
    let date = Some(date).filter(|date| {
        date.len() == 10
            && date.bytes().enumerate().all(|(i, b)| {
                if i == 4 || i == 7 {
                    b == b'-'
                } else {
                    b.is_ascii_digit()
                }
            })
    })?;
    Some(json!({"kind":"archive","commit":commit(hash)?,"date":date}))
}
/// `{"kind":"git"|"archive"|"unknown", ...}`. An unfilled VERSION, as in a Git checkout
/// copied by hand, says nothing about the files and reads as unknown.
pub fn describe(root: &Path) -> Value {
    git(root)
        .or_else(|| archive(root))
        .unwrap_or_else(|| json!({"kind":"unknown"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    const A: &str = "3e7b2765eab4bb86168f198ffd4ade4837890e92";
    const B: &str = "9cc8647aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn git_heads_archives_and_templates() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        assert_eq!(describe(root), json!({"kind":"unknown"}));
        std::fs::write(root.join("VERSION"), "$Format:%H %cs$\n").unwrap();
        assert_eq!(describe(root)["kind"], "unknown", "unfilled template");
        std::fs::write(root.join("VERSION"), format!("{A} 2026-10-30\n")).unwrap();
        assert_eq!(
            describe(root),
            json!({"kind":"archive","commit":A,"date":"2026-10-30"})
        );
        std::fs::create_dir_all(root.join(".git/refs/heads")).unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/master\n").unwrap();
        std::fs::write(
            root.join(".git/packed-refs"),
            format!("# pack-refs with: peeled\n{B} refs/heads/master\n"),
        )
        .unwrap();
        assert_eq!(
            describe(root),
            json!({"kind":"git","commit":B,"branch":"master"}),
            "a checkout wins over its VERSION; packed refs resolve"
        );
        std::fs::write(root.join(".git/refs/heads/master"), format!("{A}\n")).unwrap();
        assert_eq!(describe(root)["commit"], A, "a loose ref wins");
        std::fs::write(root.join(".git/HEAD"), format!("{B}\n")).unwrap();
        assert_eq!(
            describe(root),
            json!({"kind":"git","commit":B,"branch":null}),
            "detached"
        );
    }

    #[test]
    fn worktree_pointer_and_common_dir() {
        let base = tempfile::tempdir().unwrap();
        let common = base.path().join("main.git");
        let own = common.join("worktrees/w");
        std::fs::create_dir_all(&own).unwrap();
        std::fs::create_dir_all(common.join("refs/heads")).unwrap();
        std::fs::write(own.join("HEAD"), "ref: refs/heads/dev\n").unwrap();
        std::fs::write(own.join("commondir"), "../..\n").unwrap();
        std::fs::write(common.join("refs/heads/dev"), format!("{A}\n")).unwrap();
        let root = base.path().join("w");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(".git"), format!("gitdir: {}\n", own.display())).unwrap();
        assert_eq!(
            describe(&root),
            json!({"kind":"git","commit":A,"branch":"dev"})
        );
    }
}
