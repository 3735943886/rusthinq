//! Bounded off-thread driver/module preparation, with explicit filesystem ownership.
use crate::{Compiled, Error, Limits, context, modules::Source};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
const MAX_SOURCE: usize = 524288;
#[derive(Clone, Debug)]
pub struct Config {
    pub watch: bool,
    pub directory: PathBuf,
    pub topic_prefix: String,
    pub bindings: BTreeMap<String, String>,
}
fn name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn read(root: &Path, name: &str) -> io::Result<String> {
    if !self::name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid driver/module name",
        ));
    }
    let path = root.join(format!("{name}.rhai")).canonicalize()?;
    if !path.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "driver escapes configured directory",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_SOURCE as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SOURCE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "driver source exceeded",
        ));
    }
    String::from_utf8(bytes).map_err(io::Error::other)
}
fn modules(
    root: &Path,
    source: &str,
    seen: &mut BTreeSet<String>,
    visiting: &mut BTreeSet<String>,
    bundle: &mut Vec<Source>,
    total: &mut usize,
) -> io::Result<()> {
    for line in source.lines() {
        let Some(import) = line.trim().strip_prefix("import ") else {
            continue;
        };
        let module = import
            .trim()
            .strip_prefix('"')
            .and_then(|value| value.split_once('"').map(|pair| pair.0))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "only static named imports are supported",
                )
            })?;
        if seen.contains(module) {
            continue;
        }
        if visiting.len() + seen.len() >= 16 || !visiting.insert(module.into()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cyclic/oversized driver module bundle",
            ));
        }
        let source = read(root, module)?;
        *total = total
            .checked_add(source.len() + module.len())
            .filter(|total| *total <= MAX_SOURCE)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "module source budget exceeded")
            })?;
        modules(root, &source, seen, visiting, bundle, total)?;
        visiting.remove(module);
        seen.insert(module.into());
        bundle.push(Source {
            name: module.into(),
            source,
        });
    }
    Ok(())
}
impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if !self.directory.is_dir()
            || self.topic_prefix.is_empty()
            || self.topic_prefix.len() > 256
            || self.topic_prefix.contains(['#', '+', '\0'])
            || self.bindings.len() > 256
            || self.bindings.iter().any(|(id, model)| {
                id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) || !name(model)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid driver configuration",
            ));
        }
        Ok(())
    }
    /// Fingerprint only this model and its static module dependencies.
    pub fn source_revision(&self, id: &str, model: &str) -> io::Result<u64> {
        use std::hash::{Hash, Hasher};
        let root = self.directory.canonicalize()?;
        let model = self.bindings.get(id).map(String::as_str).unwrap_or(model);
        let source = read(&root, model)?;
        let mut bundle = Vec::new();
        let mut total = source.len();
        modules(
            &root,
            &source,
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut bundle,
            &mut total,
        )?;
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        model.hash(&mut hash);
        source.hash(&mut hash);
        for module in bundle {
            module.name.hash(&mut hash);
            module.source.hash(&mut hash);
        }
        Ok(hash.finish())
    }
    pub fn prepare(
        &self,
        id: &str,
        model: &str,
        thinq2: bool,
        consumer: bool,
    ) -> Result<Compiled, Error> {
        let root = self
            .directory
            .canonicalize()
            .map_err(|e| Error::Compile(e.to_string()))?;
        let model = self.bindings.get(id).map(String::as_str).unwrap_or(model);
        let source = read(&root, model).map_err(|e| Error::Compile(e.to_string()))?;
        let mut bundle = Vec::new();
        let mut total = source.len();
        modules(
            &root,
            &source,
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            &mut bundle,
            &mut total,
        )
        .map_err(|e| Error::Compile(e.to_string()))?;
        let mut ctx = context::Config::new(id.into(), model.into());
        ctx.topic_prefix = self.topic_prefix.clone();
        ctx.thinq2 = thinq2;
        ctx.driver_api = true;
        ctx.message_seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Compile(e.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| Error::GenerationExhausted)?;
        ctx.state_keys = 256;
        ctx.state_bytes = 262144;
        let limits = Limits {
            source_bytes: MAX_SOURCE,
            string_bytes: 131072,
            operations: 5_000_000,
            outputs: 256,
            output_bytes: 1_048_576,
        };
        let compiled = Compiled::with_context(&source, limits, consumer, ctx)?;
        let mut entry = String::new();
        entry.push_str("fn __init(ctx,text) {");
        if compiled.has_function("publish_config", 1) {
            entry.push_str("publish_config(ctx);");
        }
        if compiled.has_function("start", 1) {
            entry.push_str("start(ctx);");
        }
        entry.push_str("}\n");
        entry.push_str("fn __data(ctx,text) {");
        if compiled.has_function("on_data", 2) {
            if thinq2 {
                entry.push_str("on_data(ctx,hex_decode(text));");
            } else {
                entry.push_str("let body=json_parse(bytes_utf8(hex_decode(text))).Body; if body.Format==\"B64\" && body.Data!=() {on_data(ctx,base64_decode(body.Data));}");
            }
        }
        entry.push_str("}\nfn __response(ctx,text) {");
        if compiled.has_function("on_response", 2) {
            entry.push_str("on_response(ctx,json_parse(text));");
        }
        entry.push_str("}\nfn __timer(ctx,text) {");
        if compiled.has_function("on_timer", 2) {
            entry.push_str("on_timer(ctx,text);");
        }
        entry.push_str("}\nfn __drop(ctx,text) {");
        if compiled.has_function("on_drop", 1) {
            entry.push_str("on_drop(ctx);");
        }
        // Commands reach the driver unvalidated; any semantic check is the script's.
        entry.push_str("}\nfn __command(ctx,text) {");
        if compiled.has_function("on_set_property", 3) {
            entry.push_str(
                "let command=json_parse(text);on_set_property(ctx,command.prop,command.value);",
            );
        }
        entry.push_str("}\n");
        compiled.with_entry(&entry)?.with_modules(bundle)
    }
}
