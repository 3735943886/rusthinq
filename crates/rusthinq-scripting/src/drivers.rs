//! Bounded off-thread driver/module preparation, with explicit filesystem ownership.
use crate::{Compiled, Error, Limits, context, modules::Source};
use rhai::{ASTNode, Engine, Expr, OptimizationLevel, Stmt};
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
/// LG model ids may contain `.` (`F_V8_Y___W.B_2QEUK`); modules stay plain names.
/// Every dot-separated part is a valid name, so `.`, `..` and hidden files are rejected.
fn model(model: &str) -> bool {
    model.len() <= 128 && model.split('.').all(name)
}

/// Where `name.rhai` lives: the root itself, which holds local overrides and flat
/// installs, else its `drivers/` or `modules/` subdirectory, a scripts checkout as is.
fn locate(root: &Path, subdirectory: &str, name: &str) -> PathBuf {
    let flat = root.join(format!("{name}.rhai"));
    if flat.exists() {
        flat
    } else {
        root.join(subdirectory).join(format!("{name}.rhai"))
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Driver,
    Module,
}

fn read(root: &Path, name: &str, kind: Kind) -> io::Result<String> {
    let (valid, subdirectory) = match kind {
        Kind::Driver => (self::model(name), "drivers"),
        Kind::Module => (self::name(name), "modules"),
    };
    if !valid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid driver/module name",
        ));
    }
    let path = locate(root, subdirectory, name).canonicalize()?;
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
    // Use Rhai's parser so whitespace, comments, escaped strings and multiple
    // imports per line have the same meaning as the source we later execute.
    let mut engine = Engine::new();
    engine
        .set_optimization_level(OptimizationLevel::None)
        .set_max_expr_depths(200, 100);
    let ast = engine
        .compile(source)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let mut imports = Vec::new();
    let static_imports = ast.walk(&mut |path| {
        if let Some(ASTNode::Stmt(Stmt::Import(import, _))) = path.last() {
            let Expr::StringConstant(module, _) = &import.0 else {
                return false;
            };
            imports.push(module.to_string());
        }
        true
    });
    if !static_imports {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "only static named imports are supported",
        ));
    }
    for module in &imports {
        let module = module.as_str();
        if seen.contains(module) {
            continue;
        }
        if visiting.len() + seen.len() >= 16 || !visiting.insert(module.into()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cyclic/oversized driver module bundle",
            ));
        }
        let source = read(root, module, Kind::Module)?;
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
/// Top-level `import "name";` without `as`: the driver inherits that module's functions.
fn bases(source: &str) -> io::Result<Vec<String>> {
    let ast = Engine::new()
        .compile(source)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok(ast
        .statements()
        .iter()
        .filter_map(|stmt| match stmt {
            Stmt::Import(import, _) if import.1.name.is_empty() => match &import.0 {
                Expr::StringConstant(module, _) => Some(module.to_string()),
                _ => None,
            },
            _ => None,
        })
        .collect())
}
/// Inherited modules, nearest first: a later entry's functions yield to every earlier one.
fn inheritance(bundle: &[Source], source: &str, chain: &mut Vec<String>) -> io::Result<()> {
    for base in bases(source)?.into_iter().rev() {
        if chain.contains(&base) {
            continue;
        }
        let module = bundle
            .iter()
            .find(|module| module.name == base)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inherited module"))?;
        chain.push(base);
        inheritance(bundle, &module.source, chain)?;
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
                id.is_empty()
                    || id.len() > 256
                    || id.chars().any(char::is_control)
                    || !self::model(model)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid driver configuration",
            ));
        }
        Ok(())
    }
    /// A driver file exists for this model (bindings are per device and do not apply).
    pub fn has_driver(&self, model: &str) -> bool {
        self::model(model)
            && self.directory.canonicalize().is_ok_and(|root| {
                locate(&root, "drivers", model)
                    .canonicalize()
                    .is_ok_and(|path| path.starts_with(&root) && path.is_file())
            })
    }
    /// Fingerprint only this model and its static module dependencies.
    pub fn source_revision(&self, id: &str, model: &str) -> io::Result<u64> {
        use std::hash::{Hash, Hasher};
        let root = self.directory.canonicalize()?;
        let model = self.bindings.get(id).map(String::as_str).unwrap_or(model);
        let source = read(&root, model, Kind::Driver)?;
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
        self.prepare_with_metadata(id, model, thinq2, consumer, None)
    }
    pub fn prepare_with_metadata(
        &self,
        id: &str,
        model: &str,
        thinq2: bool,
        consumer: bool,
        metadata: Option<(&str, &str)>,
    ) -> Result<Compiled, Error> {
        let root = self
            .directory
            .canonicalize()
            .map_err(|e| Error::Compile(e.to_string()))?;
        let model = self.bindings.get(id).map(String::as_str).unwrap_or(model);
        let source = read(&root, model, Kind::Driver).map_err(|e| Error::Compile(e.to_string()))?;
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
        if let Some((name, version)) = metadata {
            ctx.model_name = name.into();
            ctx.sw_version = version.into();
        }
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
        let mut chain = Vec::new();
        inheritance(&bundle, &source, &mut chain).map_err(|e| Error::Compile(e.to_string()))?;
        let mut compiled = Compiled::with_context(&source, limits, consumer, ctx)?;
        for base in &chain {
            let module = bundle.iter().find(|module| &module.name == base);
            compiled = compiled.with_base(module.map_or("", |module| module.source.as_str()))?;
        }
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
        entry.push_str("}\nfn __cloud(ctx,text) {");
        if compiled.has_function("on_cloud_event", 2) {
            entry.push_str("on_cloud_event(ctx,json_parse(text));");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_ids_may_contain_dots_but_not_empty_parts() {
        for valid in ["DHUM_056905_WW", "F_V8_Y___W.B_2QEUK", "a.b.c", "x-1"] {
            assert!(model(valid), "{valid}");
        }
        for invalid in ["", ".", "..", ".hidden", "a.", "a..b", "../x", "a/b", "a b"] {
            assert!(!model(invalid), "{invalid}");
        }
        assert!(!model(&"a".repeat(129)));
        assert!(!name("a.b"), "module names stay plain");
    }

    #[test]
    fn dotted_model_loads_while_dotted_module_does_not() {
        let root = std::env::temp_dir().join(format!("rusthinq-drivers-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("F_V8_Y___W.B_2QEUK.rhai"), "fn start(ctx) {}").unwrap();
        let root = root.canonicalize().unwrap();
        let read_model = read(&root, "F_V8_Y___W.B_2QEUK", Kind::Driver);
        let read_module = read(&root, "F_V8_Y___W.B_2QEUK", Kind::Module);
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(read_model.unwrap(), "fn start(ctx) {}");
        assert_eq!(read_module.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn checkout_layout_resolves_and_root_files_override() {
        let root = tempfile_dir("layout");
        std::fs::create_dir_all(root.join("drivers")).unwrap();
        std::fs::create_dir_all(root.join("modules")).unwrap();
        std::fs::write(root.join("drivers/M.rhai"), "import \"c\" as c;").unwrap();
        std::fs::write(root.join("modules/c.rhai"), "fn shipped() {}").unwrap();
        let root = root.canonicalize().unwrap();
        let driver = read(&root, "M", Kind::Driver).unwrap();
        let shipped = read(&root, "c", Kind::Module).unwrap();
        std::fs::write(root.join("c.rhai"), "fn local() {}").unwrap();
        let local = read(&root, "c", Kind::Module).unwrap();
        let module_as_driver = read(&root, "c", Kind::Driver).unwrap();
        let missing = read(&root, "M", Kind::Module);
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(driver, "import \"c\" as c;");
        assert_eq!(shipped, "fn shipped() {}");
        assert_eq!(
            local, "fn local() {}",
            "a root file overrides the shipped one"
        );
        assert_eq!(
            module_as_driver, "fn local() {}",
            "the root stays flat for both"
        );
        assert_eq!(missing.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    fn tempfile_dir(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("rusthinq-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn unaliased_import_inherits_hooks_nearest_first() {
        let root = tempfile_dir("inherit");
        std::fs::create_dir_all(root.join("drivers")).unwrap();
        std::fs::create_dir_all(root.join("modules")).unwrap();
        std::fs::write(root.join("modules/base.rhai"), "fn on_data(ctx, data) {}").unwrap();
        std::fs::write(root.join("modules/mid.rhai"), "import \"base\";").unwrap();
        std::fs::write(
            root.join("drivers/M.rhai"),
            "import \"mid\"; import \"x\" as x;",
        )
        .unwrap();
        std::fs::write(root.join("modules/x.rhai"), "fn f() {}").unwrap();
        let config = Config {
            watch: false,
            directory: root.clone(),
            topic_prefix: "t".into(),
            bindings: Default::default(),
        };
        let compiled = config.prepare("d", "M", true, true);
        let source = std::fs::read_to_string(root.join("drivers/M.rhai")).unwrap();
        let bundle = vec![
            Source {
                name: "base".into(),
                source: "fn on_data(ctx, data) {}".into(),
            },
            Source {
                name: "mid".into(),
                source: "import \"base\";".into(),
            },
        ];
        let mut chain = Vec::new();
        inheritance(&bundle, &source, &mut chain).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(chain, ["mid", "base"], "aliased imports are not inherited");
        assert!(compiled.unwrap().has_function("on_data", 2));
    }
}
