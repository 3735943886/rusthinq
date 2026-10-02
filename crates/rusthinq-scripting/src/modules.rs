//! Caller-provided source bundle. No paths, directory scans, or file resolver.
use crate::{Compiled, Error};
use rhai::{AST, Module, Scope, Stmt, module_resolvers::StaticModuleResolver};
use std::collections::BTreeSet;

pub struct Source {
    pub name: String,
    pub source: String,
}
impl Compiled {
    /// Prepare one ordered bundle off the runtime loop. Dependencies precede importers.
    /// The source budget covers the entry script and all module names/sources together.
    pub fn with_modules(mut self, sources: Vec<Source>) -> Result<Self, Error> {
        if self.modules_loaded || sources.len() > 16 {
            return Err(Error::InvalidConfig);
        }
        let mut names = BTreeSet::new();
        let mut bytes = self.source_bytes;
        for source in &sources {
            if source.name.is_empty()
                || source.name.len() > 64
                || !source
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
                || !names.insert(source.name.clone())
            {
                return Err(Error::InvalidConfig);
            }
            bytes = bytes
                .checked_add(source.name.len())
                .and_then(|bytes| bytes.checked_add(source.source.len()))
                .filter(|bytes| *bytes <= self.limits.source_bytes)
                .ok_or(Error::InvalidConfig)?;
        }
        self.buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .preparing = true;
        let mut resolver = StaticModuleResolver::new();
        let prepared =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), Error> {
                for source in sources {
                    self.engine.set_module_resolver(resolver.clone());
                    let ast = self.engine.compile(&source.source).map_err(|error| {
                        Error::Compile(format!("module {}: {error}", source.name))
                    })?;
                    let ast = self
                        .support_ast
                        .as_ref()
                        .map_or(ast.clone(), |support| ast.merge(support));
                    let module = Module::eval_ast_as_new(Scope::new(), &ast, &self.engine)
                        .map_err(|error| {
                            Error::Compile(format!("module {}: {error}", source.name))
                        })?;
                    resolver.insert(source.name, module);
                }
                Ok(())
            }));
        self.buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .preparing = false;
        match prepared {
            Ok(result) => result?,
            Err(_) => return Err(Error::Compile("module initialization panic".into())),
        }
        self.engine.set_module_resolver(resolver);
        // call_fn creates a fresh import environment each time. Re-evaluate only imports,
        // keeping initialization and mutable top-level scope out of later callback runs.
        let imports = self
            .ast
            .statements()
            .iter()
            .filter(|stmt| matches!(stmt, Stmt::Import(..)))
            .cloned();
        self.invocation_ast = Some(AST::new(imports, self.ast.shared_lib().clone()));
        self.source_bytes = bytes;
        self.modules_loaded = true;
        Ok(self)
    }
}
