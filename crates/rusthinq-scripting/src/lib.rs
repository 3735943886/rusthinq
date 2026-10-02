//! L5 bounded, semantics-agnostic Rhai execution. The caller owns worker scheduling and sinks.
mod codecs;
pub mod context;
pub mod drivers;
pub mod modules;
pub mod preparation;
pub mod scheduling;
pub mod worker;
use rhai::{
    AST, CallFnOptions, Dynamic, Engine, EvalAltResult, Scope,
    module_resolvers::StaticModuleResolver,
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Limits {
    pub source_bytes: usize,
    pub string_bytes: usize,
    pub operations: u64,
    pub outputs: usize,
    pub output_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            source_bytes: 65536,
            string_bytes: 16384,
            operations: 100000,
            outputs: 64,
            output_bytes: 65536,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Publish(String),
    Send(String),
    Timer { name: String, after_ms: Option<u64> },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Busy,
    Stopped,
    InputExceeded,
    Worker(String),
    InvalidConfig,
    Compile(String),
    Execution(String),
    Faulted,
    Stale,
    GenerationExhausted,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    pub generation: u64,
    pub outputs: Vec<Output>,
    pub error: Option<Error>,
}
struct Buffer {
    preparing: bool,
    outputs: Vec<Output>,
    bytes: usize,
    limits: Limits,
    consumer: bool,
}
impl Buffer {
    fn timer(&mut self, name: String, after_ms: Option<u64>) -> Result<(), Box<EvalAltResult>> {
        if name.is_empty() || name.len() > 256 {
            return Err("invalid timer name".into());
        }
        self.emit(format!("timer:{name}:{after_ms:?}"), false)?;
        *self.outputs.last_mut().expect("admitted timer") = Output::Timer { name, after_ms };
        Ok(())
    }
    fn emit(&mut self, value: String, publish: bool) -> Result<(), Box<EvalAltResult>> {
        if self.preparing {
            return Err("module initialization output forbidden".into());
        }
        if publish && !self.consumer {
            return Err("consumer disabled".into());
        }
        let bytes = self
            .bytes
            .checked_add(value.len())
            .ok_or_else(|| Box::<EvalAltResult>::from("output size overflow"))?;
        if value.len() > self.limits.string_bytes
            || bytes > self.limits.output_bytes
            || self.outputs.len() >= self.limits.outputs
        {
            return Err("output capacity exceeded".into());
        }
        self.bytes = bytes;
        self.outputs.push(if publish {
            Output::Publish(value)
        } else {
            Output::Send(value)
        });
        Ok(())
    }
}
pub struct Compiled {
    support_ast: Option<AST>,
    invocation_ast: Option<AST>,
    source_bytes: usize,
    modules_loaded: bool,
    context: Option<context::Context>,
    engine: Engine,
    ast: AST,
    buffer: Arc<Mutex<Buffer>>,
    limits: Limits,
}
impl Compiled {
    /// Opt into callbacks taking `(ctx, text)`. Only opaque primitives are installed:
    /// publish/send, `publish_raw(topic, payload, retain)`, device sends and timers.
    pub fn with_context(
        source: &str,
        limits: Limits,
        consumer_enabled: bool,
        config: context::Config,
    ) -> Result<Self, Error> {
        let ctx = context::Context::new(config, limits.string_bytes)?;
        let mut compiled = Self::new(source, limits, consumer_enabled)?;
        context::install(&mut compiled.engine);
        codecs::install(&mut compiled.engine);
        for (name, publish) in [("publish", true), ("send", false)] {
            let buffer = compiled.buffer.clone();
            compiled.engine.register_fn(
                name,
                move |_ctx: &mut context::Context,
                      value: String|
                      -> Result<(), Box<EvalAltResult>> {
                    buffer
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .emit(value, publish)
                },
            );
        }
        let buffer = compiled.buffer.clone();
        compiled.engine.register_fn(
            "publish_raw",
            move |_ctx: &mut context::Context,
                  topic: String,
                  payload: Dynamic,
                  retain: bool|
                  -> Result<(), Box<EvalAltResult>> {
                let payload: serde_json::Value =
                    rhai::serde::from_dynamic(&payload).map_err(|e| e.to_string())?;
                let text = serde_json::json!({"topic":topic,"payload":payload,"retain":retain})
                    .to_string();
                buffer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .emit(text, true)
            },
        );
        let buffer = compiled.buffer.clone();
        compiled.engine.register_fn(
            "send_json",
            move |ctx: &mut context::Context, text: String| -> Result<(), Box<EvalAltResult>> {
                let text = ctx.json(text)?;
                buffer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .emit(text, false)
            },
        );
        if ctx.driver_api() {
            let buffer = compiled.buffer.clone();
            compiled.engine.register_fn(
                "send_raw",
                move |ctx: &mut context::Context,
                      bytes: rhai::Blob|
                      -> Result<(), Box<EvalAltResult>> {
                    let text = ctx.wire(
                        "packet",
                        1,
                        rusthinq_protocol::hex::encode_upper(bytes).into(),
                    )?;
                    buffer
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .emit(text, false)
                },
            );
            let buffer = compiled.buffer.clone();
            compiled.engine.register_fn(
                "send_clip",
                move |ctx: &mut context::Context,
                      cmd: String,
                      msg_type: i64,
                      text: String|
                      -> Result<(), Box<EvalAltResult>> {
                    let data: serde_json::Value =
                        serde_json::from_str(&text).map_err(|e| e.to_string())?;
                    let text = ctx.wire(&cmd, msg_type, data)?;
                    buffer
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .emit(text, false)
                },
            );
        }
        for cancel in [false, true] {
            let buffer = compiled.buffer.clone();
            if cancel {
                compiled.engine.register_fn(
                    "cancel_timer",
                    move |_ctx: &mut context::Context, name: String| {
                        buffer
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .timer(name, None)
                    },
                );
            } else {
                compiled.engine.register_fn(
                    "set_timer",
                    move |_ctx: &mut context::Context, name: String, after_ms: i64| {
                        buffer
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .timer(name, Some(after_ms.max(0) as u64))
                    },
                );
            }
        }
        compiled.context = Some(ctx);
        Ok(compiled)
    }
    /// Install caller-owned Rhai helpers globally (also visible inside modules).
    /// Semantic helpers stay in scripts; initialization cannot send/publish.
    pub fn with_support(mut self, source: &str) -> Result<Self, Error> {
        let total = self
            .source_bytes
            .checked_add(source.len())
            .filter(|n| *n <= self.limits.source_bytes)
            .ok_or(Error::InvalidConfig)?;
        self.buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .preparing = true;
        let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let ast = self
                .engine
                .compile(source)
                .map_err(|e| Error::Compile(e.to_string()))?;
            if !ast.statements().is_empty() {
                return Err(Error::Compile(
                    "support source must contain only functions".into(),
                ));
            }
            let module = rhai::Module::eval_ast_as_new(Scope::new(), &ast, &self.engine)
                .map_err(|e| Error::Compile(e.to_string()))?;
            Ok::<_, Error>((ast, module))
        }));
        self.buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .preparing = false;
        let (ast, module) =
            prepared.map_err(|_| Error::Compile("support initialization panic".into()))??;
        self.ast = self.ast.merge(&ast);
        self.support_ast = Some(ast);
        self.engine.register_global_module(module.into());
        self.source_bytes = total;
        Ok(self)
    }
    pub fn context_device(&self) -> Option<&str> {
        self.context.as_ref().map(|ctx| ctx.device())
    }
    pub fn has_function(&self, name: &str, arguments: usize) -> bool {
        self.ast
            .iter_functions()
            .any(|function| function.name == name && function.params.len() == arguments)
    }
    pub fn with_entry(mut self, source: &str) -> Result<Self, Error> {
        self.source_bytes = self
            .source_bytes
            .checked_add(source.len())
            .filter(|n| *n <= self.limits.source_bytes)
            .ok_or(Error::InvalidConfig)?;
        let entry = self
            .engine
            .compile(source)
            .map_err(|e| Error::Compile(e.to_string()))?;
        if !entry.statements().is_empty() {
            return Err(Error::InvalidConfig);
        }
        self.ast = self.ast.merge(&entry);
        if let Some(invocation) = &mut self.invocation_ast {
            *invocation = invocation.merge(&entry);
        }
        Ok(self)
    }
    /// Configure the actual consumer before attaching this compiled generation.
    pub fn set_consumer_enabled(&mut self, enabled: bool) {
        self.buffer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .consumer = enabled;
    }
    /// Compile off the transport loop, before swapping the running generation.
    /// No filesystem resolver, MQTT factory, or semantic validation is installed.
    pub fn new(source: &str, limits: Limits, consumer_enabled: bool) -> Result<Self, Error> {
        if limits.source_bytes == 0
            || limits.string_bytes == 0
            || limits.operations == 0
            || limits.outputs == 0
            || limits.output_bytes == 0
            || source.len() > limits.source_bytes
        {
            return Err(Error::InvalidConfig);
        }
        let buffer = Arc::new(Mutex::new(Buffer {
            preparing: false,
            outputs: Vec::new(),
            bytes: 0,
            limits: limits.clone(),
            consumer: consumer_enabled,
        }));
        let mut engine = Engine::new();
        engine.set_module_resolver(StaticModuleResolver::new());
        engine
            .set_max_expr_depths(200, 100)
            .set_max_operations(limits.operations)
            .set_max_string_size(limits.string_bytes)
            .set_max_array_size(1024)
            .set_max_map_size(256)
            .set_max_call_levels(32);
        engine.on_print(|_| {});
        engine.on_debug(|_, _, _| {});
        for (name, publish) in [("publish", true), ("send", false)] {
            let buffer = buffer.clone();
            engine.register_fn(
                name,
                move |value: String| -> Result<(), Box<EvalAltResult>> {
                    let mut buffer = buffer.lock().unwrap_or_else(|e| e.into_inner());
                    buffer.emit(value, publish)
                },
            );
        }
        let ast = engine
            .compile(source)
            .map_err(|error| Error::Compile(error.to_string()))?;
        Ok(Self {
            support_ast: None,
            invocation_ast: None,
            source_bytes: source.len(),
            modules_loaded: false,
            context: None,
            engine,
            ast,
            buffer,
            limits,
        })
    }
}
pub struct Host {
    compiled: Compiled,
    scope: Scope<'static>,
    generation: u64,
    initialized: bool,
    faulted: bool,
}
impl Host {
    pub fn new(compiled: Compiled) -> Self {
        Self {
            compiled,
            scope: Scope::new(),
            generation: 1,
            initialized: false,
            faulted: false,
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// The caller must invoke serially on a device-owned blocking worker.
    /// Outputs preserve the emitted prefix on failure; downstream must fence the
    /// returned generation and report delivery separately from script execution.
    pub fn invoke(&mut self, generation: u64, function: &str, input: &str) -> Outcome {
        let error = if generation != self.generation {
            Some(Error::Stale)
        } else if self.faulted {
            Some(Error::Faulted)
        } else {
            None
        };
        if let Some(error) = error {
            return Outcome {
                generation,
                outputs: Vec::new(),
                error: Some(error),
            };
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<(), Box<EvalAltResult>> {
                if input.len() > self.compiled.limits.string_bytes || function.len() > 256 {
                    return Err("invocation input exceeded".into());
                }
                if !self.initialized {
                    let _ = self
                        .compiled
                        .engine
                        .eval_ast_with_scope::<Dynamic>(&mut self.scope, &self.compiled.ast)?;
                    self.initialized = true;
                }
                if let Some(ctx) = &self.compiled.context {
                    let _ = self.compiled.engine.call_fn_with_options::<Dynamic>(
                        CallFnOptions::new()
                            .eval_ast(self.compiled.invocation_ast.is_some())
                            .rewind_scope(false),
                        &mut self.scope,
                        self.compiled
                            .invocation_ast
                            .as_ref()
                            .unwrap_or(&self.compiled.ast),
                        function,
                        (ctx.clone(), input.to_owned()),
                    )?;
                } else {
                    let _ = self.compiled.engine.call_fn_with_options::<Dynamic>(
                        CallFnOptions::new()
                            .eval_ast(self.compiled.invocation_ast.is_some())
                            .rewind_scope(false),
                        &mut self.scope,
                        self.compiled
                            .invocation_ast
                            .as_ref()
                            .unwrap_or(&self.compiled.ast),
                        function,
                        (input.to_owned(),),
                    )?;
                }
                Ok(())
            },
        ));
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Error::Execution(error.to_string())),
            Err(_) => Some(Error::Execution("native panic".into())),
        };
        self.faulted = error.is_some();
        let mut buffer = self
            .compiled
            .buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let outputs = std::mem::take(&mut buffer.outputs);
        buffer.bytes = 0;
        Outcome {
            generation: self.generation,
            outputs,
            error,
        }
    }
    /// Compile failure cannot touch this host; pass a successfully compiled replacement.
    pub fn reload(&mut self, compiled: Compiled) -> Result<u64, Error> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(Error::GenerationExhausted)?;
        *self = Self::new(compiled);
        self.generation = generation;
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_panic_faults_only_its_host_without_replaying_emitted_prefix() {
        let mut compiled = Compiled::new(
            "fn input(v) {publish(v); crash();}",
            Limits::default(),
            true,
        )
        .unwrap();
        fn crash() {
            panic!("injected native panic")
        }
        compiled.engine.register_fn("crash", crash);
        let mut host = Host::new(compiled);
        let result = host.invoke(1, "input", "before-panic");
        assert_eq!(result.outputs, vec![Output::Publish("before-panic".into())]);
        assert_eq!(result.error, Some(Error::Execution("native panic".into())));
        assert_eq!(host.invoke(1, "input", "retry").error, Some(Error::Faulted));
        let mut other =
            Host::new(Compiled::new("fn input(v) {send(v);}", Limits::default(), true).unwrap());
        assert_eq!(other.invoke(1, "input", "healthy").error, None);
    }
}

pub mod testing;
