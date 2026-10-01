//! L5 bounded, IL-agnostic Rhai execution. The caller owns worker scheduling and sinks.
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
    outputs: Vec<Output>,
    bytes: usize,
    limits: Limits,
    consumer: bool,
}
pub struct Compiled {
    engine: Engine,
    ast: AST,
    buffer: Arc<Mutex<Buffer>>,
    limits: Limits,
}
impl Compiled {
    /// Compile off the transport loop, before swapping the running generation.
    /// No filesystem resolver, MQTT factory, or IL validation is installed.
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
            outputs: Vec::new(),
            bytes: 0,
            limits: limits.clone(),
            consumer: consumer_enabled,
        }));
        let mut engine = Engine::new();
        engine.set_module_resolver(StaticModuleResolver::new());
        engine
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
                    if publish && !buffer.consumer {
                        return Err("consumer disabled".into());
                    }
                    let bytes = buffer
                        .bytes
                        .checked_add(value.len())
                        .ok_or_else(|| Box::<EvalAltResult>::from("output size overflow"))?;
                    if value.len() > buffer.limits.string_bytes
                        || bytes > buffer.limits.output_bytes
                        || buffer.outputs.len() >= buffer.limits.outputs
                    {
                        return Err("output capacity exceeded".into());
                    }
                    buffer.bytes = bytes;
                    buffer.outputs.push(if publish {
                        Output::Publish(value)
                    } else {
                        Output::Send(value)
                    });
                    Ok(())
                },
            );
        }
        let ast = engine
            .compile(source)
            .map_err(|error| Error::Compile(error.to_string()))?;
        Ok(Self {
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
                let _ = self.compiled.engine.call_fn_with_options::<Dynamic>(
                    CallFnOptions::new().eval_ast(false).rewind_scope(false),
                    &mut self.scope,
                    &self.compiled.ast,
                    function,
                    (input.to_owned(),),
                )?;
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
