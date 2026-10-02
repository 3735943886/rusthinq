use super::*;
impl Runtime {
    pub(super) fn script_rejected(&self, device: String, reason: impl std::fmt::Debug) {
        self.emit(Event::Rejected {
            device,
            reason: format!("script: {reason:?}"),
        });
    }
    pub(super) fn script_transport(&mut self, event: &TransportEvent) {
        let (id, function, input) = match event {
            TransportEvent::Response(id, body) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.response.clone()),
                Ok(body.to_string()),
            ),
            TransportEvent::Ready(id, body) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.ready.clone()),
                Ok(body.to_string()),
            ),
            TransportEvent::Data(id, data) => (
                id,
                self.script_callbacks
                    .get(&id.device)
                    .and_then(|c| c.data.clone()),
                self.script_callbacks.get(&id.device).map_or_else(
                    || Ok(String::new()),
                    |callbacks| callbacks.data_encoding.encode(data),
                ),
            ),
            _ => return,
        };
        let Some(function) = function else {
            return;
        };
        let Some(owner) = self.scripts.as_mut() else {
            return;
        };
        let Some(generation) = owner.generation(&id.device) else {
            return;
        };
        let input = match input {
            Ok(input) => input,
            Err(error) => {
                self.script_rejected(id.device.clone(), error);
                return;
            }
        };
        let sequence = match self.script_schedule.next_sequence() {
            Ok(sequence) => sequence,
            Err(error) => {
                self.script_rejected(id.device.clone(), error);
                return;
            }
        };
        match owner.invoke(
            &self.model.devices(),
            &id.device,
            generation,
            function,
            input,
        ) {
            Ok(call) => {
                let device = id.device.clone();
                self.script_schedule
                    .admitted(device.clone(), sequence)
                    .expect("single owner reserves admission without yielding");
                self.script_results
                    .spawn(async move { (sequence, device, call.wait().await) });
            }
            Err(error) => self.script_rejected(id.device.clone(), error),
        }
    }
    pub(super) fn invoke_script(
        &mut self,
        device: String,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<u64, rusthinq_scripting::Error> {
        let owner = self
            .scripts
            .as_mut()
            .ok_or(rusthinq_scripting::Error::InvalidConfig)?;
        let sequence = self.script_schedule.next_sequence()?;
        let call = owner.invoke(&self.model.devices(), &device, generation, function, input)?;
        self.script_schedule
            .admitted(device.clone(), sequence)
            .expect("single owner reserves admission without yielding");
        self.script_results
            .spawn(async move { (sequence, device, call.wait().await) });
        Ok(sequence)
    }
    pub(super) fn fire_timers(&mut self) {
        for (device, name, context) in self.script_schedule.due_timers(Instant::now()) {
            if !self.model.devices().iter().any(|d| {
                d.entry.id == device
                    && d.session == Some(context.session)
                    && d.online
                    && d.removal.is_none()
            }) {
                continue;
            }
            if self
                .scripts
                .as_ref()
                .and_then(|owner| owner.generation(&device))
                != Some(context.generation)
            {
                continue;
            }
            if let Some(function) = self
                .script_callbacks
                .get(&device)
                .and_then(|c| c.timer.clone())
                && let Err(error) =
                    self.invoke_script(device.clone(), context.generation, function, name)
            {
                self.script_rejected(device, error);
            }
        }
    }
    pub(super) fn script_result(
        &mut self,
        sequence: u64,
        device: String,
        completion: Result<crate::scripts::Completion, rusthinq_scripting::Error>,
        dispatch: bool,
    ) {
        match self.script_schedule.complete(sequence, &device, completion) {
            Ok(ready) => {
                for (next, id, completion) in ready {
                    if dispatch {
                        self.script_complete(next, id, completion);
                    } else {
                        self.script_rejected(id, rusthinq_scripting::Error::Stopped);
                    }
                }
            }
            Err(error) => self.script_rejected(device, error),
        }
    }
    pub(super) fn script_complete(
        &mut self,
        sequence: u64,
        device: String,
        completion: Result<crate::scripts::Completion, rusthinq_scripting::Error>,
    ) {
        // L3 may already have closed/replaced a session before its broadcast is consumed.
        self.reconcile();
        let completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                self.script_rejected(device, error);
                return;
            }
        };
        let context = completion.context();
        let Some(owner) = self.scripts.as_mut() else {
            return;
        };
        let outcome = match owner.accept(&self.model.devices(), completion) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.script_rejected(device, error);
                return;
            }
        };
        self.emit(Event::ScriptExecuted {
            sequence,
            context: context.clone(),
            error: outcome.error.as_ref().map(|e| format!("{e:?}")),
        });
        self.shared
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                device.clone(),
                (context.session, context.generation, outcome.error.is_some()),
            );
        for output in outcome.outputs {
            let result = match output {
                rusthinq_scripting::Output::Publish(payload) => self
                    .script_sink
                    .as_ref()
                    .ok_or_else(|| "publication sink disabled".to_string())
                    .and_then(|sink| sink.try_publish(&context, payload)),
                rusthinq_scripting::Output::Send(payload) => {
                    if !self
                        .script_schedule
                        .delivery_available(self.script_deliveries.len())
                    {
                        Err("delivery capacity exceeded".into())
                    } else {
                        match self.server.send(
                            &SessionId {
                                device: device.clone(),
                                generation: context.session.generation,
                            },
                            payload.as_bytes(),
                        ) {
                            Ok(receipt) => {
                                let context = context.clone();
                                self.script_deliveries
                                    .spawn(async move { (context, receipt.wait().await) });
                                Ok(())
                            }
                            Err(error) => Err(format!("send: {error:?}")),
                        }
                    }
                }
                rusthinq_scripting::Output::Timer { name, after_ms } => {
                    let enabled = self
                        .script_callbacks
                        .get(&device)
                        .and_then(|c| c.timer.as_ref())
                        .is_some();
                    self.script_schedule.set_timer(
                        device.clone(),
                        name,
                        context.clone(),
                        after_ms,
                        enabled,
                        Instant::now(),
                    )
                }
            };
            if let Err(reason) = result {
                self.script_rejected(device.clone(), reason);
                break;
            }
        }
        if let Some(error) = outcome.error {
            self.script_rejected(device, error);
        }
    }
    pub(super) fn retirement_outputs(
        &mut self,
        outputs: Vec<(crate::scripts::Context, rusthinq_scripting::Outcome)>,
    ) {
        self.reconcile();
        for (context, outcome) in outputs {
            self.emit(Event::ScriptStopped {
                context: context.clone(),
                error: outcome.error.as_ref().map(|error| format!("{error:?}")),
            });
            let current = self.model.devices().iter().any(|device| {
                device.entry.id == context.device
                    && device.entry.incarnation == context.session.incarnation
                    && device.entry.last_generation == context.session.generation
                    && device.session.is_none()
                    && device.removal.is_none()
            });
            if !current {
                continue;
            }
            self.shared
                .retired_scripts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(context.device.clone(), context.clone());
            for output in outcome.outputs {
                let result = match output {
                    rusthinq_scripting::Output::Publish(payload) => self
                        .script_sink
                        .as_ref()
                        .ok_or_else(|| "publication sink disabled".to_string())
                        .and_then(|sink| sink.try_publish_retired(&context, payload)),
                    rusthinq_scripting::Output::Send(_) => {
                        Err("shutdown callback cannot send to device".into())
                    }
                    rusthinq_scripting::Output::Timer { .. } => {
                        Err("shutdown callback cannot schedule timers".into())
                    }
                };
                if let Err(reason) = result {
                    self.script_rejected(context.device.clone(), reason);
                    break;
                }
            }
            if let Some(error) = outcome.error {
                self.script_rejected(context.device, error);
            }
        }
    }
    pub(super) fn reap_completed_scripts(&mut self) {
        while let Some(owner) = self.scripts.as_mut() {
            owner.start_reaping();
            let Some(result) = owner.try_reap_next() else {
                break;
            };
            self.retirement_result(result);
        }
    }
    pub(super) fn retirement_result(
        &mut self,
        result: Result<
            (crate::scripts::Context, Option<rusthinq_scripting::Outcome>),
            rusthinq_scripting::Error,
        >,
    ) {
        match result {
            Ok((context, Some(outcome))) => self.retirement_outputs(vec![(context, outcome)]),
            Ok((_, None)) => {}
            Err(error) => self.script_rejected(String::new(), error),
        }
    }
    pub(super) fn attach_request(&mut self, mut request: Box<AttachScript>, stopped: bool) {
        if request.result.is_closed() {
            return;
        }
        self.reconcile();
        if !stopped
            && self
                .scripts
                .as_ref()
                .is_some_and(|owner| owner.retirement_pending())
        {
            self.pending_attach = Some(request);
            return;
        }
        request
            .compiled
            .set_consumer_enabled(self.script_sink.is_some());
        let result = if stopped {
            Err(rusthinq_scripting::Error::Stopped)
        } else if let Some(owner) = self.scripts.as_mut() {
            owner.attach(
                &self.model.devices(),
                request.device.clone(),
                request.session,
                request.compiled,
                request.config,
            )
        } else {
            Err(rusthinq_scripting::Error::InvalidConfig)
        };
        if result.is_ok() {
            let _ = self
                .scripts
                .as_ref()
                .expect("script owner")
                .set_shutdown_callback(&request.device, request.callbacks.shutdown.clone());
            self.shared
                .script_states
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(request.device.clone(), (request.session, 1, false));
            self.script_schedule
                .retain_timers(|id, _| id != request.device);
            self.script_callbacks
                .insert(request.device, request.callbacks);
        }
        let _ = request.result.send(result);
    }
}

impl Runtime {
    pub(super) fn prepare_scripts_turn(&mut self, stopping: bool) {
        self.reap_completed_scripts();
        if self.pending_attach.is_some()
            && !self
                .scripts
                .as_ref()
                .is_some_and(|owner| owner.retirement_pending())
        {
            let request = self.pending_attach.take().expect("pending attachment");
            self.attach_request(request, stopping);
            self.reap_completed_scripts();
        }
    }
    pub(super) async fn reload_request(
        &mut self,
        request: ReloadScript,
        stop: &watch::Receiver<bool>,
    ) {
        let mut request = request;
        if !request.result.is_closed() {
            if *stop.borrow() || stop.has_changed().is_err() {
                let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
                return;
            }
            self.reconcile();
            request
                .compiled
                .set_consumer_enabled(self.script_sink.is_some());
            let current = self.model.devices().iter().any(|device| {
                device.entry.id == request.device
                    && device.session == Some(request.session)
                    && device.removal.is_none()
            });
            let result = if !current {
                Err(rusthinq_scripting::Error::Stale)
            } else if request.initialize
                && let Err(error) = self.script_schedule.next_sequence()
            {
                Err(error)
            } else if let Some(owner) = self.scripts.as_mut() {
                match owner.reload(
                    &self.model.devices(),
                    &request.device,
                    request.generation,
                    request.compiled,
                ) {
                    Ok(reload) => reload.wait().await,
                    Err(error) => Err(error),
                }
            } else {
                Err(rusthinq_scripting::Error::InvalidConfig)
            };
            self.reconcile();
            let current = self.model.devices().iter().any(|device| {
                device.entry.id == request.device
                    && device.session == Some(request.session)
                    && device.removal.is_none()
            });
            let result = if *stop.borrow() || stop.has_changed().is_err() {
                Err(rusthinq_scripting::Error::Stopped)
            } else if !current {
                Err(rusthinq_scripting::Error::Stale)
            } else {
                result
            };
            if let Ok(generation) = result {
                self.shared
                    .script_states
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(request.device.clone(), (request.session, generation, false));
                self.script_schedule
                    .retain_timers(|id, _| id != request.device);
            }
            // Queue initialization before another actor turn can admit commands/data.
            let result = match result {
                Ok(generation) if request.initialize => self
                    .invoke_script(
                        request.device.clone(),
                        generation,
                        "__init".into(),
                        String::new(),
                    )
                    .map(|_| generation),
                other => other,
            };
            let _ = request.result.send(result);
        }
    }
    pub(super) fn prepare_reload_request(&mut self, request: PrepareReload) {
        if request.result.is_closed() {
            return;
        }
        let outcome = (|| {
            self.reconcile();
            let config = self
                .drivers
                .as_ref()
                .ok_or(rusthinq_scripting::Error::InvalidConfig)?
                .clone();
            if !self.model.devices().iter().any(|d| {
                d.entry.id == request.device
                    && d.session == Some(request.session)
                    && d.online
                    && d.removal.is_none()
            }) || self
                .scripts
                .as_ref()
                .and_then(|owner| owner.generation(&request.device))
                != Some(request.generation)
            {
                return Err(rusthinq_scripting::Error::Stale);
            }
            let (session, model, thinq2) = self
                .shared
                .driver_models
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&request.device)
                .cloned()
                .ok_or(rusthinq_scripting::Error::InvalidConfig)?;
            if session != request.session {
                return Err(rusthinq_scripting::Error::Stale);
            }
            if !rusthinq_scripting::preparation::Preparation::<SessionKey>::can_prepare(
                self.driver_preparation.len() + self.driver_reload_preparation.len(),
            ) {
                return Err(rusthinq_scripting::Error::Busy);
            }
            Ok((config, model, thinq2))
        })();
        match outcome {
            Err(error) => {
                let _ = request.result.send(Err(error));
            }
            Ok((config, model, thinq2)) => {
                self.driver_reload_preparation.spawn_blocking(move || {
                    let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        config.prepare(&request.device, &model, thinq2, true)
                    }))
                    .unwrap_or_else(|_| {
                        Err(rusthinq_scripting::Error::Compile(
                            "driver preparation panic".into(),
                        ))
                    });
                    (request, model, prepared)
                });
            }
        }
    }
    pub(super) async fn stop_scripts(&mut self) -> io::Result<()> {
        while let Some(result) = self.driver_reload_preparation.join_next().await {
            let (request, _, _) = result.map_err(io::Error::other)?;
            let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
        }
        while let Some(result) = self.driver_preparation.join_next().await {
            let _ = result.map_err(io::Error::other)?;
        }
        if let Some(scripts) = self.scripts.take() {
            match scripts.shutdown_with_outputs().await {
                Ok(outputs) => self.retirement_outputs(outputs),
                Err(error) => self.script_rejected(String::new(), error),
            }
        }
        if let Some(request) = self.pending_attach.take() {
            let _ = request.result.send(Err(rusthinq_scripting::Error::Stopped));
        }
        self.script_attach.close();
        Ok(())
    }
    pub(super) async fn drain_script_results(&mut self) -> io::Result<()> {
        while let Ok(request) = self.script_attach.try_recv() {
            request.cancel();
        }
        while let Some(result) = self.script_results.join_next().await {
            let (sequence, device, completion) = result.map_err(io::Error::other)?;
            self.script_result(sequence, device, completion, false);
        }
        while let Some(result) = self.script_deliveries.join_next().await {
            let (context, delivery) = result.map_err(io::Error::other)?;
            self.emit(Event::ScriptDelivery { context, delivery });
        }
        Ok(())
    }
    pub(super) fn reconcile_scripts(&mut self, devices: &[Device]) {
        if let Some(scripts) = &mut self.scripts {
            scripts.reconcile(&self.model.devices());
        }
        self.shared
            .retired_scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, context| {
                devices.iter().any(|device| {
                    device.entry.id == context.device
                        && device.entry.incarnation == context.session.incarnation
                        && device.entry.last_generation == context.session.generation
                        && device.session.is_none()
                        && device.removal.is_none()
                })
            });
        self.script_schedule.retain_timers(|_, context| {
            devices.iter().any(|device| {
                device.entry.id == context.device
                    && device.session == Some(context.session)
                    && device.online
                    && device.removal.is_none()
            })
        });
        self.preparation_policy.retain(|id, session| {
            devices
                .iter()
                .any(|d| d.entry.id == id && d.session == Some(*session) && d.removal.is_none())
        });
        self.shared
            .script_states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|id, (session, _, _)| {
                devices.iter().any(|d| {
                    d.entry.id == *id && d.session == Some(*session) && d.removal.is_none()
                })
            });
        self.script_callbacks.retain(|id, _| {
            self.model.devices().iter().any(|device| {
                &device.entry.id == id && device.session.is_some() && device.removal.is_none()
            })
        });
    }
}

macro_rules! runtime_select { ($runtime:ident,$stop:ident,$stopping:ident; $($branches:tt)*) => { tokio::select! { biased;
                Some(result)=async {$runtime.scripts.as_mut().expect("script owner").reap_next().await}, if $runtime.scripts.as_ref().is_some_and(|owner|owner.has_reaping())=>{$runtime.retirement_result(result);},
                Some(result)=$runtime.driver_reload_preparation.join_next(), if !$runtime.driver_reload_preparation.is_empty() && !$stopping => {
                    let (request,model,prepared)=result.map_err(io::Error::other)?;
                    let current=$runtime.shared.driver_models.lock().unwrap_or_else(|e|e.into_inner())
                        .get(&request.device).is_some_and(|(session,current,_)|*session==request.session && *current==model);
                    if !current {let _=request.result.send(Err(rusthinq_scripting::Error::Stale));}
                    else {match prepared {
                        Err(error)=>{
                            $runtime.script_rejected(request.device.clone(),format!("driver reload preparation: {error:?}"));
                            let _=request.result.send(Err(error));
                        },
                        Ok(compiled)=>{$runtime.reload_request(ReloadScript {
                            initialize:true,device:request.device,session:request.session,generation:request.generation,
                            compiled,result:request.result,
                        },&$stop).await;},
                    }}
                },
                Some(result)=$runtime.driver_preparation.join_next(), if !$runtime.driver_preparation.is_empty() && !$stopping && !$runtime.scripts.as_ref().is_some_and(|owner|owner.retirement_pending())=>{
                    let (id,session,model,prepared)=result.map_err(io::Error::other)?;
                    $runtime.reconcile();
                    let current=$runtime.model.devices().iter().any(|d|d.entry.id==id && d.session==Some(session) && d.online && d.removal.is_none()) && $runtime.preparation_policy.is_current(&id,&session,&model);
                    if current {
                        let result=match prepared {
                            Err(error)=>Err(error),
                            Ok(mut compiled)=>{
                                compiled.set_consumer_enabled($runtime.script_sink.is_some());
                                $runtime.reconcile();
                                if !$runtime.scripts.as_ref().expect("driver owner").can_attach() && $runtime.scripts.as_ref().expect("driver owner").retirement_pending() {
                                    $runtime.driver_preparation.spawn(async move {(id,session,model,Ok(compiled))});
                                    continue;
                                }
                                let current=$runtime.model.devices().iter().any(|d|d.entry.id==id && d.session==Some(session) && d.online && d.removal.is_none());
                                if current && !*$stop.borrow() {$runtime.scripts.as_mut().expect("driver owner").attach(&$runtime.model.devices(),id.clone(),session,compiled,rusthinq_scripting::worker::Config {capacity:16,input_bytes:131072,source_bytes:524288})} else {Err(rusthinq_scripting::Error::Stale)}
                            }
                        };
                        if let Err(error)=result {$runtime.script_rejected(id.clone(),error);}
                        else {
                            $runtime.script_callbacks.insert(id.clone(),crate::scripts::Callbacks {response:Some("__response".into()),data:Some("__data".into()),ready:None,timer:Some("__timer".into()),shutdown:Some("__drop".into()),data_encoding:crate::scripts::DataEncoding::Hex});
                            let _=$runtime.scripts.as_ref().expect("driver owner").set_shutdown_callback(&id,Some("__drop".into()));
                            $runtime.shared.script_states.lock().unwrap_or_else(|e|e.into_inner()).insert(id.clone(),(session,1,false));
                            if let Err(error)=$runtime.invoke_script(id.clone(),1,"__init".into(),String::new()) {$runtime.script_rejected(id.clone(),error);}
                            {let queue=$runtime.preparation_policy.take_buffer(&id,&session);
                                for data in queue {if let Err(error)=$runtime.invoke_script(id.clone(),1,"__data".into(),rusthinq_protocol::hex::encode(data)) {$runtime.script_rejected(id.clone(),error);break;}}
                            }
                        }
                    }
                    for device in $runtime.model.devices() {$runtime.prepare_driver(&device.entry.id);}
                },
                Some(result) = $runtime.script_deliveries.join_next(), if !$runtime.script_deliveries.is_empty() => {
                    let (context, delivery) = result.map_err(io::Error::other)?;
                    $runtime.emit(Event::ScriptDelivery {context, delivery});
                }
                Some(result) = $runtime.script_results.join_next(), if !$runtime.script_results.is_empty() => {
                    let (sequence, device, completion) = result.map_err(io::Error::other)?;
                    $runtime.script_result(sequence, device, completion, !$stopping && !*$stop.borrow() && $stop.has_changed().is_ok());
                }
                Some(command) = $runtime.script_attach.recv(), if !$stopping && $runtime.pending_attach.is_none() => match command {
                ScriptCommand::Invoke {device,session,generation,function,input,result}=>{
                    if !result.is_closed() {
                        $runtime.reconcile();
                        let current=$runtime.model.devices().iter().any(|d|d.entry.id==device && d.session==Some(session) && d.online && d.removal.is_none());
                        let outcome=if current {$runtime.invoke_script(device,generation,function,input)} else {Err(rusthinq_scripting::Error::Stale)};
                        let _=result.send(outcome);
                    }
                },
                ScriptCommand::Attach(request) => {$runtime.attach_request(request,*$stop.borrow() || $stop.has_changed().is_err());},
                ScriptCommand::Reload(request) => {$runtime.reload_request(*request, &$stop).await;},
                ScriptCommand::PrepareReload(request) => {$runtime.prepare_reload_request(request);},
                },
 $($branches)* } }; }
pub(super) use runtime_select;
