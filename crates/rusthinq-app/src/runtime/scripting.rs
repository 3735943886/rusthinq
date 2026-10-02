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
        if self.script_results.len() + self.script_buffer.len() >= self.script_limit {
            self.script_rejected(id.device.clone(), "callback capacity exceeded");
            return;
        }
        let input = match input {
            Ok(input) => input,
            Err(error) => {
                self.script_rejected(id.device.clone(), error);
                return;
            }
        };
        let Some(sequence) = self.script_sequence.checked_add(1) else {
            self.script_rejected(id.device.clone(), "callback sequence exhausted");
            return;
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
                self.script_sequence = sequence;
                self.script_order
                    .entry(device.clone())
                    .or_default()
                    .push_back(sequence);
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
        if self.script_results.len() + self.script_buffer.len() >= self.script_limit {
            return Err(rusthinq_scripting::Error::Busy);
        }
        let sequence = self
            .script_sequence
            .checked_add(1)
            .ok_or(rusthinq_scripting::Error::GenerationExhausted)?;
        let call = owner.invoke(&self.model.devices(), &device, generation, function, input)?;
        self.script_sequence = sequence;
        self.script_order
            .entry(device.clone())
            .or_default()
            .push_back(sequence);
        self.script_results
            .spawn(async move { (sequence, device, call.wait().await) });
        Ok(sequence)
    }
    pub(super) fn fire_timers(&mut self) {
        let due: Vec<_> = self
            .script_timers
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= Instant::now())
            .map(|(key, _)| key.clone())
            .collect();
        for (device, name) in due {
            let (context, _) = self
                .script_timers
                .remove(&(device.clone(), name.clone()))
                .expect("due timer");
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
        self.script_buffer
            .insert(sequence, (device.clone(), completion));
        while let Some(next) = self
            .script_order
            .get(&device)
            .and_then(|queue| queue.front())
            .copied()
        {
            let Some((id, completion)) = self.script_buffer.remove(&next) else {
                break;
            };
            self.script_order
                .get_mut(&device)
                .expect("callback queue")
                .pop_front();
            if dispatch {
                self.script_complete(next, id, completion);
            } else {
                self.script_rejected(id, rusthinq_scripting::Error::Stopped);
            }
        }
        if self
            .script_order
            .get(&device)
            .is_some_and(|queue| queue.is_empty())
        {
            self.script_order.remove(&device);
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
                    if self.script_deliveries.len() >= self.script_limit {
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
                    let key = (device.clone(), name);
                    if let Some(after_ms) = after_ms {
                        if self
                            .script_callbacks
                            .get(&device)
                            .and_then(|c| c.timer.as_ref())
                            .is_none()
                        {
                            Err("timer callback disabled".into())
                        } else if !self.script_timers.contains_key(&key)
                            && self
                                .script_timers
                                .keys()
                                .filter(|(id, _)| id == &device)
                                .count()
                                >= 64
                        {
                            Err("timer capacity exceeded".into())
                        } else if let Some(deadline) =
                            Instant::now().checked_add(Duration::from_millis(after_ms))
                        {
                            self.script_timers.insert(key, (context.clone(), deadline));
                            Ok(())
                        } else {
                            Err("timer deadline exceeded".into())
                        }
                    } else {
                        self.script_timers.remove(&key);
                        Ok(())
                    }
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
            self.script_timers
                .retain(|(id, _), _| id != &request.device);
            self.script_callbacks
                .insert(request.device, request.callbacks);
        }
        let _ = request.result.send(result);
    }
}
