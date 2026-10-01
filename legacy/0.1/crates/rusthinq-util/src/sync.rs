//! Poison-recovering Mutex/RwLock with a parking_lot-like API over `std::sync`.
//!
//! Locks recover from poison (return the guard) so short critical sections keep
//! working after a panic in another thread — matching prior parking_lot usage.

use std::sync::{
    Mutex as StdMutex, MutexGuard, RwLock as StdRwLock, RwLockReadGuard, RwLockWriteGuard,
};

#[derive(Debug)]
pub struct Mutex<T: ?Sized> {
    inner: StdMutex<T>,
}

impl<T> Mutex<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            inner: StdMutex::new(value),
        }
    }

    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        self.inner.into_inner().unwrap_or_else(|e| e.into_inner())
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[derive(Debug, Default)]
pub struct RwLock<T: ?Sized> {
    inner: StdRwLock<T>,
}

impl<T> RwLock<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            inner: StdRwLock::new(value),
        }
    }

    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }
}
