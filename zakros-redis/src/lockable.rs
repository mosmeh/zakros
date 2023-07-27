use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
};

pub trait RwLockable<'a, T> {
    type ReadGuard: Deref<Target = T>;
    type WriteGuard: DerefMut<Target = T>;

    fn read(&'a self) -> Self::ReadGuard;
    fn write(&'a self) -> Self::WriteGuard;
}

impl<'a, T: 'a> RwLockable<'a, T> for RefCell<T> {
    type ReadGuard = Ref<'a, T>;
    type WriteGuard = RefMut<'a, T>;

    fn read(&'a self) -> Self::ReadGuard {
        self.borrow()
    }

    fn write(&'a self) -> Self::WriteGuard {
        self.borrow_mut()
    }
}

impl<'a, T: 'a> RwLockable<'a, T> for RwLock<T> {
    type ReadGuard = RwLockReadGuard<'a, T>;
    type WriteGuard = RwLockWriteGuard<'a, T>;

    fn read(&'a self) -> Self::ReadGuard {
        self.read()
    }

    fn write(&'a self) -> Self::WriteGuard {
        self.write()
    }
}

impl<'a, T: 'a> RwLockable<'a, T> for RefCell<RwLockWriteGuard<'_, T>> {
    type ReadGuard = Ref<'a, T>;
    type WriteGuard = RefMut<'a, T>;

    fn read(&'a self) -> Self::ReadGuard {
        Ref::map(self.borrow(), |s| s.deref())
    }

    fn write(&'a self) -> Self::WriteGuard {
        RefMut::map(self.borrow_mut(), |s| s.deref_mut())
    }
}

pub trait ReadLockable<'a, T> {
    type Guard: Deref<Target = T>;

    fn read(&'a self) -> Self::Guard;
}

impl<'a, T, L> ReadLockable<'a, T> for L
where
    T: 'a,
    L: RwLockable<'a, T>,
{
    type Guard = L::ReadGuard;

    fn read(&'a self) -> Self::Guard {
        self.read()
    }
}
