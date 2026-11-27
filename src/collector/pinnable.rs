use crate::handle::Handle;
use crate::Guard;

#[allow(private_bounds)]
pub trait PinnableOnce<'a, T, H>
where
    T: Send,
    H: Handle<T> + ?Sized,
{
    fn pin_once(self) -> Guard<'a, T, H>
    where
        Self: 'a;
}

#[allow(private_bounds)]
pub trait Pinnable<T, H>
where
    T: Send,
    H: Handle<T> + ?Sized,
{
    fn pin(&'_ self) -> Guard<'_, T, H>;
}

impl<'a, T, H, X> PinnableOnce<'a, T, H> for &'a X
where
    T: Send,
    H: Handle<T> + ?Sized,
    X: Pinnable<T, H> + ?Sized,
{
    fn pin_once(self) -> Guard<'a, T, H>
    where
        Self: 'a,
    {
        self.pin()
    }
}
