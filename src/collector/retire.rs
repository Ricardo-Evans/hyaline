pub trait Retire<T> {
    fn retire(&self, data: T, operation: fn(T));
}

