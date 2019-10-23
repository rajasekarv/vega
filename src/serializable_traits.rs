use objekt;
use serde_traitobject::{deserialize, serialize, Deserialize, Serialize};
use std::{
    any,
    borrow::{Borrow, BorrowMut},
    boxed, error, fmt, marker,
    ops::{self, Deref, DerefMut},
};

// Data passing through RDD needs to satisfy the following traits. Default and Debug is not necessary.
// Default and Debug are added here only for debugging convenience during development stage. Sync is also not necessary I think. Have to look into it.
pub trait Data:
    Clone
    + any::Any
    + Send
    + Sync
    + fmt::Debug
    + serde::ser::Serialize
    + serde::de::DeserializeOwned
    + 'static
{
}
impl<
        T: Clone
            + any::Any
            + Send
            + Sync
            + fmt::Debug
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    > Data for T
{
}

pub trait AnyData:
    objekt::Clone + any::Any + Send + Sync + fmt::Debug + Serialize + Deserialize + 'static
{
    fn as_any(&self) -> &dyn any::Any;
    /// Convert to a `&mut std::any::Any`.
    fn as_any_mut(&mut self) -> &mut dyn any::Any;
    /// Convert to a `std::boxed::Box<dyn std::any::Any>`.
    fn into_any(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send>`.
    fn into_any_send(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Sync>`.
    fn into_any_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Sync>;
    /// Convert to a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
    fn into_any_send_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send + Sync>;
}

objekt::clone_trait_object!(AnyData);

// Automatically implementing the Data trait for all types which implements the required traits
impl<
        T: objekt::Clone + any::Any + Send + Sync + fmt::Debug + Serialize + Deserialize + 'static,
    > AnyData for T
{
    fn as_any(&self) -> &dyn any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
    fn into_any(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any> {
        self
    }
    fn into_any_send(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send> {
        self
    }
    fn into_any_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Sync> {
        self
    }
    fn into_any_send_sync(self: boxed::Box<Self>) -> boxed::Box<dyn any::Any + Send + Sync> {
        self
    }
}

impl serde::ser::Serialize for boxed::Box<dyn AnyData + 'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize(&self, serializer)
    }
}
impl<'de> serde::de::Deserialize<'de> for boxed::Box<dyn AnyData + 'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Box<dyn AnyData + 'static>>::deserialize(deserializer).map(|x| x.0)
    }
}
impl serde::ser::Serialize for boxed::Box<dyn AnyData + Send + 'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize(&self, serializer)
    }
}
impl<'de> serde::de::Deserialize<'de> for boxed::Box<dyn AnyData + Send + 'static> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <Box<dyn AnyData + Send + 'static>>::deserialize(deserializer).map(|x| x.0)
    }
}

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Box<T: ?Sized>(boxed::Box<T>);
impl<T> Box<T> {
    /// Create a new Box wrapper
    pub fn new(t: T) -> Self {
        Self(boxed::Box::new(t))
    }
}
impl<T: ?Sized> Box<T> {
    /// Convert to a regular `std::Boxed::Box<T>`. Coherence rules prevent currently prevent `impl Into<std::boxed::Box<T>> for Box<T>`.
    pub fn into_box(self) -> boxed::Box<T> {
        self.0
    }
}
impl Box<dyn AnyData> {
    /// Convert into a `std::boxed::Box<dyn std::any::Any>`.
    pub fn into_any(self) -> boxed::Box<dyn any::Any> {
        self.0.into_any()
    }
}
impl Box<dyn AnyData + Send> {
    /// Convert into a `std::boxed::Box<dyn std::any::Any + Send>`.
    pub fn into_any_send(self) -> boxed::Box<dyn any::Any + Send> {
        self.0.into_any_send()
    }
}
impl Box<dyn AnyData + Sync> {
    /// Convert into a `std::boxed::Box<dyn std::any::Any + Sync>`.
    pub fn into_any_sync(self) -> boxed::Box<dyn any::Any + Sync> {
        self.0.into_any_sync()
    }
}
impl Box<dyn AnyData + Send + Sync> {
    /// Convert into a `std::boxed::Box<dyn std::any::Any + Send + Sync>`.
    pub fn into_any_send_sync(self) -> boxed::Box<dyn any::Any + Send + Sync> {
        self.0.into_any_send_sync()
    }
}
impl<T: ?Sized + marker::Unsize<U>, U: ?Sized> ops::CoerceUnsized<Box<U>> for Box<T> {}
impl<T: ?Sized> Deref for Box<T> {
    type Target = boxed::Box<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: ?Sized> DerefMut for Box<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<T: ?Sized> AsRef<boxed::Box<T>> for Box<T> {
    fn as_ref(&self) -> &boxed::Box<T> {
        &self.0
    }
}
impl<T: ?Sized> AsMut<boxed::Box<T>> for Box<T> {
    fn as_mut(&mut self) -> &mut boxed::Box<T> {
        &mut self.0
    }
}
impl<T: ?Sized> AsRef<T> for Box<T> {
    fn as_ref(&self) -> &T {
        &*self.0
    }
}
impl<T: ?Sized> AsMut<T> for Box<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}
impl<T: ?Sized> Borrow<T> for Box<T> {
    fn borrow(&self) -> &T {
        &*self.0
    }
}
impl<T: ?Sized> BorrowMut<T> for Box<T> {
    fn borrow_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}
impl<T: ?Sized> From<boxed::Box<T>> for Box<T> {
    fn from(t: boxed::Box<T>) -> Self {
        Self(t)
    }
}
// impl<T: ?Sized> Into<boxed::Box<T>> for Box<T> {
// 	fn into(self) -> boxed::Box<T> {
// 		self.0
// 	}
// }
impl<T> From<T> for Box<T> {
    fn from(t: T) -> Self {
        Self(boxed::Box::new(t))
    }
}
impl<T: error::Error> error::Error for Box<T> {
    fn description(&self) -> &str {
        error::Error::description(&**self)
    }
    #[allow(deprecated)]
    fn cause(&self) -> Option<&dyn error::Error> {
        error::Error::cause(&**self)
    }
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        error::Error::source(&**self)
    }
}
impl<T: fmt::Debug + ?Sized> fmt::Debug for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}
impl<T: fmt::Display + ?Sized> fmt::Display for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}
impl<A, F: ?Sized> ops::FnOnce<A> for Box<F>
where
    F: FnOnce<A>,
{
    type Output = F::Output;
    extern "rust-call" fn call_once(self, args: A) -> Self::Output {
        self.0.call_once(args)
    }
}
impl<A, F: ?Sized> ops::FnMut<A> for Box<F>
where
    F: FnMut<A>,
{
    extern "rust-call" fn call_mut(&mut self, args: A) -> Self::Output {
        self.0.call_mut(args)
    }
}
impl<A, F: ?Sized> ops::Fn<A> for Box<F>
where
    F: Fn<A>,
{
    extern "rust-call" fn call(&self, args: A) -> Self::Output {
        self.0.call(args)
    }
}
impl<T: Serialize + ?Sized + 'static> serde::ser::Serialize for Box<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize(&self.0, serializer)
    }
}
impl<'de, T: Deserialize + ?Sized + 'static> serde::de::Deserialize<'de> for Box<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize(deserializer).map(Self)
    }
}

pub trait Func<V, U>:
    Fn(V) -> U + 'static + Send + Sync + objekt::Clone + Serialize + Deserialize
{
    fn as_func(self: boxed::Box<Self>) -> boxed::Box<dyn Fn(V) -> U>;
}
//objekt::clone_trait_object!(Func<V: Data,U: Data>);

impl<V, U, T: Fn(V) -> U + 'static + Send + Sync + objekt::Clone + Serialize + Deserialize>
    Func<V, U> for T
{
    fn as_func(self: boxed::Box<Self>) -> boxed::Box<dyn Fn(V) -> U> {
        self
    }
}
