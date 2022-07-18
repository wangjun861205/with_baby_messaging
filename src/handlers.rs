use anyhow::Error;
use chrono::Utc;
use std::future::Future;

pub(crate) trait ConnStatusStorer {
    fn set(&mut self, uid: i32, ts: i64) -> Result<(), Error>;
    fn get(&mut self, uid: i32) -> Result<Option<i64>, Error>;
    fn unset(&mut self, uid: i32) -> Result<(), Error>;
    fn set_init(&mut self, uid: i32) -> Result<(), Error>;
    fn get_init(&mut self, uid: i32) -> Result<bool, Error>;
    fn unset_init(&mut self, uid: i32) -> Result<(), Error>;
}

pub(crate) fn handle_heart_beat<S>(storer: &mut S, uid: i32) -> Result<(), Error>
where
    S: ConnStatusStorer,
{
    let now = Utc::now().timestamp();
    storer.set(uid, now)
}

pub(crate) fn handle_logout<S>(storer: &mut S, uid: i32) -> Result<(), Error>
where
    S: ConnStatusStorer,
{
    storer.unset(uid)
}

pub trait MessageStorer {
    type StoreOutput: Future<Output = Result<(), anyhow::Error>>;
    type LoadOutput: Future<Output = Result<Vec<String>, anyhow::Error>>;
    fn store(self, uid: i32, content: String) -> Self::StoreOutput;
    fn load(self, uid: i32) -> Self::LoadOutput;
}

pub(crate) async fn handler_send<S, M>(
    status_storer: &mut S,
    message_storer: &mut M,
    target: i32,
    content: String,
) -> Result<(), Error>
where
    S: ConnStatusStorer,
    M: MessageStorer,
{
    unimplemented!()
}
