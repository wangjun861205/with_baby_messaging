use crate::handlers::ConnStatusStorer;
use anyhow::{Context, Error};
use r2d2::{ManageConnection, PooledConnection};
use redis::Commands;

fn activity_key(uid: i32) -> String {
    format!("user_activity_{}", uid)
}

fn initializing_key(uid: i32) -> String {
    format!("user_initializing_{}", uid)
}

#[derive(Debug)]
pub struct RedisConnectionManager {
    addr: String,
}

impl RedisConnectionManager {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_owned(),
        }
    }
}

impl ManageConnection for RedisConnectionManager {
    type Connection = redis::Client;
    type Error = redis::RedisError;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        redis::Client::open(self.addr.clone())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.get_connection().is_ok()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.get_connection().map(|v| ())
    }
}

pub(crate) struct RedisConnStatusStorer {
    client: PooledConnection<RedisConnectionManager>,
}

impl RedisConnStatusStorer {
    pub(crate) fn new(client: PooledConnection<RedisConnectionManager>) -> Result<Self, Error> {
        Ok(Self { client })
    }
}

impl ConnStatusStorer for RedisConnStatusStorer {
    fn set(&mut self, uid: i32, ts: i64) -> Result<(), Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .set(activity_key(uid), ts)
            .context("failed to set redis value")
    }

    fn get(&mut self, uid: i32) -> Result<Option<i64>, Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .get(activity_key(uid))
            .context("failed to set redis value")
    }

    fn unset(&mut self, uid: i32) -> Result<(), Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .del(activity_key(uid))
            .context("fialed to delete redis value")
    }

    fn set_init(&mut self, uid: i32) -> Result<(), Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .set(initializing_key(uid), true)
            .context("fialed to delete redis value")
    }

    fn get_init(&mut self, uid: i32) -> Result<bool, Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .exists(initializing_key(uid))
            .context("fialed to delete redis value")
    }

    fn unset_init(&mut self, uid: i32) -> Result<(), Error> {
        self.client
            .get_connection()
            .context("failed to get redis connection")?
            .del(initializing_key(uid))
            .context("fialed to delete redis value")
    }
}
