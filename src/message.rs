use crate::handlers::MessageStorer;
use cassandra_cpp::{Cluster, Error, Session};
use r2d2::{ManageConnection, PooledConnection};
use std::future::{self, Future};
use std::pin::Pin;

#[derive(Debug)]
pub struct CassandraConnectionManager {
    addr: String,
}

impl CassandraConnectionManager {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_owned(),
        }
    }
}

impl ManageConnection for CassandraConnectionManager {
    type Connection = Session;
    type Error = cassandra_cpp::Error;
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Cluster::default().set_contact_points(&self.addr)?.connect()
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        return true;
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        if conn.get_metrics().available_connections == 0 {
            return Err(cassandra_cpp::Error::from_kind(
                cassandra_cpp::ErrorKind::Msg("no available connection".into()),
            ));
        }
        Ok(())
    }
}

pub(crate) struct CassandraStorer {
    sess: PooledConnection<CassandraConnectionManager>,
}

impl CassandraStorer {
    pub fn new(sess: PooledConnection<CassandraConnectionManager>) -> Result<Self, Error> {
        Ok(Self { sess })
    }
}

impl MessageStorer for CassandraStorer {
    type StoreOutput = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>;
    type LoadOutput = Pin<Box<dyn Future<Output = Result<Vec<String>, anyhow::Error>>>>;
    fn store(self, uid: i32, content: String) -> Self::StoreOutput {
        Box::pin(async move {
            let mut stmt = self
                .sess
                .prepare("INSERT INTO with_baby.messages (uid, content) VALUES (?, ?)")
                .map_err(|e| anyhow::Error::msg("failed to prepare cassandra statement"))?
                .await
                .map_err(|e| anyhow::Error::msg("failed to prepare cassandra statement"))?
                .bind();
            stmt.bind_int32(0, uid);
            stmt.bind_string(0, &content);
            self.sess
                .execute(&stmt)
                .await
                .map_err(|e| anyhow::Error::msg("failed to execute statement"))?;
            Ok(())
        })
    }
    fn load(self, uid: i32) -> Self::LoadOutput {
        Box::pin(async move {
            let mut stmt = self
                .sess
                .prepare("SELECT content FROM with_baby.messages WHERE uid = ?")
                .map_err(|e| {
                    anyhow::Error::msg(e.0.to_string())
                        .context("failed to prepare cassandra statement")
                })?
                .await
                .map_err(|e| {
                    anyhow::Error::msg(e.0.to_string())
                        .context("failed to prepare cassandra statement")
                })?
                .bind();
            stmt.bind_int32(0, uid).map_err(|e| {
                anyhow::Error::msg(e.0.to_string())
                    .context("failed to bind argument for cql statement")
            })?;
            let mut l = Vec::new();
            let result = self.sess.execute(&stmt).await.map_err(|e| {
                anyhow::Error::msg(e.0.to_string()).context("failed to execute statement")
            })?;
            for row in result.into_iter() {
                l.push(
                    row.get_column_by_name("content")
                        .map_err(|e| {
                            anyhow::Error::msg(e.0.to_string())
                                .context("failed to load message from cassandra")
                        })?
                        .to_string(),
                );
            }
            Ok(l)
        })
    }
}