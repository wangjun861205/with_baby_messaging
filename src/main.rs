use actix::{Actor, StreamHandler};
use actix_web::{
    self,
    rt::spawn,
    web::{Data, Payload},
    HttpRequest, HttpResponse,
};
use actix_web_actors::ws::{self, Message, ProtocolError, WebsocketContext};
use dotenv;
use r2d2::Pool;
use serde::{Deserialize, Serialize};
use serde_json;

mod handlers;
mod message;
mod status;

#[derive(Serialize, Deserialize)]
enum Type {
    HeartBeat,
    Logout,
    Send,
    Acknowledge,
}

#[derive(Serialize, Deserialize)]
enum Body {
    Logout { uid: i32 },
    Send { target: i32, content: String },
    HeartBeat { uid: i32 },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Command {
    HeartBeat { uid: i32 },
    Login { uid: i32 },
    Logout { uid: i32 },
    Send { target: i32, content: String },
    Acknowledge { msg_id: String },
}

struct IM<S: handlers::ConnStatusStorer, M: handlers::MessageStorer> {
    status_storer: S,
    message_broker: M,
}

impl<S, M> IM<S, M>
where
    S: handlers::ConnStatusStorer + std::marker::Unpin + 'static,
    M: handlers::MessageStorer + std::marker::Unpin + 'static,
{
    fn new(status_storer: S, message_broker: M) -> Self {
        Self {
            status_storer,
            message_broker,
        }
    }
}

impl<S, M> Actor for IM<S, M>
where
    S: handlers::ConnStatusStorer + std::marker::Unpin + 'static,
    M: handlers::MessageStorer + std::marker::Unpin + 'static,
{
    type Context = WebsocketContext<IM<S, M>>;
}

impl<S, M> StreamHandler<Result<Message, ProtocolError>> for IM<S, M>
where
    S: handlers::ConnStatusStorer + std::marker::Unpin + 'static,
    M: handlers::MessageStorer + std::marker::Unpin + 'static,
{
    fn handle(&mut self, item: Result<Message, ProtocolError>, ctx: &mut Self::Context) {
        if let Ok(m) = item {
            if let Message::Text(s) = m {
                if let Ok(cmd) = serde_json::from_str::<Command>(&s) {
                    match cmd {
                        Command::HeartBeat { uid } => {
                            handlers::handle_heart_beat(&mut self.status_storer, uid).unwrap();
                        }
                        Command::Login { uid } => {
                            spawn(async move { loop {} });
                        }
                        Command::Logout { uid } => {
                            handlers::handle_logout(&mut self.status_storer, uid).unwrap();
                        }
                        Command::Send { target, content } => {}
                        Command::Acknowledge { msg_id } => {}
                    }
                } else {
                    ctx.text("invalid command");
                }
            }
        }
    }
}

async fn index(
    req: HttpRequest,
    stream: Payload,
    cass_pool: Data<Pool<message::CassandraConnectionManager>>,
    redis_pool: Data<Pool<status::RedisConnectionManager>>,
) -> Result<HttpResponse, actix_web::Error> {
    let cass_conn = cass_pool
        .get()
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    let redis_conn = redis_pool
        .get()
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    let status_storer = status::RedisConnStatusStorer::new(redis_conn)
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
    let msg_storer = message::CassandraStorer::new(cass_conn).unwrap();
    let im = IM::new(status_storer, msg_storer);
    let resp = ws::start(im, &req, stream);
    resp
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().expect("failed to read .env");
    actix_web::HttpServer::new(move || {
        let cass_mgr = message::CassandraConnectionManager::new(
            &dotenv::var("CASSANDRA_ADDR").expect("failed to read CASSANDRA_ADDR"),
        );
        let cass_pool = r2d2::Pool::new(cass_mgr).expect("failed to connect to cassandra cluster");
        let redis_mgr = status::RedisConnectionManager::new(
            &dotenv::var("REDIS_ADDR").expect("failed to read REDIS_ADDR"),
        );
        let redis_pool = r2d2::Pool::new(redis_mgr);
        actix_web::App::new()
            .app_data(Data::new(cass_pool))
            .app_data(Data::new(redis_pool))
            .route("/ws", actix_web::web::get().to(index))
    })
    .bind("localhost:8000")?
    .run()
    .await
}
