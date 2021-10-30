use actix_web::{web, App, HttpServer};
#[macro_use]
extern crate serde_derive;


mod handler;                                             
mod config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/{stream}", web::post().to(handler::post_event))
            .route("/{stream}", web::put().to(handler::put_stream))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
