use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
#[macro_use]
extern crate serde_derive;

mod configs;
mod handler;                                             

extern crate config;

async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    let stream_name_clone = stream_name.clone();
    let s3_client = configs::ConfigToml::s3client();
    match handler::create_stream(Some(s3_client), stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Created Stream {}", stream_name_clone)),
        Err(_) => {
            HttpResponse::Ok().body(format!("Failed to create Stream"))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/{stream}", web::put().to(put_stream))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
