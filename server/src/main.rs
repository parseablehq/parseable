use actix_web::{middleware, web, App, HttpServer, Error};
use actix_web::dev::ServiceRequest;
use actix_web_httpauth::extractors::basic::BasicAuth;
use actix_web_httpauth::middleware::HttpAuthentication;
use env_logger;

mod handler;                                             
mod option;
mod storage;
mod banner;

// Init
// Read S3
// Fetch all schemas
// Local cache for schemas/stream
// config file validation

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    banner::print();
    env_logger::init();
    let opt = option::get_opts();
    HttpServer::new(|| {
        let auth = HttpAuthentication::basic(validator);
        // match opt.master_key {
        //     Some(key) => auth = HttpAuthentication::basic(validator),
        //     _ => auth = nu,
        // }
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(auth)
            .route("/{stream}", web::post().to(handler::post_event))
            .route("/{stream}", web::put().to(handler::put_stream))
    })
    .bind(opt.http_addr)?
    .run()
    .await
}

async fn validator(req: ServiceRequest, _credentials: BasicAuth) -> Result<ServiceRequest, Error> {
    // pass through for now
    Ok(req)
}

