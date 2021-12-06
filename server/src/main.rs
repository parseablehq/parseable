use actix_web::{middleware, web, App, HttpServer, Error};
use actix_web::dev::ServiceRequest;
use actix_web_httpauth::extractors::basic::BasicAuth;
use env_logger;


mod handler;                                             
mod option;
mod storage;
mod banner;
mod event;

// Init
// Read S3
// Fetch all schemas
// Local cache for schemas/stream
// config file validation

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    banner::print();
    let opt = option::get_opts();
    run_http(opt).await?;
    Ok(())
}

async fn run_http(
    opt: option::Opt,
) -> anyhow::Result<()> {
    env_logger::init();
    let opt_clone = opt.clone();
    let http_server = HttpServer::new(move || create_app!(opt_clone)).disable_signals();
    http_server.bind(&opt.http_addr)?.run().await?;
    Ok(())
}

async fn validator(req: ServiceRequest, _credentials: BasicAuth) -> Result<ServiceRequest, Error> {
    // pass through for now
    Ok(req)
}

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/v1/{stream}")
        .route(web::put().to(handler::put_stream))
        .route(web::post().to(handler::post_event)));
}

pub fn configure_auth(cfg: &mut web::ServiceConfig, opts: &option::Opt) {
    if opts.master_key.is_none() {
        cfg.app_data(validator);
    } else {
        cfg.app_data(validator);
    }
}

#[macro_export]
macro_rules! create_app {
    ($opt:expr) => {
        App::new()
            .configure(|cfg| configure_routes(cfg))
            .configure(|cfg| configure_auth(cfg, &$opt))
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
    };
}