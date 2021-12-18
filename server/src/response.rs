use actix_web::dev::HttpResponseBuilder;
use actix_web::HttpResponse;

pub struct ServerResponse {
    pub http_response: HttpResponseBuilder,
    pub msg: String,
}

impl ServerResponse {
    pub fn success_server_response(&self) -> HttpResponse {
        log::info!("{}", self.msg);
        HttpResponse::Ok().body(format!("{}", self.msg))
    }
    pub fn error_server_response(&self) -> HttpResponse {
        log::error!("{}", self.msg);
        HttpResponse::Ok().body(format!("{}", self.msg))
    }
}
