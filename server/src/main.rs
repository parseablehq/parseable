use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};

mod s3;

fn read_config() -> s3::ConfigToml {
    s3::read_config("Config.toml")
}

async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    let stream_name_clone = stream_name.clone();
    let s3_bucket = read_config().s3.aws_bucket_name;
    let s3_client = s3::init_s3client(read_config());

    match s3::create_stream(Some(s3_client), s3_bucket, stream_name) {
        Ok(_) => HttpResponse::Ok().body(format!("Created Stream {}", stream_name_clone)),
        Err(_) => {
            HttpResponse::Ok().body(format!("Failed to create Stream {}", stream_name_clone))
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
