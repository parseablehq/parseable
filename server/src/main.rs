use actix_web::{put, get, App, HttpRequest, HttpResponse, HttpServer};
mod stream;
mod s3;



#[put("/{stream}")]
async fn put_stream(req: HttpRequest) -> HttpResponse {
    let stream_name: String = req.match_info().get("stream").unwrap().parse().unwrap();
    let stream_name_clone = stream_name.clone();
    match stream::insert_stream(stream_name, stream::Stream::empty()) {
        None => HttpResponse::Ok().body(format!("Created Stream {}", stream_name_clone)),
        Some(_) =>   {
            HttpResponse::Ok().body(format!("Updated Stream {}", stream_name_clone))
        } 
    }
}

#[get("/list")]
async fn list_stream() -> HttpResponse {
    let map = stream::STREAMS.lock().unwrap();
    for  (k, _) in map.iter() {
        println!("key={}", k);
    }
    HttpResponse::Ok().body("Listed Stream")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let read_config = s3::read_config("Config.toml");
    let init_s3client = s3::init_s3client(read_config);
    let create_stream = s3::create_stream(init_s3client.0,init_s3client.1, "stream_name");
    println!("{:?}", create_stream);
    HttpServer::new(|| App::new().service(put_stream).service(list_stream))
    .bind("127.0.0.1:8080")?
    .run()
    .await
}