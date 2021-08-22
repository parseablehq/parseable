use actix_web::{put, get, App, HttpRequest, HttpResponse, HttpServer};
mod stream;
mod file;
mod object;



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
    file::export_env();
    let err = object::aws_s3();
    println!("{:?}",err);
    HttpServer::new(|| App::new().service(put_stream).service(list_stream))
    .bind("127.0.0.1:8080")?
    .run()
    .await
}