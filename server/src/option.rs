use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
pub struct Opt {
    /// The address on which the http server will listen.
    #[structopt(long, env = "JET_ADDR", default_value = "127.0.0.1:5678")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[structopt(long, env = "JET_MASTER_KEY")]
    pub master_key: Option<String>,

    /// The local storage path is used as temporary landing point
    /// for incoming events and local cache while querying data pulled
    /// from object storage backend
    #[structopt(long, env = "JET_LOCAL_STORAGE", default_value = "./data")]
    pub local_disk_path: String,

    /// The endpoint to AWS S3 or compatible object storage platform
    #[structopt(long, env = "AWS_ENDPOINT_URL", default_value = "https://play.minio.io/")]
    pub s3_endpoint_url: String,

    /// The access key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "AWS_ACCESS_KEY_ID", default_value = "Q3AM3UQ867SPQQA43P2F")]
    pub s3_access_key_id: String,

    /// The secret key for AWS S3 or compatible object storage platform
    #[structopt(long, env = "AWS_SECRET_ACCESS_KEY", default_value = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")]
    pub s3_secret_key: String,

    /// The region for AWS S3 or compatible object storage platform
    #[structopt(long, env = "AWS_DEFAULT_REGION", default_value = "us-east-1")]
    pub s3_default_region: String,
    
    /// The AWS S3 or compatible object storage bucket to be used for storage
    #[structopt(long, env = "AWS_BUCKET_NAME", default_value = "67111b0f870e443ca59200b51221243b")]
    pub s3_bucket_name: String,
}

pub fn get_opts() -> Opt {
    Opt::from_args()
}
