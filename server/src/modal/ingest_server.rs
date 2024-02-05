
use super::parseable_server::ParseableServer;
use actix_web_prometheus::PrometheusMetrics;
use async_trait::async_trait;

use crate::{
    option::CONFIG,
};

pub struct IngestServer;




            App::new()
                .wrap(prometheus.clone())
                .wrap(actix_web::middleware::Logger::default())
                .wrap(actix_web::middleware::Compress::default())
                .wrap(cross_origin_config())

                .run()
                .await?;
        } else {
        }

        Ok(())
    }
}
