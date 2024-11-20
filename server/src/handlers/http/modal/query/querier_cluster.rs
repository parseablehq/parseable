/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use actix_web::{HttpRequest, Responder};
use http::StatusCode;
use itertools::Itertools;
use relative_path::RelativePathBuf;

use crate::{
    handlers::http::{
        cluster::utils::{check_liveness, to_url_string},
        ingest::PostError,
        modal::{query::Method, IngestorMetadata, LEADER},
    },
    option::CONFIG,
    storage::{
        object_storage::ingestor_metadata_path, ObjectStorageError, PARSEABLE_ROOT_DIRECTORY,
    },
};

use super::LeaderRequest;

pub async fn remove_ingestor(req: HttpRequest) -> Result<impl Responder, PostError> {
    let domain_name: String = req.match_info().get("ingestor").unwrap().parse().unwrap();
    let domain_name = to_url_string(domain_name);

    if LEADER.lock().await.is_leader() {
        if check_liveness(&domain_name).await {
            return Err(PostError::Invalid(anyhow::anyhow!(
                "The ingestor is currently live and cannot be removed"
            )));
        }
        let object_store = CONFIG.storage().get_object_store();

        let ingestor_metadatas = object_store
            .get_objects(
                Some(&RelativePathBuf::from(PARSEABLE_ROOT_DIRECTORY)),
                Box::new(|file_name| file_name.starts_with("ingestor")),
            )
            .await?;

        let ingestor_metadata = ingestor_metadatas
            .iter()
            .map(|elem| serde_json::from_slice::<IngestorMetadata>(elem).unwrap_or_default())
            .collect_vec();

        let ingestor_metadata = ingestor_metadata
            .iter()
            .filter(|elem| elem.domain_name == domain_name)
            .collect_vec();

        let ingestor_meta_filename =
            ingestor_metadata_path(Some(&ingestor_metadata[0].ingestor_id)).to_string();

        match object_store
            .try_delete_ingestor_meta(ingestor_meta_filename)
            .await
        {
            Ok(_) => Ok(format!("Ingestor {} removed successfully", domain_name)),
            Err(err) => {
                if matches!(err, ObjectStorageError::IoError(_)) {
                    Err(PostError::ObjectStorageError(err))
                    // format!("Ingestor {} is not found", domain_name)
                } else {
                    Err(PostError::CustomError(err.to_string()))
                    // format!("Error removing ingestor {}\n Reason: {}", domain_name, err)
                }
            }
        }
    } else {
        let resource = domain_name.to_string();
        let request = LeaderRequest {
            body: None,
            api: "cluster",
            resource: Some(&resource),
            method: Method::Delete,
        };

        let res = request.request().await?;

        match res.status() {
            StatusCode::OK => Ok(res.text().await?),
            _ => {
                let err_msg = res.text().await?;
                Err(PostError::CustomError(err_msg.to_string()))
            }
        }
    }
}
