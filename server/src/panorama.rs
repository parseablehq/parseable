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

use arrow::pyarrow::PyArrowType;
use arrow_array::RecordBatch;
use once_cell::sync::Lazy;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyFloat, PyModule, PyString};
use serde::{Deserialize, Serialize};

use crate::handlers::http::panorama::{PanoramaFunction, PanoramaRequestBody};

pub static PANORAMA_SESSION: Lazy<Panorama> = Lazy::new(Panorama::init);

// TODO: How to pass it around in LazyStatic? Then we can have one init method
#[derive(Debug)]
pub struct Panorama {
    pub panorama_module: Py<PyModule>,
}

/// This struct represents one record for plotting
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct PanoramaRecord {
    pub ds: Option<String>,
    pub yhat: Option<f64>,
    pub yhat_lower: Option<f64>,
    pub yhat_upper: Option<f64>,
    pub y: Option<f64>,
}

impl FromPyObject<'_> for PanoramaRecord {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        // if the key 'ds' exists, then expect others to be present
        if let Ok(ds) = ob.get_item("ds") {
            let ds = ds.downcast_into::<PyString>().unwrap().to_string();
            let yhat = ob
                .get_item("yhat")
                .unwrap()
                .downcast_into::<PyFloat>()
                .unwrap()
                .to_string()
                .parse::<f64>()
                .unwrap();
            let yhat_lower = ob
                .get_item("yhat_lower")
                .unwrap()
                .downcast_into::<PyFloat>()
                .unwrap()
                .to_string()
                .parse::<f64>()
                .unwrap();
            let yhat_upper = ob
                .get_item("yhat_upper")
                .unwrap()
                .downcast_into::<PyFloat>()
                .unwrap()
                .to_string()
                .parse::<f64>()
                .unwrap();
            let y = match ob.get_item("y") {
                Ok(y) => Some(
                    y.downcast_into::<PyFloat>()
                        .unwrap()
                        .to_string()
                        .parse::<f64>()
                        .unwrap(),
                ),
                Err(e) => return Err(e),
            };

            // make the record object
            let record = PanoramaRecord {
                ds: Some(ds),
                yhat: Some(yhat),
                yhat_lower: Some(yhat_lower),
                yhat_upper: Some(yhat_upper),
                y,
            };
            Ok(record)
        } else {
            // make the record object
            let record = PanoramaRecord {
                ds: None,
                yhat: None,
                yhat_lower: None,
                yhat_upper: None,
                y: None,
            };
            Ok(record)
        }
    }
}

impl Panorama {
    pub fn init() -> Self {
        Python::with_gil(|py| {
            let panorama_module = PyModule::from_code_bound(
                py,
                &std::fs::read_to_string("../panorama/python_assets/panorama_module.py").unwrap(),
                "panorama_module.py",
                "panorama_module",
            )
            .unwrap()
            .as_unbound()
            .clone();

            Panorama { panorama_module }
        })
    }
    pub fn python_function(
        &self,
        py: Python,
        body: PanoramaRequestBody,
        records: Option<Vec<RecordBatch>>,
        _fields: Option<Vec<String>>,
    ) -> PyResult<Vec<PanoramaRecord>> {
        let function = self.panorama_module.getattr(py, "switch_function")?;
        let res: Vec<PanoramaRecord> = match body.function {
            PanoramaFunction::Train => {
                // records are expected to be not none
                if records.is_some() {
                    let records: PyArrowType<Vec<RecordBatch>> = PyArrowType(records.unwrap());
                    function
                        .call1(
                            py,
                            (
                                format!("{}", body.function),
                                body.stream.to_string(),
                                format!("{}", body.model_type),
                                body.train_start,
                                body.train_end,
                                format!("{}", body.aggregation_period),
                                None::<String>,
                                None::<String>,
                                records,
                            ),
                        )?
                        .extract(py)?
                } else {
                    return Err(PyTypeError::new_err("Records not present to train"));
                }
            }
            PanoramaFunction::Forecast => function
                .call1(
                    py,
                    (
                        format!("{}", body.function),
                        body.stream.to_string(),
                        format!("{}", body.model_type),
                        body.train_start,
                        body.train_end,
                        format!("{}", body.aggregation_period),
                        body.pred_start,
                        body.pred_end,
                        None::<String>,
                    ),
                )?
                .extract(py)?,
            PanoramaFunction::DetectAnomaly => {
                // records are expected to be not none
                if records.is_some() {
                    let records: PyArrowType<Vec<RecordBatch>> = PyArrowType(records.unwrap());
                    function
                        .call1(
                            py,
                            (
                                format!("{}", body.function),
                                body.stream.to_string(),
                                format!("{}", body.model_type),
                                body.train_start,
                                body.train_end,
                                format!("{}", body.aggregation_period),
                                None::<String>,
                                None::<String>,
                                records,
                            ),
                        )?
                        .extract(py)?
                } else {
                    return Err(PyTypeError::new_err("Records not present to train"));
                }
            }
        };
        Ok(res)
    }
}
