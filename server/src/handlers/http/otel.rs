use nom::number::complete::double;
use serde_json::Value;
use bytes::Bytes;
mod log;
mod common;
mod resource;
use std::collections::BTreeMap;



pub fn flatten_otel_logs(body: Bytes) -> Vec<BTreeMap<String, Value>> {
    let mut vec_otel_json: Vec<BTreeMap<String, Value>> = Vec::new();
    let body_str = std::str::from_utf8(&body).unwrap();

    let message: log::LogsData = serde_json::from_str(body_str).unwrap();
    let mut otel_json: BTreeMap<String, Value> = BTreeMap::new();
    for records in message.resource_logs{
        
        for record in records.iter(){
            println!("record: {:?}", record);
            let mut resource_attributes_json: BTreeMap<String, Value> = BTreeMap::new();
            for resource in record.resource.iter(){
                println!("resource: {:?}", resource);
                let attributes = &resource.attributes;
                let mut attributes_json: BTreeMap<String, String> = BTreeMap::new();
                for attributes in attributes.iter(){
                    println!("attributes: {:?}", attributes);
                    for attribute in attributes{
                        println!("attribute: {:?}", attribute);
                        let key = &attribute.key;
                        let value = &attribute.value;
                        println!("key: {:?}", key);
                        println!("value: {:?}", value);
                        
                        // for value in value.iter(){
                        //     println!("value: {:?}", value);
                            
                        //     if value.str_val.is_some(){
                        //         attributes_json.insert(key.to_owned(), value.str_val.unwrap());
                        //     }
                        //     if value.bool_val.is_some(){
                        //         attributes_json.insert(key.to_owned(), value.bool_val.unwrap());
                        //     }
                        //     if value.int_val.is_some(){
                        //         attributes_json.insert(key.to_owned(), value.int_val.unwrap());
                        //     }
                        //     if value.double_val.is_some(){
                        //         attributes_json.insert(key.to_owned(), value.double_val.unwrap());
                        //     }
                        //     // if value.array_val.is_some(){
                        //     //     attributes_json.insert(key.to_owned(), value.array_val.unwrap());
                        //     // }
                        //     // if value.kv_list_val.is_some(){
                        //     //     attributes_json.insert(key.to_owned(), value.kv_list_val.unwrap());
                        //     // }
                        //     if value.bytes_val.is_some(){
                        //         attributes_json.insert(key.to_owned(), value.bytes_val.unwrap());
                        //     }
                        
                        // }
                    }
                }
            }
            
            
            
        }
    }
    vec_otel_json
}