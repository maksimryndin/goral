pub mod sheet;
pub mod spreadsheet;
use crate::HyperConnector;
use google_sheets4::oauth2;
use http::response::Response;
use hyper::body::Body;
pub use spreadsheet::*;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct Metadata(HashMap<String, String>);
pub(crate) type HttpResponse = Response<Body>;
pub(crate) const DEFAULT_FONT: &str = "Verdana";
pub(crate) const DEFAULT_FONT_TEXT: &str = "Courier New";

impl Metadata {
    pub(crate) fn new(pairs: Vec<(&'static str, String)>) -> Self {
        let inner: HashMap<String, String> =
            pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Self(inner)
    }

    pub(crate) fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub(crate) fn contains(&self, other: &Self) -> bool {
        for k in other.0.keys() {
            if self.get(k) != other.get(k) {
                return false;
            }
        }
        true
    }
}

impl From<HashMap<String, String>> for Metadata {
    fn from(m: HashMap<String, String>) -> Self {
        Self(m)
    }
}

pub async fn get_google_auth(
    service_account_credentials_path: &str,
) -> (String, oauth2::authenticator::Authenticator<HyperConnector>) {
    let key = oauth2::read_service_account_key(service_account_credentials_path)
        .await
        .expect("failed to read service account credentials file");
    (
        key.project_id
            .clone()
            .expect("assert: service account has project id"),
        oauth2::ServiceAccountAuthenticator::builder(key)
            .build()
            .await
            .expect("failed to create Google API authenticator"),
    )
}
