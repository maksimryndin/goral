pub(crate) mod sheet;
pub(crate) mod spreadsheet;
use google_sheets4::hyper::client::connect::HttpConnector;
use google_sheets4::hyper_rustls::HttpsConnector;
use google_sheets4::oauth2;
pub(crate) use spreadsheet::*;
use std::collections::HashMap;
pub(crate) type HyperConnector = HttpsConnector<HttpConnector>;

#[derive(Debug)]
pub(crate) struct Metadata(HashMap<String, String>);

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

pub(crate) async fn get_google_auth(
    service_account_credentials_path: &str,
) -> oauth2::authenticator::Authenticator<HyperConnector> {
    let key = oauth2::read_service_account_key(service_account_credentials_path)
        .await
        .expect("failed to read service account credentials file");
    oauth2::ServiceAccountAuthenticator::builder(key)
        .build()
        .await
        .expect("failed to create Google API authenticator")
}
