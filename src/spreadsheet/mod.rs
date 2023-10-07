pub(crate) mod sheet;
pub(crate) mod spreadsheet;
use google_sheets4::hyper::client::connect::HttpConnector;
use google_sheets4::hyper_rustls::HttpsConnector;
use google_sheets4::oauth2;
pub(crate) use sheet::*;
pub(crate) use spreadsheet::*;
pub(crate) type HyperConnector = HttpsConnector<HttpConnector>;

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
