use crate::spreadsheet::sheet::{Rows, Sheet, SheetId, SheetType, UpdateSheet, VirtualSheet};
use crate::spreadsheet::{HttpResponse, Metadata};
use crate::HyperConnector;
use crate::Sender;
use chrono::Utc;
use google_sheets4::api::{
    BatchUpdateSpreadsheetRequest, BatchUpdateSpreadsheetResponse, Spreadsheet,
};
use google_sheets4::{
    hyper::{self, Body},
    hyper_rustls, oauth2, Error, Result as SheetsResult, Sheets,
};
use http::response::Response;

// https://support.google.com/docs/thread/181288162/whats-the-maximum-amount-of-rows-in-google-sheets?hl=en
const GOOGLE_SPREADSHEET_MAXIMUM_CELLS: u64 = 10_000_000;

async fn handle_error<T>(
    spreadsheet: &SpreadsheetAPI,
    result: SheetsResult<(Response<Body>, T)>,
) -> Result<T, HttpResponse> {
    match result {
        Err(e) => match e {
            // fatal
            Error::MissingAPIKey | Error::BadRequest(_) | Error::FieldClash(_) => {
                tracing::error!("{}", e);
                spreadsheet
                    .send_notification
                    .fatal(format!("Fatal error for Google API access\n```{}```", e))
                    .await;
                panic!("Fatal error for Google API access: `{}`", e);
            }
            Error::MissingToken(_) => {
                let msg = format!("`MissingToken error` for Google API\nProbably server time skewed which is now `{}`\nSync server time with NTP", Utc::now());
                tracing::error!("{}{}", e, msg);
                spreadsheet.send_notification.fatal(msg).await;
                panic!(
                    "{}Probably server time skewed. Sync server time with NTP.",
                    e
                );
            }
            Error::UploadSizeLimitExceeded(actual, limit) => {
                let msg = format!("uploading to much data {actual} vs limit of {limit} bytes");
                tracing::error!("{}", msg);
                spreadsheet
                    .send_notification
                    .fatal(format!("Fatal error for Google API access\n```{msg}```"))
                    .await;
                panic!("Fatal error for Google API access: `{}`", msg);
            }
            // retry
            Error::Failure(v) => Err(v),
            Error::HttpError(v) => Err(Response::new(Body::from(v.to_string()))),
            Error::Io(v) => Err(Response::new(Body::from(v.to_string()))),
            Error::JsonDecodeError(_, v) => Err(Response::new(Body::from(v.to_string()))),
            Error::Cancelled => Err(Response::new(Body::from("cancelled"))),
        },
        Ok(res) => Ok(res.1),
    }
}

pub struct SpreadsheetAPI {
    hub: Sheets<HyperConnector>,
    send_notification: Sender,
}

impl SpreadsheetAPI {
    pub fn new(
        authenticator: oauth2::authenticator::Authenticator<HyperConnector>,
        send_notification: Sender,
    ) -> Self {
        let hub = Sheets::new(
            hyper::Client::builder().build(
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_only()
                    .enable_http1()
                    .build(),
            ),
            authenticator,
        );
        Self {
            hub,
            send_notification,
        }
    }

    pub(crate) fn sheet_url(&self, spreadsheet_id: &str, sheet_id: SheetId) -> String {
        format!(
            "https://docs.google.com/spreadsheets/d/{}#gid={}",
            spreadsheet_id, sheet_id
        )
    }

    async fn spreadsheet_meta(&self, spreadsheet_id: &str) -> Result<Spreadsheet, HttpResponse> {
        // first get all spreadsheet sheets properties without data
        // second for sheets in interest (by tab color) fetch headers
        // and last row timestamp??
        let result = self
            .hub
            .spreadsheets()
            .get(spreadsheet_id)
            .param("fields", "sheets.properties(sheetId,title,hidden,index,tabColorStyle,sheetType,gridProperties),sheets.developerMetadata")
            .doit()
            .await;
        tracing::debug!("{:?}", result);
        handle_error(self, result).await
    }

    fn calculate_usage(num_of_cells: i32) -> f32 {
        100.0 * (num_of_cells as f32 / GOOGLE_SPREADSHEET_MAXIMUM_CELLS as f32)
    }

    pub(crate) async fn sheets_filtered_by_metadata(
        &self,
        spreadsheet_id: &str,
        metadata: &Metadata,
    ) -> Result<(Vec<Sheet>, f32, f32), HttpResponse> {
        let response = self.spreadsheet_meta(spreadsheet_id).await.map_err(|e| {
            tracing::error!("{:?}", e);
            e
        })?;

        let mut total_cells: i32 = 0;
        let mut filtered_cells: i32 = 0;
        let sheets: Vec<Sheet> = response
            .sheets
            .expect("assert: spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .map(|s| s.into())
            .map(|s: Sheet| {
                total_cells += s.number_of_cells().unwrap_or(0);
                s
            })
            .filter(|s: &Sheet| s.metadata.contains(metadata))
            .map(|s: Sheet| {
                filtered_cells += s.number_of_cells().unwrap_or(0);
                s
            })
            .collect();

        Ok((
            sheets,
            Self::calculate_usage(total_cells),
            Self::calculate_usage(filtered_cells),
        ))
    }

    async fn _crud_sheets(
        &self,
        spreadsheet_id: &str,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<BatchUpdateSpreadsheetResponse, HttpResponse> {
        let ranges: Vec<String> = sheets
            .iter()
            .filter(|s| s.sheet.sheet_type == SheetType::Grid)
            .map(|s| {
                s.sheet
                    .header_range_r1c1()
                    .expect("assert: grid sheet has a header range")
            })
            .collect();

        // TODO calculate capacity properly
        let mut requests = Vec::with_capacity(sheets.len() * 5 + data.len() + updates.len());

        for update in updates.into_iter() {
            requests.append(&mut update.into_api_requests());
        }

        for s in sheets.into_iter() {
            requests.append(&mut s.into_api_requests())
        }

        for rows in data.into_iter() {
            requests.append(&mut rows.into_api_requests())
        }

        tracing::debug!("requests:\n{:?}", requests);

        let req = BatchUpdateSpreadsheetRequest {
            include_spreadsheet_in_response: Some(false),
            requests: Some(requests),
            response_ranges: Some(ranges),
            response_include_grid_data: Some(false),
        };

        let result = self
            .hub
            .spreadsheets()
            .batch_update(req, spreadsheet_id)
            .doit()
            .await;

        tracing::debug!("{:?}", result);
        handle_error(self, result).await
    }

    pub(crate) async fn crud_sheets(
        &self,
        spreadsheet_id: &str,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<(), HttpResponse> {
        self._crud_sheets(spreadsheet_id, updates, sheets, data)
            .await
            .map_err(|e| {
                tracing::error!("{:?}", e);
                e
            })?;
        Ok(())
    }
}
