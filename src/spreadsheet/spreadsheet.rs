use anyhow::Result;
use google_sheets4::api::{
    BatchUpdateSpreadsheetRequest, BatchUpdateSpreadsheetResponse, Spreadsheet,
};

use crate::spreadsheet::sheet::{
    sheet_headers, Header, Rows, Sheet, SheetId, SheetType, UpdateSheet, VirtualSheet,
};
use crate::HyperConnector;
use google_sheets4::{hyper, hyper_rustls, oauth2, Error, Sheets};

use crate::spreadsheet::Metadata;
use crate::Sender;
use chrono::Utc;
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::instrument;

// https://support.google.com/docs/thread/181288162/whats-the-maximum-amount-of-rows-in-google-sheets?hl=en
const GOOGLE_SPREADSHEET_MAXIMUM_CELLS: u64 = 10_000_000;

macro_rules! handle_error {
    // `()` indicates that the macro takes no argument.
    ($self:ident, $send_notification:ident, $expression:expr) => {
        match $expression {
            Err(e) => match e {
                // fatal
                Error::MissingAPIKey
                | Error::BadRequest(_)
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::FieldClash(_) => {
                    tracing::error!("{}", e);
                    $self.$send_notification.fatal(format!("Fatal error for Google API access\n```{}```", e)).await;
                    panic!("Fatal error for Google API access: `{}`", e);
                }
                Error::MissingToken(_) => {
                    let msg = format!("`MissingToken error` for Google API\nProbably server time skewed which is now `{}`\nSync server time with NTP", Utc::now());
                    tracing::error!(
                        "{}{}",
                        e, msg
                    );
                    $self.$send_notification.fatal(msg).await;
                    panic!("{}Probably server time skewed. Sync server time with NTP.", e);
                }
                // retry
                Error::HttpError(_)
                | Error::Io(_)
                | Error::Cancelled
                | Error::Failure(_)
                | Error::JsonDecodeError(_, _) => Err(e),
            },
            Ok(res) => Ok(res.1),
        }
    };
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
                    .https_or_http()
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

    pub(crate) fn spreadsheet_url(&self, spreadsheet_id: &str) -> String {
        format!("https://docs.google.com/spreadsheets/d/{}", spreadsheet_id)
    }

    pub(crate) fn sheet_url(&self, spreadsheet_id: &str, sheet_id: SheetId) -> String {
        format!(
            "https://docs.google.com/spreadsheets/d/{}#gid={}",
            spreadsheet_id, sheet_id
        )
    }

    #[instrument(skip(self))]
    async fn spreadsheet_meta(&self, spreadsheet_id: &str) -> Result<Spreadsheet> {
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
        Ok(handle_error!(self, send_notification, result)?)
    }

    fn calculate_usage(num_of_cells: i32) -> u8 {
        (100.0 * (num_of_cells as f64 / GOOGLE_SPREADSHEET_MAXIMUM_CELLS as f64)) as u8
    }

    fn calculate_cells(sheets: &Vec<Sheet>) -> i32 {
        sheets
            .iter()
            .fold(0, |acc, s| acc + s.number_of_cells().unwrap_or(0))
    }

    #[instrument(skip_all)]
    pub(crate) async fn sheets_filtered_by_metadata(
        &self,
        spreadsheet_id: &str,
        metadata: &Metadata,
    ) -> Result<(Vec<Sheet>, u8, u8)> {
        let response = self.spreadsheet_meta(spreadsheet_id).await.map_err(|e| {
            tracing::error!("{}", e);
            e
        })?;

        let mut total_cells: i32 = 0;
        let mut filtered_cells: i32 = 0;
        let sheets: Vec<Sheet> = response
            .sheets
            .expect("spreadsheet should contain sheets property even if no sheets")
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

    #[instrument(skip(self, sheets))]
    async fn _crud_sheets(
        &self,
        spreadsheet_id: &str,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<BatchUpdateSpreadsheetResponse> {
        let ranges: Vec<String> = sheets
            .iter()
            .filter(|s| s.sheet.sheet_type == SheetType::Grid)
            .map(|s| s.sheet.header_range_r1c1().unwrap())
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
        Ok(handle_error!(self, send_notification, result)?)
    }

    #[instrument(skip(self, sheets))]
    pub(crate) async fn crud_sheets(
        &self,
        spreadsheet_id: &str,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<()> {
        self._crud_sheets(spreadsheet_id, updates, sheets, data)
            .await
            .map_err(|e| {
                tracing::error!("{}", e);
                e
            })?;
        Ok(())
    }
}
