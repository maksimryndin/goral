use anyhow::Result;
use google_sheets4::api::{
    AddSheetRequest, BasicFilter, BatchUpdateSpreadsheetRequest, BatchUpdateSpreadsheetResponse,
    CellData, CreateDeveloperMetadataRequest, GridProperties, GridRange, Request, RowData,
    SetBasicFilterRequest, SheetProperties, Spreadsheet, UpdateCellsRequest,
};

use crate::spreadsheet::sheet::{sheet_headers, Header, Sheet, SheetId, SheetType, VirtualSheet};
use crate::spreadsheet::HyperConnector;
use google_sheets4::{hyper, hyper_rustls, oauth2, Error, FieldMask, Sheets};

use std::collections::{BTreeMap, HashSet};

use std::str::FromStr;
use std::time::Duration;
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::Retry;
use tracing::instrument;

// https://support.google.com/docs/thread/181288162/whats-the-maximum-amount-of-rows-in-google-sheets?hl=en
const GOOGLE_SPREADSHEET_MAXIMUM_CELLS: u64 = 10_000_000;

macro_rules! handle_error {
    // `()` indicates that the macro takes no argument.
    ($expression:expr) => {
        match $expression {
            Err(e) => match e {
                // fatal
                Error::MissingAPIKey
                | Error::BadRequest(_)
                | Error::UploadSizeLimitExceeded(_, _)
                | Error::FieldClash(_) => {
                    tracing::error!("{}", e);
                    // TODO perhaps don't panic, just return - module decides to notify by messenger
                    panic!("{}", e);
                }
                Error::MissingToken(_) => {
                    tracing::error!(
                        "{}. Probably server time skewed. Sync server time with NTP.",
                        e
                    );
                    // TODO perhaps don't panic, just return - module decides to notify by messenger
                    panic!("{}", e);
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

pub(crate) struct SpreadsheetAPI {
    hub: Sheets<HyperConnector>,
}

impl SpreadsheetAPI {
    pub(crate) fn new(authenticator: oauth2::authenticator::Authenticator<HyperConnector>) -> Self {
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
        Self { hub }
    }

    pub(crate) fn spreadsheet_url(&self, spreadsheet_id: &str) -> String {
        format!("https://docs.google.com/spreadsheets/d/{}", spreadsheet_id)
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
        tracing::info!("{:?}", result);
        Ok(handle_error!(result)?)
    }

    #[instrument(skip(self, sheets))]
    async fn sheets_headers(
        &self,
        spreadsheet_id: &str,
        sheets: &Vec<Sheet>,
    ) -> Result<BTreeMap<SheetId, Vec<Header>>> {
        // first get all spreadsheet sheets properties without data
        // second for sheets in interest (by tab color) fetch headers
        // and last row timestamp??
        let call = self.hub.spreadsheets().get(spreadsheet_id).param(
            "fields",
            "sheets.properties.sheetId,sheets.data.row_data.values(effective_value,note)",
        );
        let result = sheets
            .into_iter()
            .filter(|s| s.sheet_type == SheetType::Grid)
            .fold(call, |request, sheet| {
                request.add_ranges(&sheet.header_range_r1c1().unwrap())
            })
            .doit()
            .await;
        tracing::debug!("{:?}", result);
        let response = handle_error!(result)?;
        Ok(response
            .sheets
            .expect("spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .map(|mut s| {
                (
                    s.properties.as_ref().unwrap().sheet_id.unwrap(),
                    sheet_headers(&mut s),
                )
            })
            .collect())
    }

    #[instrument(skip(self))]
    pub(crate) async fn sheets_managed_by_service(
        &self,
        spreadsheet_id: &str,
        service: &str,
        host_id: &str,
    ) -> Result<Vec<Sheet>> {
        let retry_strategy = FibonacciBackoff::from_millis(100)
            .max_delay(Duration::from_secs(10))
            .map(jitter);

        let response = Retry::spawn(retry_strategy.clone(), || {
            self.spreadsheet_meta(spreadsheet_id)
        })
        .await
        .map_err(|e| {
            tracing::error!("{}", e);
            e
        })?;
        let mut sheets: Vec<Sheet> = response
            .sheets
            .expect("spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .map(|s| s.into())
            .filter(|s: &Sheet| s.host_id == host_id && s.service == service)
            .collect();

        let mut sheets_headers = Retry::spawn(retry_strategy, || {
            self.sheets_headers(spreadsheet_id, &sheets)
        })
        .await
        .map_err(|e| {
            tracing::error!("{}", e);
            e
        })?;

        // update headers for sheets
        sheets.iter_mut().for_each(|s| {
            s.headers = sheets_headers
                .remove(&s.sheet_id)
                .expect("managed sheet is not found")
        });

        let num_of_cells = sheets
            .iter()
            .fold(0, |acc, s| acc + s.number_of_cells().unwrap_or(0));
        let usage = (100.0 * (num_of_cells as f64 / GOOGLE_SPREADSHEET_MAXIMUM_CELLS as f64)) as u8;
        // TODO run sheets truncation here
        // deleting old sheets first
        tracing::debug!(
            "usage for spreadsheet {} is {}% ({} cells, {} sheets)",
            spreadsheet_id,
            usage,
            num_of_cells,
            sheets.len()
        );
        Ok(sheets)
    }

    #[instrument(skip(self, sheets))]
    async fn _create_sheets(
        &self,
        spreadsheet_id: &str,
        sheets: Vec<VirtualSheet>,
    ) -> Result<BatchUpdateSpreadsheetResponse> {
        let ranges: Vec<String> = sheets
            .iter()
            .filter(|s| s.sheet.sheet_type == SheetType::Grid)
            .map(|s| s.sheet.header_range_r1c1().unwrap())
            .collect();

        let mut requests = Vec::with_capacity(sheets.len() * 5);

        for s in sheets.into_iter() {
            let grid_properties = if s.sheet.sheet_type == SheetType::Grid {
                assert!(s.sheet.column_count.unwrap() > 0);
                Some(GridProperties {
                    column_count: s.sheet.column_count,
                    row_count: s.sheet.row_count,
                    frozen_row_count: s.sheet.frozen_row_count,
                    ..Default::default()
                })
            } else {
                None
            };

            let metadata = s.generate_developer_metadata();

            let range = GridRange {
                sheet_id: Some(s.sheet.sheet_id),
                start_row_index: Some(0),
                end_row_index: Some(1),
                start_column_index: Some(0),
                end_column_index: s.sheet.column_count,
            };
            let filter_range = GridRange {
                sheet_id: Some(s.sheet.sheet_id),
                start_row_index: Some(0),
                end_row_index: None,
                start_column_index: Some(0),
                end_column_index: s.sheet.column_count,
            };
            let header_values: Vec<CellData> =
                s.sheet.headers.into_iter().map(|h| h.into()).collect();
            let data = vec![RowData {
                values: Some(header_values),
            }];
            requests.push(Request {
                add_sheet: Some(AddSheetRequest {
                    properties: Some(SheetProperties {
                        sheet_id: Some(s.sheet.sheet_id),
                        hidden: Some(s.sheet.hidden),
                        sheet_type: Some(s.sheet.sheet_type.to_string()),
                        grid_properties,
                        title: Some(s.sheet.title),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            });
            requests.push(Request {
                update_cells: Some(UpdateCellsRequest {
                    fields: Some(
                        FieldMask::from_str("userEnteredValue,userEnteredFormat,note").unwrap(),
                    ),
                    range: Some(range),
                    rows: Some(data),
                    ..Default::default()
                }),
                ..Default::default()
            });
            requests.push(Request {
                set_basic_filter: Some(SetBasicFilterRequest {
                    filter: Some(BasicFilter {
                        range: Some(filter_range),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            });
            for m in metadata {
                requests.push(Request {
                    create_developer_metadata: Some(CreateDeveloperMetadataRequest {
                        developer_metadata: Some(m),
                    }),
                    ..Default::default()
                })
            }
        }

        let req = BatchUpdateSpreadsheetRequest {
            include_spreadsheet_in_response: Some(true),
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
        Ok(handle_error!(result)?)
    }

    fn batch_update_response_to_sheets(response: BatchUpdateSpreadsheetResponse) -> Vec<Sheet> {
        response
            .updated_spreadsheet
            .expect("spreadsheet should contain updated_spreadsheet property")
            .sheets
            .expect("spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .map(|s| s.into())
            .collect()
    }

    #[instrument(skip(self, sheets))]
    pub(crate) async fn add_sheets(
        &self,
        spreadsheet_id: &str,
        sheets: Vec<VirtualSheet>,
    ) -> Result<Vec<Sheet>> {
        // We do not retry sheet creation as this call usually goes after
        // `sheets_managed_by_service` which is retriable and should either fix an error
        // or fail.
        // Retrying `add_sheets` would require cloning all sheets at every retry attempt
        if sheets.is_empty() {
            return Ok(vec![]);
        }

        let sheet_titles_headers: HashSet<String> =
            sheets.iter().map(|s| s.sheet.title.to_string()).collect();

        let response = self
            ._create_sheets(spreadsheet_id, sheets)
            .await
            .map_err(|e| {
                tracing::error!("{}", e);
                e
            })?;
        let sheets = Self::batch_update_response_to_sheets(response);

        // sheets contain all sheets in the spreadsheet so have to filter
        let sheets = sheets
            .into_iter()
            .filter(|s| sheet_titles_headers.contains(&s.title))
            .collect();

        // TODO update cache for all sheets
        // Filter sheets
        Ok(sheets)
    }
}
