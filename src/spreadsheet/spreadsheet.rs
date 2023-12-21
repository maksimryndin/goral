use crate::spreadsheet::sheet::{Rows, Sheet, SheetId, UpdateSheet, VirtualSheet};
use crate::spreadsheet::{HttpResponse, Metadata};

#[cfg(not(test))]
use crate::HyperConnector;
use crate::Sender;
use chrono::Utc;
use google_sheets4::api::{
    BatchUpdateSpreadsheetRequest, BatchUpdateSpreadsheetResponse, Request, Spreadsheet,
};
#[cfg(not(test))]
use google_sheets4::{
    api::{
        BatchGetValuesByDataFilterRequest, DataFilter, DeveloperMetadataLookup,
        GetSpreadsheetByDataFilterRequest, GridRange, Sheets,
    },
    oauth2::authenticator::Authenticator,
};
use google_sheets4::{
    hyper::{self, Body},
    Error, Result as SheetsResult,
};
use http::response::Response;
use serde_json::Value;

#[cfg(test)]
use crate::spreadsheet::spreadsheet::tests::TestState;

// https://support.google.com/docs/thread/181288162/whats-the-maximum-amount-of-rows-in-google-sheets?hl=en
pub(crate) const GOOGLE_SPREADSHEET_MAXIMUM_CELLS: u64 = 10_000_000;
pub(crate) const GOOGLE_SPREADSHEET_MAXIMUM_CHARS_PER_CELL: usize = 50_000;

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
    send_notification: Sender,
    #[cfg(not(test))]
    hub: Sheets<HyperConnector>,
    #[cfg(test)]
    state: tokio::sync::Mutex<TestState>,
}

impl SpreadsheetAPI {
    #[cfg(not(test))]
    pub fn new(authenticator: Authenticator<HyperConnector>, send_notification: Sender) -> Self {
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

    #[cfg(test)]
    pub(crate) fn new(send_notification: Sender, state: TestState) -> Self {
        Self {
            send_notification,
            state: tokio::sync::Mutex::new(state),
        }
    }

    #[cfg(not(test))]
    async fn get(
        &self,
        spreadsheet_id: &str,
        metadata: &Metadata,
    ) -> SheetsResult<(Response<Body>, Spreadsheet)> {
        let filters: Vec<_> = metadata
            .iter()
            .map(|(k, v)| DataFilter {
                developer_metadata_lookup: Some(DeveloperMetadataLookup {
                    visibility: Some("PROJECT".to_string()),
                    metadata_key: Some(k.to_string()),
                    metadata_value: Some(v.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();
        let req = GetSpreadsheetByDataFilterRequest {
            data_filters: Some(filters),
            ..Default::default()
        };
        self
            .hub
            .spreadsheets()
            .get_by_data_filter(req, spreadsheet_id)
            .param("fields", "sheets.properties(sheetId,title,hidden,index,tabColorStyle,sheetType,gridProperties),sheets.developerMetadata")
            .doit()
            .await
    }

    #[cfg(not(test))]
    pub(crate) async fn get_sheet_data(
        &self,
        spreadsheet_id: &str,
        sheet_id: SheetId,
    ) -> Result<Vec<Vec<Value>>, HttpResponse> {
        let req = BatchGetValuesByDataFilterRequest {
            data_filters: Some(vec![DataFilter {
                grid_range: Some(GridRange {
                    sheet_id: Some(sheet_id),
                    start_row_index: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            major_dimension: Some("ROWS".to_string()),
            value_render_option: Some("UNFORMATTED_VALUE".to_string()),
            ..Default::default()
        };

        let result = self
            .hub
            .spreadsheets()
            .values_batch_get_by_data_filter(req, spreadsheet_id)
            .doit()
            .await;
        tracing::debug!("{:?}", result);
        let response = handle_error(self, result).await.map_err(|e| {
            tracing::error!("{:?}", e);
            e
        })?;
        Ok(match response.value_ranges {
            Some(r) => r
                .into_iter()
                .filter_map(|range| range.value_range.and_then(|r| r.values))
                .flatten()
                .collect::<Vec<Vec<Value>>>(),
            None => vec![vec![]],
        })
    }

    #[cfg(test)]
    pub(crate) async fn get_sheet_data(
        &self,
        spreadsheet_id: &str,
        sheet_id: SheetId,
    ) -> Result<Vec<Vec<Value>>, HttpResponse> {
        let mut state = self.state.lock().await;
        state.get_sheet_data(spreadsheet_id, sheet_id).await
    }

    #[cfg(test)]
    async fn get(
        &self,
        spreadsheet_id: &str,
        _metadata: &Metadata,
    ) -> SheetsResult<(Response<Body>, Spreadsheet)> {
        let mut state = self.state.lock().await;
        state.get(spreadsheet_id).await
    }

    #[cfg(not(test))]
    async fn update(
        &self,
        req: BatchUpdateSpreadsheetRequest,
        spreadsheet_id: &str,
    ) -> SheetsResult<(Response<Body>, BatchUpdateSpreadsheetResponse)> {
        self.hub
            .spreadsheets()
            .batch_update(req, spreadsheet_id)
            .doit()
            .await
    }

    #[cfg(test)]
    async fn update(
        &self,
        req: BatchUpdateSpreadsheetRequest,
        spreadsheet_id: &str,
    ) -> SheetsResult<(Response<Body>, BatchUpdateSpreadsheetResponse)> {
        let mut state = self.state.lock().await;
        state.update(req, spreadsheet_id).await
    }

    pub(crate) fn sheet_url(&self, spreadsheet_id: &str, sheet_id: SheetId) -> String {
        format!(
            "https://docs.google.com/spreadsheets/d/{}#gid={}",
            spreadsheet_id, sheet_id
        )
    }

    pub(crate) fn spreadsheet_baseurl(&self, spreadsheet_id: &str) -> String {
        format!(
            "https://docs.google.com/spreadsheets/d/{}#gid=",
            spreadsheet_id
        )
    }

    pub(crate) async fn sheets_filtered_by_metadata(
        &self,
        spreadsheet_id: &str,
        metadata: &Metadata,
    ) -> Result<Vec<Sheet>, HttpResponse> {
        let result = self.get(spreadsheet_id, metadata).await;

        tracing::debug!("{:?}", result);
        let response = handle_error(self, result).await.map_err(|e| {
            tracing::error!("{:?}", e);
            e
        })?;

        let sheets: Vec<Sheet> = response
            .sheets
            .expect("assert: spreadsheet should contain sheets property even if no sheets")
            .into_iter()
            .map(|s| s.into())
            .filter(|s: &Sheet| s.metadata.contains(metadata))
            .collect();

        Ok(sheets)
    }

    async fn _crud_sheets(
        &self,
        spreadsheet_id: &str,
        mut truncate: Vec<Request>,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<BatchUpdateSpreadsheetResponse, HttpResponse> {
        // TODO calculate capacity properly
        let mut requests =
            Vec::with_capacity(truncate.len() + sheets.len() * 5 + data.len() + updates.len());
        requests.append(&mut truncate);

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
            response_ranges: None,
            response_include_grid_data: Some(false),
        };

        let result = self.update(req, spreadsheet_id).await;

        tracing::debug!("{:?}", result);
        handle_error(self, result).await
    }

    pub(crate) async fn crud_sheets(
        &self,
        spreadsheet_id: &str,
        truncate: Vec<Request>,
        updates: Vec<UpdateSheet>,
        sheets: Vec<VirtualSheet>,
        data: Vec<Rows>,
    ) -> Result<(), HttpResponse> {
        self._crud_sheets(spreadsheet_id, truncate, updates, sheets, data)
            .await
            .map_err(|e| {
                tracing::error!("{:?}", e);
                e
            })?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::spreadsheet::sheet::tests::mock_sheet_with_properties;
    use google_sheets4::api::Sheet as GoogleSheet;
    use google_sheets4::api::{
        AddSheetRequest, AppendCellsRequest, BasicFilter, CreateDeveloperMetadataRequest,
        DeleteRangeRequest, DeleteSheetRequest, DeveloperMetadata, GridRange, Request,
        SetBasicFilterRequest, SetDataValidationRequest, UpdateCellsRequest,
        UpdateDeveloperMetadataRequest,
    };
    use hyper::{header, Body, Response as HyperResponse, StatusCode};
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use tokio::time::sleep;

    pub(crate) struct TestState {
        sheets: HashMap<SheetId, GoogleSheet>,
        sheet_titles: HashSet<String>,
        metadata: HashMap<i32, (SheetId, usize)>,
        respond_with_error: Option<Error>,
        basic_response_duration_millis: u64,
    }

    impl TestState {
        pub(crate) fn new(
            sheets: Vec<GoogleSheet>,
            respond_with_error: Option<Error>,
            basic_response_duration_millis: Option<u64>,
        ) -> Self {
            let mut sheet_titles = HashSet::with_capacity(sheets.len());
            let mut metadata = HashMap::with_capacity(sheets.len());
            let sheets: HashMap<SheetId, GoogleSheet> = sheets
                .into_iter()
                .map(|s| {
                    sheet_titles.insert(
                        s.properties
                            .as_ref()
                            .unwrap()
                            .title
                            .as_ref()
                            .unwrap()
                            .to_string(),
                    );
                    let sheet_id = s.properties.as_ref().unwrap().sheet_id.unwrap();
                    for (i, m) in s
                        .developer_metadata
                        .as_ref()
                        .unwrap_or(&vec![])
                        .iter()
                        .enumerate()
                    {
                        metadata.insert(m.metadata_id.unwrap(), (sheet_id, i));
                    }
                    (sheet_id, s)
                })
                .collect();
            Self {
                sheets,
                sheet_titles,
                metadata: HashMap::new(),
                respond_with_error,
                basic_response_duration_millis: basic_response_duration_millis.unwrap_or(200),
            }
        }

        pub(crate) fn bad_response(text: String) -> Error {
            Error::Failure(
                HyperResponse::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(Body::from(text))
                    .expect("test assert: test state mock can create responses from strings"),
            )
        }

        pub(crate) async fn get(&mut self, _: &str) -> SheetsResult<(Response<Body>, Spreadsheet)> {
            sleep(Duration::from_millis(self.basic_response_duration_millis)).await;
            if let Some(err) = self.respond_with_error.take() {
                return Err(err);
            }
            let mut sheets: Vec<GoogleSheet> = self.sheets.clone().into_values().collect();
            sheets.sort_unstable_by_key(|s| s.properties.as_ref().unwrap().index);

            Ok((
                HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(Body::from("streaming"))
                    .unwrap(),
                Spreadsheet {
                    data_source_schedules: None,
                    data_sources: None,
                    developer_metadata: None,
                    named_ranges: None,
                    properties: None,
                    sheets: Some(sheets),
                    spreadsheet_id: None,
                    spreadsheet_url: None,
                },
            ))
        }

        pub(crate) async fn get_sheet_data(
            &mut self,
            _spreadsheet_id: &str,
            _sheet_id: SheetId,
        ) -> Result<Vec<Vec<Value>>, HttpResponse> {
            Ok(vec![])
        }

        pub(crate) async fn update(
            &mut self,
            req: BatchUpdateSpreadsheetRequest,
            _: &str,
        ) -> SheetsResult<(Response<Body>, BatchUpdateSpreadsheetResponse)> {
            let requests = req
                .requests
                .expect("test assert: batch update must have requests");
            sleep(Duration::from_millis(self.basic_response_duration_millis)).await;
            if let Some(err) = self.respond_with_error.take() {
                return Err(err);
            }

            for r in requests.into_iter() {
                match r {
                    Request {
                        add_sheet:
                            Some(AddSheetRequest {
                                properties: Some(mut properties),
                            }),
                        ..
                    } => {
                        let sheet_id = properties
                            .sheet_id
                            .expect("assert: goral creates sheets with sheet_id");
                        let title = properties
                            .title
                            .as_ref()
                            .expect("assert: goral creates sheets with title");
                        if self.sheet_titles.contains(title) {
                            return Err(Self::bad_response(format!(
                                "sheet with title {title} already exists!"
                            )));
                        }
                        self.sheet_titles.insert(title.to_string());
                        properties.index = Some(self.sheets.len() as i32);
                        // goral creates GRID sheets
                        // we decrease row count here for correct counting
                        // for append cells requests
                        // one row is empty for first row to be frozen
                        let current_row_count = properties
                            .grid_properties
                            .as_ref()
                            .unwrap()
                            .row_count
                            .unwrap();
                        properties
                            .grid_properties
                            .as_mut()
                            .expect("assert: goral creates grid sheets")
                            .row_count = Some(current_row_count - 1);
                        self.sheets
                            .insert(sheet_id, mock_sheet_with_properties(properties));
                    }

                    Request {
                        append_cells:
                            Some(AppendCellsRequest {
                                sheet_id: Some(sheet_id),
                                rows: Some(rows),
                                ..
                            }),
                        ..
                    } => {
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            let grid_properties = sheet
                                .properties
                                .as_mut()
                                .expect("assert: goral creates sheets with properties")
                                .grid_properties
                                .as_mut()
                                .expect("assert: goral creates grid sheets with grid_properties");
                            if let Some(row_count) = grid_properties.row_count {
                                grid_properties.row_count = Some(row_count + (rows.len() as i32));
                            } else {
                                return Err(Self::bad_response(format!(
                                    "cannot append cells to a non-grid sheet!"
                                )));
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to append cells to!"
                            )));
                        }
                    }

                    Request {
                        update_cells:
                            Some(UpdateCellsRequest {
                                range:
                                    Some(GridRange {
                                        sheet_id: Some(sheet_id),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    } => {
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to update cells!"
                            )));
                        }
                    }

                    Request {
                        create_developer_metadata:
                            Some(CreateDeveloperMetadataRequest {
                                developer_metadata: Some(metadata),
                                ..
                            }),
                        ..
                    } => {
                        let sheet_id = metadata
                            .location
                            .as_ref()
                            .expect("assert: goral sets location for new metadata")
                            .sheet_id
                            .unwrap();
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            if let Some(m) = sheet.developer_metadata.as_mut() {
                                self.metadata.insert(
                                    metadata
                                        .metadata_id
                                        .expect("assert: goral sets metadata_id for new metadata"),
                                    (sheet_id, m.len()),
                                );
                                m.push(metadata);
                            } else {
                                self.metadata.insert(
                                    metadata
                                        .metadata_id
                                        .expect("assert: goral sets metadata_id for new metadata"),
                                    (sheet_id, 0),
                                );
                                sheet.developer_metadata = Some(vec![metadata]);
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to create metadata for!"
                            )));
                        }
                    }

                    Request {
                        set_basic_filter:
                            Some(SetBasicFilterRequest {
                                filter:
                                    Some(BasicFilter {
                                        range:
                                            Some(GridRange {
                                                sheet_id: Some(sheet_id),
                                                ..
                                            }),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    } => {
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to add basic filter to!"
                            )));
                        }
                    }

                    Request {
                        update_developer_metadata:
                            Some(UpdateDeveloperMetadataRequest {
                                developer_metadata: Some(DeveloperMetadata { metadata_value, .. }),
                                data_filters: Some(data_filters),
                                ..
                            }),
                        ..
                    } => {
                        let metadata_id = data_filters[0]
                            .developer_metadata_lookup
                            .as_ref()
                            .unwrap()
                            .metadata_id
                            .unwrap();
                        if let Some((sheet_id, index)) = self.metadata.get(&metadata_id) {
                            let sheet = self.sheets.get_mut(&sheet_id).unwrap();
                            let metadatas = sheet.developer_metadata.as_mut().unwrap();
                            metadatas[*index].metadata_value = metadata_value;
                        } else {
                            return Err(Self::bad_response(format!(
                                "metadata with id {metadata_id} not found!"
                            )));
                        }
                    }

                    Request {
                        set_data_validation:
                            Some(SetDataValidationRequest {
                                range:
                                    Some(GridRange {
                                        sheet_id: Some(sheet_id),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    } => {
                        if !self.sheets.contains_key(&sheet_id) {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to set data validation on!"
                            )));
                        }
                    }

                    Request {
                        delete_range:
                            Some(DeleteRangeRequest {
                                range:
                                    Some(GridRange {
                                        sheet_id: Some(sheet_id),
                                        start_row_index: Some(1),
                                        end_row_index: Some(rows),
                                        ..
                                    }),
                                shift_dimension: Some(dimension),
                            }),
                        ..
                    } => {
                        assert_eq!(dimension, "ROWS");
                        if let Some(sheet) = self.sheets.get_mut(&sheet_id) {
                            let grid_properties = sheet
                                .properties
                                .as_mut()
                                .expect("assert: goral creates sheets with properties")
                                .grid_properties
                                .as_mut()
                                .expect("assert: goral creates grid sheets with grid_properties");
                            if let Some(row_count) = grid_properties.row_count {
                                grid_properties.row_count = Some(row_count - rows);
                            } else {
                                return Err(Self::bad_response(format!(
                                    "cannot delete cells from a non-grid sheet!"
                                )));
                            }
                        } else {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to delete cells from!"
                            )));
                        }
                    }

                    Request {
                        delete_sheet:
                            Some(DeleteSheetRequest {
                                sheet_id: Some(sheet_id),
                            }),
                        ..
                    } => {
                        if self.sheets.remove(&sheet_id).is_none() {
                            return Err(Self::bad_response(format!(
                                "sheet with id {sheet_id} not found to delete!"
                            )));
                        }
                    }

                    _ => panic!("test assert: unhandled request {r:?}"),
                }
            }

            Ok((
                HyperResponse::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
                    .body(Body::from("streaming"))
                    .unwrap(),
                BatchUpdateSpreadsheetResponse {
                    ..Default::default()
                },
            ))
        }
    }
}
