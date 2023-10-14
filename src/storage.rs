use crate::spreadsheet::sheet::{
    str_to_id, Header, Rows, Sheet, SheetId, TabColorRGB, UpdateSheet, VirtualSheet,
};
use crate::spreadsheet::{Metadata, SpreadsheetAPI};
use crate::{get_service_tab_color, Sender};
use anyhow::Result;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use google_sheets4::api::{CellData, CellFormat, ExtendedValue, NumberFormat, RowData};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

const METADATA_SERVICE_KEY: &str = "service";
const METADATA_HOST_ID_KEY: &str = "host";
const METADATA_LOG_NAME: &str = "name";
const METADATA_CREATED_AT: &str = "created_at";
const METADATA_UPDATED_AT: &str = "updated_at";
const METADATA_KEYS: &str = "keys";
const DATETIME_COLUMN_NAME: &str = "datetime";
const MILLIS_PER_DAY: f64 = (24 * 60 * 60 * 1000) as f64;
const KEYS_DELIMITER: &str = "~^~";

#[derive(Debug)]
pub(crate) enum Datavalue {
    Text(String),
    Number(f64),
    Datetime(NaiveDateTime),
    Bool(bool),
}

#[derive(Debug)]
pub(crate) struct Datarow {
    log_name: String,
    timestamp: NaiveDateTime,
    data: Vec<(String, Datavalue)>,
    sheet_id: Option<SheetId>,
}

impl Datarow {
    pub(crate) fn new(
        log_name: String,
        timestamp: NaiveDateTime,
        data: Vec<(String, Datavalue)>,
    ) -> Self {
        Self {
            log_name,
            timestamp,
            data,
            sheet_id: None,
        }
    }

    pub(crate) fn sheet_id(&mut self, host_id: &str, service_name: &str) -> SheetId {
        if let Some(sheet_id) = self.sheet_id {
            return sheet_id;
        }
        let keys = self.keys_sorted().join(",");
        let id_string = format!("{host_id}_{service_name}_{}_{}", self.log_name, keys);
        let sheet_id = str_to_id(&id_string);
        self.sheet_id = Some(sheet_id);
        sheet_id
    }

    pub(crate) fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.data.len() + 1);
        keys.push(DATETIME_COLUMN_NAME);
        for (k, _) in self.data.iter() {
            keys.push(k);
        }
        keys
    }

    fn keys_sorted(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.data.len() + 1);
        keys.push(DATETIME_COLUMN_NAME);
        for (k, _) in self.data.iter() {
            keys.push(k);
        }
        keys.sort_unstable();
        keys
    }

    fn headers(&self) -> Vec<Header> {
        let mut headers = Vec::with_capacity(self.data.len() + 1);
        headers.push(Header::new(DATETIME_COLUMN_NAME.to_string(), None));
        for (k, _) in self.data.iter() {
            headers.push(Header::new(k.to_string(), None));
        }
        headers
    }

    fn sort_by_keys(&mut self, keys: &Vec<String>) {
        let cap = self.data.len();
        let mut data: HashMap<String, Datavalue> =
            mem::replace(&mut self.data, Vec::with_capacity(cap))
                .into_iter()
                .collect();
        // we skip first header which is DATETIME_COLUMN_NAME
        for h in keys.into_iter().skip(1) {
            self.data.push(data.remove_entry(h).unwrap());
        }
    }

    fn values(self) -> Vec<Datavalue> {
        let mut values = Vec::with_capacity(self.data.len() + 1);
        values.push(Datavalue::Datetime(self.timestamp));
        for (_, v) in self.data.into_iter() {
            values.push(v);
        }
        values
    }
}

impl Into<RowData> for Datarow {
    fn into(self) -> RowData {
        let row = self
            .values()
            .into_iter()
            .map(|v| {
                let (value, format) = match v {
                    Datavalue::Text(t) => (
                        ExtendedValue {
                            string_value: Some(t),
                            ..Default::default()
                        },
                        CellFormat {
                            ..Default::default()
                        },
                    ),
                    Datavalue::Number(f) => (
                        ExtendedValue {
                            number_value: Some(f),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: None,
                                type_: Some("NUMBER".to_string()),
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Bool(b) => (
                        ExtendedValue {
                            bool_value: Some(b),
                            ..Default::default()
                        },
                        CellFormat {
                            ..Default::default()
                        },
                    ),
                    Datavalue::Datetime(d) => (
                        ExtendedValue {
                            number_value: Some(convert_datetime_to_spreadsheet_double(d)),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"yyy mmm d \[ddd\] h:mm:ss".to_string()),
                                type_: Some("DATE_TIME".to_string()),
                            }),
                            ..Default::default()
                        },
                    ),
                };
                CellData {
                    user_entered_value: Some(value),
                    user_entered_format: Some(format),
                    ..Default::default()
                }
            })
            .collect();
        RowData { values: Some(row) }
    }
}

pub struct Storage {
    google: SpreadsheetAPI,
    host_id: String,
    send_notification: Sender,
}

impl Storage {
    pub fn new(host_id: String, google: SpreadsheetAPI, send_notification: Sender) -> Self {
        Self {
            host_id,
            google,
            send_notification,
        }
    }
}

pub fn create_log(storage: Arc<Storage>, spreadsheet_id: String, service: String) -> AppendableLog {
    let tab_color_rgb = get_service_tab_color(&service);
    AppendableLog {
        storage,
        spreadsheet_id,
        service,
        tab_color_rgb,
    }
}

pub struct AppendableLog {
    storage: Arc<Storage>,
    service: String,
    spreadsheet_id: String,
    tab_color_rgb: TabColorRGB,
}

impl AppendableLog {
    async fn fetch_sheets(&self) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>> {
        let basic_metadata = Metadata::new(vec![
            (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
            (METADATA_SERVICE_KEY, self.service.to_string()),
        ]);

        let (existing_service_sheets, _total_usage, _service_usage) = self
            .storage
            .google
            .sheets_filtered_by_metadata(&self.spreadsheet_id, &basic_metadata)
            .await?;

        // TODO check usage and prepare deletion requests for Sheets and Obsolete metadata
        //self.storage.send_notification.warn(format!())

        Ok(existing_service_sheets
            .into_iter()
            .map(|s| {
                let keys: Vec<String> = s
                    .meta_value(METADATA_KEYS)
                    .expect("assert: meta key is set at sheet creation")
                    .split(KEYS_DELIMITER)
                    .map(|k| k.to_string())
                    .collect();
                (s.sheet_id(), (s, keys))
            })
            .collect())
    }

    pub(crate) async fn healthcheck(&mut self) -> Result<()> {
        self.fetch_sheets().await?;
        Ok(())
    }

    pub(crate) async fn append(&mut self, datarows: Vec<Datarow>) -> Result<()> {
        let rows_count = datarows.len();
        let result = self._append(datarows).await.map_err(|e| {
            let message = format!(
                "Saving batch data ({rows_count} rows) failed for service {}",
                self.service
            );
            tracing::error!("{}", message);
            self.storage.send_notification.try_error(message);
            e
        });
        result
    }

    // for newly created log sheet its headers order is determined by its first datarow. Fields for other datarows for the same sheet are sorted accordingly.
    async fn _append(&mut self, datarows: Vec<Datarow>) -> Result<()> {
        if datarows.is_empty() {
            return Ok(());
        }
        // check that contains timestamp and all keys are the same
        let existing_sheets = self.fetch_sheets().await?;

        tracing::debug!("existing sheets:\n{:?}", existing_sheets);

        // Check for sheet type to be grid
        let mut sheets_to_create: HashMap<SheetId, (VirtualSheet, Vec<String>)> = HashMap::new();
        let _metadata_to_update: HashMap<SheetId, Vec<RowData>> = HashMap::new();
        let mut data_to_append: HashMap<SheetId, Rows> = HashMap::new();
        let mut sheets_to_update: Vec<UpdateSheet> = vec![];

        let timestamp = Utc::now();

        datarows.into_iter().for_each(|mut datarow| {
            let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);
            if let Some((sheet, keys)) = existing_sheets.get(&sheet_id) {
                let new_metadata = vec![(METADATA_UPDATED_AT, timestamp.to_rfc3339())];
                sheets_to_update.push(UpdateSheet::new(
                    sheet.sheet_id(),
                    Metadata::new(new_metadata),
                ));
                datarow.sort_by_keys(keys);
                data_to_append
                    .entry(sheet.sheet_id())
                    .or_insert(Rows::new(
                        sheet.sheet_id(),
                        sheet
                            .row_count()
                            .expect("assert: sheets managed by goral are grid"),
                    ))
                    .push(datarow.into());
            } else if let Some((sheet, keys)) = sheets_to_create.get(&sheet_id) {
                // align data according to headers of sheet to be created
                datarow.sort_by_keys(keys);
                data_to_append
                    .entry(sheet.sheet_id())
                    .or_insert(Rows::new(sheet.sheet_id(), sheet.row_count().unwrap()))
                    .push(datarow.into());
            } else {
                let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);
                let title = prepare_sheet_title(
                    &self.storage.host_id,
                    &self.service,
                    &datarow.log_name,
                    &timestamp,
                    &sheet_id,
                );
                let timestamp = timestamp.to_rfc3339();
                let headers = datarow.headers();
                let metadata = vec![
                    (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
                    (METADATA_SERVICE_KEY, self.service.to_string()),
                    (METADATA_LOG_NAME, datarow.log_name.clone()),
                    (METADATA_KEYS, datarow.keys().join(KEYS_DELIMITER)),
                    (METADATA_CREATED_AT, timestamp.clone()),
                    (METADATA_UPDATED_AT, timestamp.clone()),
                ];
                let sheet = VirtualSheet::new_grid(
                    sheet_id,
                    title,
                    headers,
                    Metadata::new(metadata),
                    self.tab_color_rgb,
                );
                let row_count = sheet.row_count().expect("assert: grid sheet has row count");
                sheets_to_create.insert(
                    sheet_id,
                    (
                        sheet,
                        datarow.keys().iter().map(|k| k.to_string()).collect(),
                    ),
                );
                data_to_append
                    .entry(sheet_id)
                    .or_insert(Rows::new(sheet_id, row_count))
                    .push(datarow.into());
            }
        });

        let sheets_to_add = sheets_to_create.into_iter().map(|(_, (s, _))| s).collect();
        let data: Vec<Rows> = data_to_append.into_iter().map(|(_, rows)| rows).collect();

        tracing::debug!("sheets_to_update:\n{:?}", sheets_to_update);
        tracing::debug!("sheets_to_add:\n{:?}", sheets_to_add);
        tracing::debug!("data:\n{:?}", data);

        self.storage
            .google
            .crud_sheets(&self.spreadsheet_id, sheets_to_update, sheets_to_add, data)
            .await
    }

    pub(crate) fn spreadsheet_url(&self) -> String {
        self.storage.google.spreadsheet_url(&self.spreadsheet_id)
    }

    pub(crate) fn host_id(&self) -> &str {
        &self.storage.host_id
    }

    pub(crate) fn sheet_url(&self, sheet_id: SheetId) -> String {
        self.storage
            .google
            .sheet_url(&self.spreadsheet_id, sheet_id)
    }
}

fn prepare_sheet_title(
    host_id: &str,
    service: &str,
    log_name: &str,
    timestamp: &DateTime<Utc>,
    sheet_id: &SheetId,
) -> String {
    format!(
        "{}:{}:{} {} {}",
        host_id,
        service,
        log_name,
        timestamp.format("%yy/%m/%d %H:%M:%S").to_string(),
        sheet_id
    )
}

// https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
fn convert_datetime_to_spreadsheet_double(d: NaiveDateTime) -> f64 {
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    if d < base {
        return 0.0;
    }
    let days = (d - base).num_days() as f64;
    let millis = d
        .time()
        .signed_duration_since(NaiveTime::from_hms_milli_opt(0, 0, 0, 0).unwrap())
        .num_milliseconds();
    let fraction_of_day = millis as f64 / MILLIS_PER_DAY;
    days + fraction_of_day
}
