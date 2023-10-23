use crate::spreadsheet::sheet::{
    str_to_id, Header, Rows, Sheet, SheetId, TabColorRGB, UpdateSheet, VirtualSheet,
};
use crate::spreadsheet::{HttpResponse, Metadata, SpreadsheetAPI};
use crate::{get_service_tab_color, Sender, APP_NAME, HOST_ID_CHARS_LIMIT};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use google_sheets4::api::{CellData, CellFormat, ExtendedValue, NumberFormat, RowData, TextFormat};

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

const METADATA_SERVICE_KEY: &str = "service";
const METADATA_HOST_ID_KEY: &str = "host";
const METADATA_LOG_NAME: &str = "name";
const METADATA_CREATED_AT: &str = "created_at";
const METADATA_UPDATED_AT: &str = "updated_at";
const METADATA_KEYS: &str = "keys";
const DATETIME_COLUMN_NAME: &str = "datetime";
const MILLIS_PER_DAY: f64 = (24 * 60 * 60 * 1000) as f64;
const DEFAULT_FONT: &str = "Verdana";
const KEYS_DELIMITER: &str = "~^~";
const THOUSAND: u64 = 10_u64.pow(3);
const MILLION: u64 = 10_u64.pow(6);
const BILLION: u64 = 10_u64.pow(9);
const TRILLION: u64 = 10_u64.pow(12);

const fn size_pattern(size: u64) -> &'static str {
    if size >= TRILLION {
        "#,,,,\"T\""
    } else if size >= BILLION {
        "#,,,\"G\""
    } else if size >= MILLION {
        "#,,\"M\""
    } else if size >= THOUSAND {
        "#,\"K\""
    } else {
        "#"
    }
}

#[derive(Debug)]
pub(crate) enum Datavalue {
    Text(String),
    Number(f64),
    Integer(u64),
    IntegerID(u64),
    Percent(f64),
    HeatmapPercent(f64),
    Datetime(NaiveDateTime),
    Bool(bool),
    Size(u64),
    NotAvailable,
}

#[derive(Debug)]
pub struct Datarow {
    log_name: String,
    timestamp: NaiveDateTime,
    data: Vec<(String, Datavalue)>,
    sheet_id: Option<SheetId>,
    note: Option<String>,
}

impl Datarow {
    pub(crate) fn new(
        log_name: String,
        timestamp: NaiveDateTime,
        data: Vec<(String, Datavalue)>,
        note: Option<String>,
    ) -> Self {
        Self {
            log_name,
            timestamp,
            data,
            sheet_id: None,
            note,
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
            self.data.push(
                data.remove_entry(h)
                    .expect("assert: datarow should be sorted by keys which it contains"),
            );
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

// https://developers.google.com/sheets/api/guides/formats
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
                            text_format: Some(TextFormat {
                                font_family: Some("Courier New".to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Percent(p) => (
                        ExtendedValue {
                            number_value: Some(p / 100.0),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#%".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::HeatmapPercent(p) => (
                        ExtendedValue {
                            number_value: Some(p / 100.0),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(
                                    r"[Red][>0.8]#%;[Color22][>0.5]#%;[Color10]#%".to_string(),
                                ),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Integer(i) => (
                        ExtendedValue {
                            number_value: Some(i as f64),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#,###".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::IntegerID(i) => (
                        ExtendedValue {
                            number_value: Some(i as f64),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(r"#".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
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
                                pattern: Some(r"####.##".to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
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
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
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
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::Size(s) => (
                        ExtendedValue {
                            number_value: Some(s as f64),
                            ..Default::default()
                        },
                        CellFormat {
                            number_format: Some(NumberFormat {
                                pattern: Some(size_pattern(s).to_string()),
                                type_: Some("NUMBER".to_string()),
                            }),
                            text_format: Some(TextFormat {
                                font_family: Some(DEFAULT_FONT.to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    Datavalue::NotAvailable => (
                        ExtendedValue {
                            string_value: Some("N/A".to_string()),
                            ..Default::default()
                        },
                        CellFormat {
                            text_format: Some(TextFormat {
                                font_family: Some("Courier New".to_string()),
                                ..Default::default()
                            }),
                            horizontal_alignment: Some("CENTER".to_string()),
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
    project_id: String,
    send_notification: Sender,
}

impl Storage {
    pub fn new(
        host_id: String,
        project_id: String,
        google: SpreadsheetAPI,
        send_notification: Sender,
    ) -> Self {
        assert!(
            host_id.chars().count() <= HOST_ID_CHARS_LIMIT,
            "host id should be no more than {HOST_ID_CHARS_LIMIT} characters"
        );
        Self {
            host_id,
            project_id,
            google,
            send_notification,
        }
    }

    pub async fn welcome(&self) {
        let msg = format!(
            "{APP_NAME} started with [api usage page](https://console.cloud.google.com/apis/dashboard?project={}&show=all) and [api quota page](https://console.cloud.google.com/iam-admin/quotas?project={})", 
            self.project_id, self.project_id);
        tracing::info!("{}", msg);
        self.send_notification.info(msg).await;
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
    async fn fetch_sheets(&self) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>, HttpResponse> {
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

    pub(crate) async fn healthcheck(&mut self) -> Result<(), HttpResponse> {
        self.fetch_sheets().await?;
        Ok(())
    }

    pub(crate) async fn append(&mut self, datarows: Vec<Datarow>) -> Result<(), HttpResponse> {
        self._append(datarows, 32).await
    }

    pub(crate) async fn append_no_retry(
        &mut self,
        mut datarows: Vec<Datarow>,
    ) -> Result<Vec<String>, HttpResponse> {
        let sheet_ids = datarows
            .iter_mut()
            .map(|d| {
                let sheet_id = d.sheet_id(&self.storage.host_id, &self.service);
                self.sheet_url(sheet_id)
            })
            .collect();
        self._append(datarows, 0).await?;
        Ok(sheet_ids)
    }

    async fn _append(
        &mut self,
        datarows: Vec<Datarow>,
        retry_duration: u64,
    ) -> Result<(), HttpResponse> {
        let rows_count = datarows.len();
        tracing::info!(
            "appending to log {} rows for service {}",
            rows_count,
            self.service
        );
        let result = self
            ._core_append(datarows, retry_duration)
            .await
            .map_err(|e| {
                let message = format!(
                    "Saving batch data of {rows_count} rows failed for service {}",
                    self.service
                );
                tracing::error!("{}", message);
                self.storage.send_notification.try_error(message);
                e
            });
        tracing::info!(
            "appended to log {} rows for service {}",
            rows_count,
            self.service
        );
        result
    }

    // for newly created log sheet its headers order is determined by its first datarow. Fields for other datarows for the same sheet are sorted accordingly.
    async fn _core_append(
        &mut self,
        datarows: Vec<Datarow>,
        retry_duration: u64,
    ) -> Result<(), HttpResponse> {
        if datarows.is_empty() {
            return Ok(());
        }
        // We do not retry sheet `crud` as it goes after
        // `fetch_sheets` which is retriable and should
        // either fix an error or fail.
        // Retrying `crud` is not idempotent and
        // would require cloning all sheets at every retry attempt
        // https://developers.google.com/sheets/api/limits#example-algorithm
        let retry_strategy = ExponentialBackoff::from_millis(2)
            .max_delay(Duration::from_secs(retry_duration))
            .map(jitter);

        let mut retries = 0;
        let existing_sheets = Retry::spawn(retry_strategy.clone(), || {
            if retries > 0 {
                tracing::warn!(
                    "retrying #{} to fetch sheets for service {}",
                    retries,
                    self.service
                );
            }
            retries += 1;
            self.fetch_sheets()
        })
        .await?;

        tracing::debug!("existing sheets:\n{:?}", existing_sheets);

        // Check for sheet type to be grid
        let mut sheets_to_create: HashMap<SheetId, (VirtualSheet, Vec<String>)> = HashMap::new();
        let mut data_to_append: HashMap<SheetId, Rows> = HashMap::new();
        let mut sheets_to_update: HashMap<SheetId, UpdateSheet> = HashMap::new();

        let timestamp = Utc::now();

        datarows.into_iter().for_each(|mut datarow| {
            let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);

            if let Some((sheet, keys)) = existing_sheets.get(&sheet_id) {
                // for existing sheets we only change updated_at
                // so it is enough to have one update per sheet
                let new_metadata = vec![(METADATA_UPDATED_AT, timestamp.to_rfc3339())];
                sheets_to_update
                    .entry(sheet.sheet_id())
                    .or_insert(UpdateSheet::new(
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
                    .or_insert(Rows::new(
                        sheet.sheet_id(),
                        sheet.row_count().expect(
                            "assert: sheets created by services are grids which have row count",
                        ),
                    ))
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
        let sheets_to_update = sheets_to_update.into_iter().map(|(_, u)| u).collect();
        let data: Vec<Rows> = data_to_append.into_iter().map(|(_, rows)| rows).collect();

        tracing::debug!("sheets_to_update:\n{:?}", sheets_to_update);
        tracing::debug!("sheets_to_add:\n{:?}", sheets_to_add);
        tracing::debug!("data:\n{:?}", data);

        self.storage
            .google
            .crud_sheets(&self.spreadsheet_id, sheets_to_update, sheets_to_add, data)
            .await
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

macro_rules! sheet_name_jitter {
    ($sheet_id:expr) => {
        (*$sheet_id as u8) >> 3
    };
}

fn prepare_sheet_title(
    host_id: &str,
    service: &str,
    log_name: &str,
    timestamp: &DateTime<Utc>,
    sheet_id: &SheetId,
) -> String {
    // to prevent sheet titles conflicts we "randomize" a little bit sheet creation datetime
    let jitter = sheet_name_jitter!(sheet_id);
    let timestamp = *timestamp + Duration::from_secs(jitter.into());
    // use @ as a delimeter
    // see https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
    format!(
        "{}@{}@{} {}",
        log_name,
        host_id,
        service,
        timestamp.format("%yy/%m/%d %H:%M:%S").to_string(),
    )
}

// https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
fn convert_datetime_to_spreadsheet_double(d: NaiveDateTime) -> f64 {
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)
        .expect("assert: static datetime")
        .and_hms_opt(0, 0, 0)
        .expect("assert: static datetime");
    if d < base {
        return 0.0;
    }
    let days = (d - base).num_days() as f64;
    let millis = d
        .time()
        .signed_duration_since(
            NaiveTime::from_hms_milli_opt(0, 0, 0, 0).expect("assert: static time"),
        )
        .num_milliseconds();
    let fraction_of_day = millis as f64 / MILLIS_PER_DAY;
    days + fraction_of_day
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter() {
        let jitter = sheet_name_jitter!(&137328873_i32);
        assert!(
            jitter < 2_u8.pow(5),
            "sheet_name_jitter should produce values less than 2^5"
        )
    }

    #[test]
    fn size_pattern_cases() {
        assert_eq!("#".to_string(), size_pattern(670_u64));
        assert_eq!("#,\"K\"".to_string(), size_pattern(6_701_u64));
        assert_eq!("#,,\"M\"".to_string(), size_pattern(1_020_111_u64));
        assert_eq!("#,,,\"G\"".to_string(), size_pattern(1_020_111_324_u64));
        assert_eq!(
            "#,,,,\"T\"".to_string(),
            size_pattern(1_020_111_324_428_u64)
        );
    }

    #[test]
    fn datarow_id() {
        let scrape_time = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("assert: static datetime")
            .and_hms_opt(0, 0, 0)
            .expect("assert: static datetime");
        let mut datarow = Datarow::new(
            "/dev/shm".to_string(),
            scrape_time,
            vec![
                (format!("disk_use"), Datavalue::HeatmapPercent(3_f64)),
                (format!("disk_free"), Datavalue::Size(400_u64)),
            ],
            None,
        );
        assert_eq!(895475598, datarow.sheet_id("host1", "system"));
    }
}
