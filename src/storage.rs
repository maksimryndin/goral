use crate::rules::{rules_dropdowns, Rule, RULES_LOG_NAME};
use crate::spreadsheet::datavalue::Datarow;
use crate::spreadsheet::sheet::{
    CleanupSheet, Rows, Sheet, SheetId, SheetType, TabColorRGB, UpdateSheet, VirtualSheet,
};
use crate::spreadsheet::spreadsheet::GOOGLE_SPREADSHEET_MAXIMUM_CELLS;
use crate::spreadsheet::{HttpResponse, Metadata, SpreadsheetAPI};
use crate::{get_service_tab_color, Sender, HOST_ID_CHARS_LIMIT};
use chrono::{DateTime, Utc};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const METADATA_SERVICE_KEY: &str = "service";
const METADATA_HOST_ID_KEY: &str = "host";
const METADATA_LOG_NAME: &str = "name";
const METADATA_CREATED_AT: &str = "created_at";
const METADATA_UPDATED_AT: &str = "updated_at";
const METADATA_KEYS: &str = "keys";
const KEYS_DELIMITER: &str = "~^~";

pub struct Storage {
    google: SpreadsheetAPI,
    host_id: String,
    send_notification: Sender,
}

impl Storage {
    pub fn new(host_id: String, google: SpreadsheetAPI, send_notification: Sender) -> Self {
        assert!(
            host_id.chars().count() <= HOST_ID_CHARS_LIMIT,
            "host id should be no more than {HOST_ID_CHARS_LIMIT} characters"
        );
        Self {
            host_id,
            google,
            send_notification,
        }
    }
}

pub fn create_log(
    storage: Arc<Storage>,
    spreadsheet_id: String,
    service: String,
    truncate_at: f32,
) -> AppendableLog {
    let tab_color_rgb = get_service_tab_color(&service);
    AppendableLog {
        storage,
        spreadsheet_id,
        service,
        tab_color_rgb,
        rules_sheet_id: None,
        truncate_at,
    }
}

pub struct AppendableLog {
    storage: Arc<Storage>,
    service: String,
    spreadsheet_id: String,
    tab_color_rgb: TabColorRGB,
    rules_sheet_id: Option<SheetId>,
    truncate_at: f32,
}

#[derive(Debug)]
struct SheetUsage {
    sheet_id: SheetId,
    row_count: i32,
    column_count: i32,
    updated_at: DateTime<Utc>,
}

impl AppendableLog {
    async fn fetch_sheets(&self) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>, HttpResponse> {
        let basic_metadata = Metadata::new(vec![
            (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
            (METADATA_SERVICE_KEY, self.service.to_string()),
        ]);

        let existing_service_sheets = self
            .storage
            .google
            .sheets_filtered_by_metadata(&self.spreadsheet_id, &basic_metadata)
            .await?;

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
        self._append(
            datarows,
            Some(Duration::from_secs(32)),
            Some(Duration::from_secs(5)),
        )
        .await
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
        self._append(datarows, None, None).await?;
        Ok(sheet_ids)
    }

    async fn _append(
        &mut self,
        datarows: Vec<Datarow>,
        retry_limit: Option<Duration>,
        timeout: Option<Duration>,
    ) -> Result<(), HttpResponse> {
        let rows_count = datarows
            .iter()
            .filter(|d| d.log_name() != RULES_LOG_NAME)
            .count();
        tracing::info!("appending {} rows for service {}", rows_count, self.service);
        let result = self
            ._core_append(datarows, retry_limit, timeout)
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
        result
    }

    // https://developers.google.com/sheets/api/limits#example-algorithm
    async fn timed_fetch_sheets(
        &self,
        maximum_backoff: Duration,
        timeout: Duration,
    ) -> Result<HashMap<SheetId, (Sheet, Vec<String>)>, HttpResponse> {
        let mut total_time = Duration::from_millis(0);
        let mut wait = Duration::from_millis(2);
        let mut retry = 0;
        let max_backoff = tokio::time::sleep(maximum_backoff);
        tokio::pin!(max_backoff);
        while total_time < maximum_backoff {
            tokio::select! {
                _ = &mut max_backoff => {break;}
                _ = tokio::time::sleep(timeout) => {
                    let msg = format!("No response from Google API for timeout {timeout:?} for retry {retry}");
                    tracing::error!("{}", msg);
                    self.storage.send_notification.fatal(msg).await;
                    panic!("assert: Google API request timed-out");
                },
                res = self.fetch_sheets() => {
                    match res {
                        Ok(val) => {
                            return Ok(val);},
                        Err(e) => {
                            tracing::error!("error {:?} for retry {}", e, retry);
                        }
                    }
                }
            }
            let jittered = wait + jitter_duration();
            tokio::time::sleep(jittered).await;
            total_time += jittered;
            retry += 1;
            wait *= 2;
        }
        let msg = format!("Google API request maximum retry duration {maximum_backoff:?} is reached with {retry} retries");
        tracing::error!("{}", msg);
        self.storage.send_notification.fatal(msg).await;
        panic!("assert: Google API request maximum retry duration");
    }

    // for newly created log sheet its headers order is determined by its first datarow. Fields for other datarows for the same sheet are sorted accordingly.
    async fn _core_append(
        &mut self,
        datarows: Vec<Datarow>,
        retry_limit: Option<Duration>,
        timeout: Option<Duration>,
    ) -> Result<(), HttpResponse> {
        if datarows.is_empty() {
            return Ok(());
        }

        let existing_sheets = if let Some(retry_limit) = retry_limit {
            // We do not retry sheet `crud` as it goes after
            // `fetch_sheets` which is retriable and should
            // either fix an error or fail.
            // Retrying `crud` is not idempotent and
            // would require cloning all sheets at every retry attempt
            self.timed_fetch_sheets(
                retry_limit,
                timeout.expect("assert: timeout is set if retry_limit is set"),
            )
            .await?
        } else {
            self.fetch_sheets().await?
        };

        tracing::debug!("existing sheets:\n{:?}", existing_sheets);

        let truncate_requests = Self::prepare_truncate_requests(&existing_sheets, self.truncate_at);

        // Check for sheet type to be grid
        let mut sheets_to_create: HashMap<SheetId, (VirtualSheet, Vec<String>)> = HashMap::new();
        let mut data_to_append: HashMap<SheetId, Rows> = HashMap::new();
        let mut sheets_to_update: HashMap<SheetId, UpdateSheet> = HashMap::new();

        let timestamp = Utc::now();

        for mut datarow in datarows.into_iter() {
            let sheet_id = datarow.sheet_id(&self.storage.host_id, &self.service);

            if self.rules_sheet_id.is_none() && datarow.log_name() == RULES_LOG_NAME {
                self.rules_sheet_id = Some(sheet_id);
            }

            if let Some((sheet, keys)) = existing_sheets.get(&sheet_id) {
                if datarow.log_name() == RULES_LOG_NAME {
                    // we don't append for existing rules sheets
                    continue;
                }
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
                    datarow.log_name(),
                    &timestamp,
                    &sheet_id,
                );
                let timestamp = timestamp.to_rfc3339();
                let headers = datarow.headers();
                let metadata = vec![
                    (METADATA_HOST_ID_KEY, self.storage.host_id.to_string()),
                    (METADATA_SERVICE_KEY, self.service.to_string()),
                    (METADATA_LOG_NAME, datarow.log_name().clone()),
                    (METADATA_KEYS, datarow.keys().join(KEYS_DELIMITER)),
                    (METADATA_CREATED_AT, timestamp.clone()),
                    (METADATA_UPDATED_AT, timestamp.clone()),
                ];
                let mut sheet = VirtualSheet::new_grid(
                    sheet_id,
                    title,
                    headers,
                    Metadata::new(metadata),
                    self.tab_color_rgb,
                );
                if datarow.log_name() == RULES_LOG_NAME {
                    sheet = sheet.with_dropdowns(rules_dropdowns());
                }
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
        }

        let sheets_to_add: Vec<VirtualSheet> =
            sheets_to_create.into_iter().map(|(_, (s, _))| s).collect();
        let sheets_to_update: Vec<UpdateSheet> =
            sheets_to_update.into_iter().map(|(_, u)| u).collect();
        let data: Vec<Rows> = data_to_append.into_iter().map(|(_, rows)| rows).collect();
        let rows_count = data.len();

        tracing::debug!("truncate_requests:\n{:?}", truncate_requests);
        tracing::debug!("sheets_to_update:\n{:?}", sheets_to_update);
        tracing::debug!("sheets_to_add:\n{:?}", sheets_to_add);
        tracing::debug!("data:\n{:?}", data);

        if sheets_to_add.is_empty() && sheets_to_update.is_empty() {
            return Ok(());
        }

        self.storage
            .google
            .crud_sheets(
                &self.spreadsheet_id,
                truncate_requests,
                sheets_to_update,
                sheets_to_add,
                data,
            )
            .await?;

        tracing::info!(
            "appended to log {} rows for service {}",
            rows_count,
            self.service
        );
        Ok(())
    }

    fn prepare_truncate_requests(
        existing_service_sheets: &HashMap<SheetId, (Sheet, Vec<String>)>,
        limit: f32,
    ) -> Vec<CleanupSheet> {
        let cells_used_by_service: i32 = existing_service_sheets
            .values()
            .filter_map(|(s, _)| {
                (s.sheet_type() == SheetType::Grid).then(|| {
                    s.row_count().expect("assert: grid sheet has rows")
                        * s.column_count().expect("assert: grid sheet has columns")
                })
            })
            .sum();
        let usage =
            100.0 * (cells_used_by_service as f32 / GOOGLE_SPREADSHEET_MAXIMUM_CELLS as f32);

        if usage < limit {
            return vec![];
        }
        // remove surplus and 10% of the limit
        let cells_to_delete =
            (usage - 0.9 * limit) * GOOGLE_SPREADSHEET_MAXIMUM_CELLS as f32 / 100.0;

        let usages = existing_service_sheets
            .values()
            .filter(|(s, _)| s.sheet_type() == SheetType::Grid)
            .fold(HashMap::new(), |mut state, (s, _)| {
                let log_name = s
                    .meta_value(METADATA_LOG_NAME)
                    .expect("assert: service sheet has `name` metadata");
                // rules sheets are not included
                if log_name == RULES_LOG_NAME {
                    return state;
                }
                let sheet_usage = SheetUsage {
                    sheet_id: s.sheet_id(),
                    row_count: s.row_count().expect("assert: grid sheet has rows"),
                    column_count: s.column_count().expect("assert: grid sheet has columns"),
                    updated_at: DateTime::parse_from_rfc3339(
                        s.meta_value(METADATA_UPDATED_AT)
                            .expect("assert: service sheet has `updated_at` metadata"),
                    )
                    .expect("assert: `updated_at` metadata is serialized to rfc3339")
                    .into(),
                };
                let sheet_cells = sheet_usage.row_count * sheet_usage.column_count;
                let stat = state.entry(log_name).or_insert((0, 0.0, vec![]));
                stat.0 += sheet_cells;
                stat.1 = stat.0 as f32 / cells_used_by_service as f32; // share of usage by the log name
                stat.2.push(sheet_usage);
                state
            });

        tracing::debug!("usages:\n{:#?}", usages);
        usages
            .into_iter()
            .map(|(_, (_, log_cells_usage, mut sheets))| {
                let mut cells_to_delete_for_log = (log_cells_usage * cells_to_delete) as i32;
                sheets.sort_unstable_by_key(|s| s.updated_at);
                let mut requests = Vec::with_capacity(sheets.len());
                for sheet in sheets {
                    if cells_to_delete_for_log <= 0 {
                        break;
                    }
                    let cells = sheet.row_count * sheet.column_count;
                    if cells <= cells_to_delete_for_log {
                        // remove the whole sheet
                        requests.push(CleanupSheet::delete(sheet.sheet_id));
                        cells_to_delete_for_log -= cells;
                    } else {
                        // remove some rows
                        let rows = cells_to_delete_for_log / sheet.column_count;
                        requests.push(CleanupSheet::truncate(
                            sheet.sheet_id,
                            sheet.row_count.min(rows + 1),
                        ));
                        cells_to_delete_for_log = 0;
                    }
                }
                requests
            })
            .flatten()
            .collect()
    }

    pub(crate) fn host_id(&self) -> &str {
        &self.storage.host_id
    }

    pub(crate) fn sheet_url(&self, sheet_id: SheetId) -> String {
        self.storage
            .google
            .sheet_url(&self.spreadsheet_id, sheet_id)
    }

    pub(crate) fn spreadsheet_baseurl(&self) -> String {
        self.storage
            .google
            .spreadsheet_baseurl(&self.spreadsheet_id)
    }

    // TODO add timeout for fetch
    pub(crate) async fn get_rules(&self) -> Vec<Rule> {
        let data = self
            .storage
            .google
            .get_sheet_data(&self.spreadsheet_id, self.rules_sheet_id.expect("assert: rules sheet id is saved at the start of the service at the first append"))
            .await
            .unwrap();
        data.into_iter()
            .filter_map(|row| Rule::try_from_values(row))
            .collect()
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

pub(crate) fn jitter_duration() -> Duration {
    let mut buf = [0u8; 2];
    getrandom::getrandom(&mut buf).expect("assert: can get random from the OS");
    let jitter = u16::from_be_bytes(buf);
    let jitter = jitter >> 2; // to limit values to 2^14 = 16384
    Duration::from_millis(jitter as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::general::GENERAL_SERVICE_NAME;
    use crate::spreadsheet::datavalue::{Datarow, Datavalue};
    use crate::spreadsheet::sheet::tests::mock_ordinary_google_sheet;
    use crate::spreadsheet::tests::TestState;
    use crate::tests::TEST_HOST_ID;
    use crate::Sender;
    use chrono::NaiveDate;
    use google_sheets4::Error;
    use tokio::sync::mpsc;

    impl Storage {
        pub(crate) fn google(&self) -> &SpreadsheetAPI {
            &self.google
        }
    }

    #[test]
    fn jitter() {
        let jitter = sheet_name_jitter!(&137328873_i32);
        assert!(
            jitter < 2_u8.pow(5),
            "sheet_name_jitter should produce values less than 2^5"
        )
    }

    #[test]
    fn jittered_duration() {
        let upper_bound = Duration::from_millis(2u64.pow(14) + 1);
        for _ in 0..100 {
            assert!(jitter_duration() < upper_bound);
        }
    }

    #[tokio::test]
    async fn basic_append_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(vec![mock_ordinary_google_sheet("some sheet")], None, None),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();

        // for the sheet order we rely on the indexing
        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            3,
            "`some sheet` already exists, `log_name1` and `log_name2` sheets have been created"
        );
        assert!(
            all_sheets[1].title().contains("log_name1")
                || all_sheets[2].title().contains("log_name1")
        );
        assert_eq!(
            all_sheets[1].row_count(),
            Some(2),
            "`log_name..` contains header row and one row of data"
        );
        assert!(
            all_sheets[1].title().contains("log_name2")
                || all_sheets[2].title().contains("log_name2")
        );
        assert_eq!(
            all_sheets[2].row_count(),
            Some(2),
            "`log_name..` contains header row and one row of data"
        );

        // adding two existing log_name-keys rows
        // but for one the order of keys is different - shouldn't make any difference
        // and new one combination
        // and new combination with the same log_name, but updated keys
        // two new sheets should be created
        let datarows = vec![
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key23"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key12"), Datavalue::Size(400_u64)),
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name3".to_string(),
                timestamp,
                vec![
                    (format!("key31"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key32"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();
        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(all_sheets.len(), 5, "`some sheet`, `log_name1` and `log_name2` already exist, `log_name2` with different keys and `log_name3` sheets have been created");

        assert!(
            all_sheets[1].title().contains("log_name1")
                || all_sheets[2].title().contains("log_name1")
        );
        assert_eq!(
            all_sheets[1].row_count(),
            Some(3),
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );
        assert!(
            all_sheets[1].title().contains("log_name2")
                || all_sheets[2].title().contains("log_name2")
        );
        assert_eq!(
            all_sheets[2].row_count(),
            Some(3),
            "`log_name1` and `log_name2` contain header row and two rows of data"
        );

        assert!(
            all_sheets[3].title().contains("log_name2")
                || all_sheets[4].title().contains("log_name2")
        );
        assert_eq!(all_sheets[3].row_count(), Some(2), "`log_name2` with different keys and `log_name3` contain header row and one row of data");
        assert!(
            all_sheets[3].title().contains("log_name3")
                || all_sheets[4].title().contains("log_name3")
        );
        assert_eq!(all_sheets[4].row_count(), Some(2), "`log_name2` with different keys and `log_name3` contain header row and one row of data");

        assert_eq!(
            all_sheets[3].meta_value(METADATA_SERVICE_KEY),
            Some(&GENERAL_SERVICE_NAME.to_string())
        );
        assert_eq!(
            all_sheets[3].meta_value(METADATA_HOST_ID_KEY),
            Some(&TEST_HOST_ID.to_string())
        );

        assert!(all_sheets[3]
            .meta_value(METADATA_LOG_NAME)
            .unwrap()
            .starts_with("log_name"));
        assert_ne!(
            all_sheets[1].meta_value(METADATA_CREATED_AT),
            all_sheets[1].meta_value(METADATA_UPDATED_AT),
            "first sheets should be updated"
        );
        assert_eq!(
            all_sheets[3].meta_value(METADATA_CREATED_AT),
            all_sheets[3].meta_value(METADATA_UPDATED_AT),
            "new sheets have the same created and updated timestamps"
        );
    }

    #[tokio::test]
    async fn append_retry() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::bad_response("error to retry".to_string())),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        log.append(datarows).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "The application's API key was not found in the configuration")]
    async fn append_fatal_error() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(Error::MissingAPIKey),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append(datarows).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Google API request timed-out")]
    async fn append_request_timeout() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                None,
                Some(150),
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });
        log._append(
            datarows,
            Some(Duration::from_millis(200)),
            Some(Duration::from_millis(100)),
        )
        .await
        .unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Google API request maximum retry duration")]
    async fn append_retry_maximum_backoff() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::bad_response("error to retry".to_string())),
                Some(150),
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });
        log._append(
            datarows,
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(200)),
        )
        .await
        .unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "error to retry")]
    async fn append_without_retry() {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(
            tx.clone(),
            TestState::new(
                vec![mock_ordinary_google_sheet("some sheet")],
                Some(TestState::bad_response("error to retry".to_string())),
                None,
            ),
        );
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            100.0,
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        // adding two new log_name-keys rows
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key12"), Datavalue::Size(400_u64)),
                ],
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
            ),
        ];

        let handle = tokio::task::spawn(async move {
            assert!(
                rx.recv().await.is_some(),
                "notification is sent for nonrecoverable error"
            );
        });

        log.append_no_retry(datarows).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn truncation_flow() {
        let (tx, _) = mpsc::channel(1);
        let tx = Sender::new(tx);
        let sheets_api = SpreadsheetAPI::new(tx.clone(), TestState::new(vec![], None, None));
        let storage = Arc::new(Storage::new(
            TEST_HOST_ID.to_string(),
            sheets_api,
            tx.clone(),
        ));
        // for simplicity we create logs with one key to easily
        // make assertions on rows count (only two columns - timestamp and key)
        let mut log = create_log(
            storage,
            "spreadsheet1".to_string(),
            GENERAL_SERVICE_NAME.to_string(),
            0.01, // 0.01% of 10 000 000 cells means 1000 cells or 500 rows
        );

        let timestamp = NaiveDate::from_ymd_opt(2023, 10, 19)
            .expect("test assert: static date")
            .and_hms_opt(0, 0, 0)
            .expect("test assert: static time");

        let mut datarows = Vec::with_capacity(999);
        for _ in 0..248 {
            datarows.push(Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![(format!("key11"), Datavalue::Size(400_u64))],
            ));
            datarows.push(Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![(format!("key21"), Datavalue::Size(400_u64))],
            ));
        } // 996 cells of log_name1 and log_name2 including headers
        datarows.push(Datarow::new(
            RULES_LOG_NAME.to_string(),
            timestamp,
            vec![(format!("key21"), Datavalue::Size(400_u64))],
        )); // 2 rows of rules (including header row) or 4 cells

        log.append(datarows).await.unwrap();

        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            3,
            "`log_name1`, `log_name2`, `{}` sheets have been created",
            RULES_LOG_NAME
        );
        for i in 0..3 {
            if all_sheets[i].title().contains("log_name1")
                || all_sheets[i].title().contains("log_name2")
            {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(249),
                    "`log_name..` contains header row and 248 rows of data"
                );
            } else {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(2),
                    "`{}` contains header row and 1 row of data",
                    RULES_LOG_NAME
                );
            }
        }

        // we have 1000 cells used
        // now add datarows above the limit
        // for log_name1 the key has changed - new sheet will be created

        let mut datarows = Vec::with_capacity(10);
        for _ in 0..5 {
            datarows.push(Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![(format!("key12"), Datavalue::Size(400_u64))],
            ));
            datarows.push(Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![(format!("key21"), Datavalue::Size(400_u64))],
            ));
        } // 11 new rows (log_name1 - new sheet to be created with headers), 22 cells

        log.append(datarows).await.unwrap();

        // 10% of the limit (100 cells or 50 rows per service, or 25 rows per `log_name..` sheet) should be removed

        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();
        assert_eq!(
            all_sheets.len(),
            4,
            "`log_name1` with another key has been created"
        );

        for i in 0..3 {
            if all_sheets[i].title().contains("log_name2") {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(249 - 25 + 5),
                    "`log_name2` contains header row and 228 rows of data"
                );
            } else if all_sheets[i].title().contains("log_name1") {
                assert!(
                    all_sheets[i].row_count() == Some(249 - 25)
                        || all_sheets[i].row_count() == Some(6),
                    "`log_name1` sheets contain header row and 223 or 5 rows of data"
                );
            } else {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(2),
                    "`{}` contains header row and 1 row of data",
                    RULES_LOG_NAME
                );
            }
        }

        // now we have
        // log_name1 - 1 sheet with 224 rows, 2 columns, 448 cells, 2 sheet with 6 rows, 2 columns, 12 cells
        // log_name2 - 229 rows, 2 columns, 458 cells
        // rules - 2 rows, 2 columns, 4 cells
        // total 448 + 12 + 458 + 4 = 922 cells

        // let's add log_name1 with 500 * 2 == 1000 cells.
        let datarows = vec![
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![(format!("key12"), Datavalue::Size(400_u64))],
            );
            500
        ];
        log.append(datarows).await.unwrap();

        // in this case we have 1922 cells.
        // we remove surplus (922 cells) and 10% of the limit (100 cells)
        // as row counts (and cells counts) of log_name2 and log_name1 are roughly equal
        // log_name1  448 + 12 + 1000 = 1460 cells
        // log_name2 458 cells
        // then both sheets should be removed
        // 1022 * 1460/(1460+458) = 777 cells to remove from log_name1 or 388 rows
        // so the old log_name1 with 224 rows is deleted completely
        // and 165 rows should be removed from the new log_name1
        // 244 cells to remove from log_name2 or 122 rows

        let datarows = vec![
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![(format!("key21"), Datavalue::Size(400_u64))],
            );
            2
        ];
        log.append(datarows).await.unwrap(); // we made an append

        let all_sheets = log
            .storage
            .google
            .sheets_filtered_by_metadata(&log.spreadsheet_id, &Metadata::new(vec![]))
            .await
            .unwrap();

        assert_eq!(
            all_sheets.len(),
            3,
            "`log_name1` with another key, `log_name2`, `rules` should remain"
        );

        for i in 0..3 {
            if all_sheets[i].title().contains("log_name1") {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(6 + 500 - 165),
                    "`log_name1` contains header row and 340 rows of data"
                );
            } else if all_sheets[i].title().contains("log_name2") {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(229 + 2 - 122),
                    "`log_name2` contains header row and 108 rows of data"
                );
            } else {
                assert_eq!(
                    all_sheets[i].row_count(),
                    Some(2),
                    "`{}` contains header row and 1 row of data",
                    RULES_LOG_NAME
                );
            }
        }
    }
}
