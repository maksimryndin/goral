use crate::rules::{rules_dropdowns, Rule, RULES_LOG_NAME};
use crate::spreadsheet::datavalue::Datarow;
use crate::spreadsheet::sheet::{Rows, Sheet, SheetId, TabColorRGB, UpdateSheet, VirtualSheet};
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

pub fn create_log(storage: Arc<Storage>, spreadsheet_id: String, service: String) -> AppendableLog {
    let tab_color_rgb = get_service_tab_color(&service);
    AppendableLog {
        storage,
        spreadsheet_id,
        service,
        tab_color_rgb,
        rules_sheet_id: None,
    }
}

pub struct AppendableLog {
    storage: Arc<Storage>,
    service: String,
    spreadsheet_id: String,
    tab_color_rgb: TabColorRGB,
    rules_sheet_id: Option<SheetId>,
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
        let rows_count = datarows.len();
        tracing::info!(
            "appending to log {} rows for service {}",
            rows_count,
            self.service
        );
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
        tracing::info!(
            "appended to log {} rows for service {}",
            rows_count,
            self.service
        );
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

        tracing::debug!("sheets_to_update:\n{:?}", sheets_to_update);
        tracing::debug!("sheets_to_add:\n{:?}", sheets_to_add);
        tracing::debug!("data:\n{:?}", data);

        if sheets_to_add.is_empty() && sheets_to_update.is_empty() {
            return Ok(());
        }

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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
            ),
        ];

        log.append(datarows).await.unwrap();

        // for the sheet order we rely on the indexing
        let (all_sheets, _, _) = log
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
                None,
            ),
            Datarow::new(
                "log_name1".to_string(),
                timestamp,
                vec![
                    (format!("key12"), Datavalue::Size(400_u64)),
                    (format!("key11"), Datavalue::HeatmapPercent(3_f64)),
                ],
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
            ),
            Datarow::new(
                "log_name3".to_string(),
                timestamp,
                vec![
                    (format!("key31"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key32"), Datavalue::Size(400_u64)),
                ],
                None,
            ),
        ];

        log.append(datarows).await.unwrap();
        let (all_sheets, _, _) = log
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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
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
                None,
            ),
            Datarow::new(
                "log_name2".to_string(),
                timestamp,
                vec![
                    (format!("key21"), Datavalue::HeatmapPercent(3_f64)),
                    (format!("key22"), Datavalue::Size(400_u64)),
                ],
                None,
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
}
