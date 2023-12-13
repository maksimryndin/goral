use crate::spreadsheet::{Metadata, DEFAULT_FONT};
use google_sheets4::api::Sheet as GoogleSheet;
use google_sheets4::api::{
    AddSheetRequest, AppendCellsRequest, BasicFilter, BooleanCondition, CellData, CellFormat,
    Color, ColorStyle, ConditionValue, CreateDeveloperMetadataRequest, DataValidationRule,
    DeveloperMetadata, DeveloperMetadataLocation, ExtendedValue, GridProperties, GridRange,
    Request, RowData, SetBasicFilterRequest, SetDataValidationRequest, SheetProperties, TextFormat,
    UpdateCellsRequest, UpdateDeveloperMetadataRequest,
};
use google_sheets4::FieldMask;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hasher;
use std::mem;
use std::str::FromStr;

pub(crate) type SheetId = i32;
pub(crate) type TabColorRGB = (f32, f32, f32);

#[derive(Debug, PartialEq)]
pub(crate) enum SheetType {
    Grid,
    Chart,
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Dropdown {
    pub(crate) values: Vec<String>,
    pub(crate) column_index: u16,
}

impl From<String> for SheetType {
    fn from(t: String) -> Self {
        match t.as_str() {
            "GRID" => SheetType::Grid,
            "OBJECT" => SheetType::Chart,
            _ => SheetType::Other,
        }
    }
}

impl fmt::Display for SheetType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SheetType::Grid => write!(f, "GRID"),
            SheetType::Chart => write!(f, "OBJECT"),
            SheetType::Other => write!(f, "SHEET_TYPE_UNSPECIFIED"),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Header {
    title: String,
    note: Option<String>,
}

impl Header {
    pub(crate) fn new(title: String, note: Option<String>) -> Self {
        Self { title, note }
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}
impl Eq for Header {}

impl Into<CellData> for Header {
    fn into(self) -> CellData {
        CellData {
            user_entered_value: Some(ExtendedValue {
                string_value: Some(self.title),
                ..Default::default()
            }),
            user_entered_format: Some(CellFormat {
                background_color_style: Some(ColorStyle {
                    rgb_color: Some(Color {
                        alpha: Some(0.0),
                        red: Some(0.0),
                        green: Some(0.0),
                        blue: Some(0.0),
                    }),
                    ..Default::default()
                }),
                horizontal_alignment: Some("CENTER".to_string()),
                text_format: Some(TextFormat {
                    bold: Some(true),
                    font_family: Some(DEFAULT_FONT.to_string()),
                    foreground_color_style: Some(ColorStyle {
                        rgb_color: Some(Color {
                            alpha: Some(0.0),
                            red: Some(1.0),
                            green: Some(1.0),
                            blue: Some(1.0),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            note: self.note,
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub(crate) struct Sheet {
    pub(super) sheet_id: SheetId,
    pub(super) title: String,
    pub(super) hidden: bool,
    pub(super) index: u8,
    pub(super) sheet_type: SheetType,
    // for Grid sheets - we use the same type (i32) as an upstream libs
    pub(super) frozen_row_count: Option<i32>,
    pub(super) row_count: Option<i32>,
    pub(super) column_count: Option<i32>,
    pub(super) metadata: Metadata,
    pub(super) tab_color: TabColorRGB,
}

impl Sheet {
    pub(crate) fn number_of_cells(&self) -> Option<i32> {
        if self.sheet_type == SheetType::Grid {
            Some(
                self.row_count
                    .expect("assert: grid sheet contains row count")
                    * self
                        .column_count
                        .expect("assert: grid sheet contains column count"),
            )
        } else {
            None
        }
    }

    pub(crate) fn meta_value(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    pub(crate) fn sheet_id(&self) -> SheetId {
        self.sheet_id
    }

    pub(crate) fn row_count(&self) -> Option<i32> {
        self.row_count
    }
}

impl PartialEq for Sheet {
    fn eq(&self, other: &Self) -> bool {
        self.sheet_id == other.sheet_id
    }
}
impl Eq for Sheet {}

impl From<GoogleSheet> for Sheet {
    fn from(mut sh: GoogleSheet) -> Self {
        let metadata: HashMap<String, String> = sh
            .developer_metadata
            .take()
            .unwrap_or(vec![])
            .into_iter()
            .map(|meta| {
                (
                    meta.metadata_key
                        .expect("assert: if sheet has metadata entry, it has key"),
                    meta.metadata_value
                        .expect("assert: if sheet has metadata entry, it has value"),
                )
            })
            .collect();
        let properties = sh
            .properties
            .expect("assert: sheet properties cannot be null");
        Self {
            sheet_id: properties
                .sheet_id
                .expect("assert: sheet sheet_id cannot be null"),
            title: properties
                .title
                .expect("assert: sheet title cannot be null"),
            hidden: properties.hidden.unwrap_or(false),
            index: properties
                .index
                .expect("assert: sheet index cannot be null") as u8,
            sheet_type: properties
                .sheet_type
                .expect("assert: sheet type cannot be null")
                .into(),
            frozen_row_count: properties
                .grid_properties
                .as_ref()
                .and_then(|gp| gp.frozen_row_count),
            row_count: properties
                .grid_properties
                .as_ref()
                .and_then(|gp| gp.row_count),
            column_count: properties
                .grid_properties
                .as_ref()
                .and_then(|gp| gp.column_count),
            metadata: Metadata::from(metadata),
            tab_color: properties
                .tab_color_style
                .and_then(|tcs| tcs.rgb_color)
                .map(|rgb_color| {
                    (
                        rgb_color.red.unwrap_or(0.0),
                        rgb_color.green.unwrap_or(0.0),
                        rgb_color.blue.unwrap_or(0.0),
                    )
                })
                .unwrap_or((0.0, 0.0, 0.0)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct VirtualSheet {
    pub(super) sheet: Sheet,
    pub(super) headers: Vec<Header>,
    pub(super) dropdowns: Vec<Dropdown>,
}

impl VirtualSheet {
    fn take_developer_metadata(&mut self) -> Vec<DeveloperMetadata> {
        let metadata = mem::replace(&mut self.sheet.metadata, Metadata::new(vec![]));
        metadata
            .0
            .into_iter()
            .map(|(k, v)| DeveloperMetadata {
                metadata_id: Some(generate_metadata_id(&k, self.sheet.sheet_id)),
                location: Some(DeveloperMetadataLocation {
                    sheet_id: Some(self.sheet.sheet_id),
                    ..Default::default()
                }),
                metadata_key: Some(k),
                metadata_value: Some(v),
                visibility: Some("PROJECT".to_string()),
            })
            .collect()
    }

    pub(super) fn into_api_requests(mut self) -> Vec<Request> {
        let mut requests = vec![];
        let grid_properties = if self.sheet.sheet_type == SheetType::Grid {
            assert!(
                self.sheet
                    .column_count
                    .expect("assert: grid sheet has column count")
                    > 0
            );
            Some(GridProperties {
                column_count: self.sheet.column_count,
                row_count: self.sheet.row_count,
                frozen_row_count: self.sheet.frozen_row_count,
                ..Default::default()
            })
        } else {
            None
        };

        let range = GridRange {
            sheet_id: Some(self.sheet.sheet_id),
            start_row_index: Some(0),
            end_row_index: Some(1),
            start_column_index: Some(0),
            end_column_index: self.sheet.column_count,
        };
        let metadata = self.take_developer_metadata();
        let header_values: Vec<CellData> = self.headers.into_iter().map(|h| h.into()).collect();
        let data = vec![RowData {
            values: Some(header_values),
        }];

        requests.push(Request {
            add_sheet: Some(AddSheetRequest {
                properties: Some(SheetProperties {
                    sheet_id: Some(self.sheet.sheet_id),
                    hidden: Some(self.sheet.hidden),
                    sheet_type: Some(self.sheet.sheet_type.to_string()),
                    grid_properties,
                    title: Some(self.sheet.title),
                    tab_color_style: Some(ColorStyle {
                        rgb_color: Some(Color {
                            alpha: Some(0.0),
                            red: Some(self.sheet.tab_color.0),
                            green: Some(self.sheet.tab_color.1),
                            blue: Some(self.sheet.tab_color.2),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        });
        requests.push(Request {
            update_cells: Some(UpdateCellsRequest {
                fields: Some(
                    FieldMask::from_str("userEnteredValue,userEnteredFormat,note")
                        .expect("assert: field mask can be constructed from static str"),
                ),
                range: Some(range),
                rows: Some(data),
                ..Default::default()
            }),
            ..Default::default()
        });

        for dropdown in self.dropdowns {
            let Dropdown {
                values,
                column_index,
            } = dropdown;
            requests.push(Request {
                set_data_validation: Some(SetDataValidationRequest {
                    rule: Some(DataValidationRule {
                        condition: Some(BooleanCondition {
                            type_: Some("ONE_OF_LIST".to_string()),
                            values: Some(
                                values
                                    .into_iter()
                                    .map(|v| ConditionValue {
                                        user_entered_value: Some(v),
                                        ..Default::default()
                                    })
                                    .collect(),
                            ),
                        }),
                        show_custom_ui: Some(true),
                        strict: Some(true),
                        ..Default::default()
                    }),
                    range: Some(GridRange {
                        sheet_id: Some(self.sheet.sheet_id),
                        start_row_index: Some(1),
                        start_column_index: Some(column_index as i32),
                        end_column_index: Some(column_index as i32 + 1),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }

        for m in metadata {
            requests.push(Request {
                create_developer_metadata: Some(CreateDeveloperMetadataRequest {
                    developer_metadata: Some(m),
                }),
                ..Default::default()
            })
        }
        requests
    }

    pub(crate) fn new_grid(
        sheet_id: SheetId,
        title: String,
        headers: Vec<Header>,
        metadata: Metadata,
        tab_color: TabColorRGB,
    ) -> Self {
        Self::new(
            sheet_id,
            title,
            SheetType::Grid,
            headers,
            metadata,
            tab_color,
        )
    }

    fn new(
        sheet_id: SheetId,
        title: String,
        sheet_type: SheetType,
        headers: Vec<Header>,
        metadata: Metadata,
        tab_color: TabColorRGB,
    ) -> Self {
        let sheet = Sheet {
            sheet_id,
            title,
            hidden: false,
            index: 0,
            sheet_type,
            // for Grid sheets - we use the same type (i32) as an upstream libs
            frozen_row_count: Some(1),
            row_count: Some(2), // for headers and one row empty otherwise `You can't freeze all visible rows on the sheet`
            column_count: Some(headers.len() as i32),
            tab_color,
            metadata,
        };
        Self {
            sheet,
            headers,
            dropdowns: vec![],
        }
    }

    pub(crate) fn with_dropdowns(mut self, dropdowns: Vec<Dropdown>) -> Self {
        self.dropdowns = dropdowns;
        self
    }

    pub(crate) fn sheet_id(&self) -> SheetId {
        self.sheet.sheet_id()
    }

    pub(crate) fn row_count(&self) -> Option<i32> {
        self.sheet.row_count()
    }
}

pub(crate) fn str_to_id(s: &str) -> i32 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_bytes());
    let bytes = hasher.finish().to_be_bytes();
    (u32::from_be_bytes(
        bytes[4..8]
            .try_into()
            .expect("assert: u32 is created from 4 bytes"),
    ) as i32)
        .abs()
}

fn generate_metadata_id(key: &str, sheet_id: SheetId) -> i32 {
    str_to_id(&format!("{}{}", sheet_id, key))
}

#[derive(Debug)]
pub(crate) struct UpdateSheet {
    sheet_id: SheetId,
    metadata: Metadata,
}

impl UpdateSheet {
    pub(crate) fn new(sheet_id: SheetId, metadata: Metadata) -> Self {
        Self { sheet_id, metadata }
    }

    pub(super) fn into_api_requests(self) -> Vec<Request> {
        let mut requests = Vec::with_capacity(1 + self.metadata.0.len());
        let UpdateSheet { sheet_id, metadata } = self;
        for (k, v) in metadata.0.into_iter() {
            requests.push(Request {
                update_developer_metadata: Some(UpdateDeveloperMetadataRequest {
                    developer_metadata: Some(DeveloperMetadata {
                        metadata_id: Some(generate_metadata_id(&k, sheet_id)),
                        metadata_value: Some(v),
                        ..Default::default()
                    }),
                    fields: Some(
                        FieldMask::from_str("metadataId,metadataValue")
                            .expect("assert: field mask can be constructed from static str"),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
        requests
    }
}

#[derive(Debug)]
pub(crate) struct Rows {
    sheet_id: SheetId,
    row_count: i32,
    rows: Vec<RowData>,
}

impl Rows {
    pub(crate) fn new(sheet_id: SheetId, row_count: i32) -> Self {
        Self {
            sheet_id,
            row_count,
            rows: vec![],
        }
    }

    pub(crate) fn push(&mut self, row: RowData) {
        self.rows.push(row)
    }

    pub(super) fn into_api_requests(self) -> Vec<Request> {
        let filter_range = GridRange {
            sheet_id: Some(self.sheet_id),
            start_row_index: Some(0),
            end_row_index: Some(self.row_count + self.rows.len() as i32),
            start_column_index: Some(0),
            end_column_index: None,
        };
        vec![
            Request {
                append_cells: Some(AppendCellsRequest {
                    fields: Some(
                        FieldMask::from_str("userEnteredValue,userEnteredFormat")
                            .expect("assert: field mask can be constructed from static str"),
                    ),
                    sheet_id: Some(self.sheet_id),
                    rows: Some(self.rows),
                }),
                ..Default::default()
            },
            Request {
                set_basic_filter: Some(SetBasicFilterRequest {
                    filter: Some(BasicFilter {
                        range: Some(filter_range),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            },
        ]
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    impl Sheet {
        pub(crate) fn title(&self) -> &str {
            &self.title
        }
    }

    pub(crate) fn mock_ordinary_google_sheet(title: &str) -> GoogleSheet {
        GoogleSheet {
            banded_ranges: None,
            basic_filter: None,
            charts: None,
            column_groups: None,
            conditional_formats: None,
            data: None,
            developer_metadata: None,
            filter_views: None,
            merges: None,
            properties: Some(SheetProperties {
                data_source_sheet_properties: None,
                grid_properties: Some(GridProperties {
                    column_count: Some(26),
                    column_group_control_after: None,
                    frozen_column_count: None,
                    frozen_row_count: None,
                    hide_gridlines: None,
                    row_count: Some(1000),
                    row_group_control_after: None,
                }),
                hidden: None,
                index: Some(0),
                right_to_left: None,
                sheet_id: Some(0),
                sheet_type: Some("GRID".to_string()),
                tab_color: None,
                tab_color_style: Some(ColorStyle {
                    rgb_color: Some(Color {
                        alpha: None,
                        blue: None,
                        green: None,
                        red: Some(1.0),
                    }),
                    theme_color: None,
                }),
                title: Some(title.to_string()),
            }),
            protected_ranges: None,
            row_groups: None,
            slicers: None,
        }
    }

    pub(crate) fn mock_sheet_with_properties(properties: SheetProperties) -> GoogleSheet {
        GoogleSheet {
            banded_ranges: None,
            basic_filter: None,
            charts: None,
            column_groups: None,
            conditional_formats: None,
            data: None,
            developer_metadata: None,
            filter_views: None,
            merges: None,
            properties: Some(properties),
            protected_ranges: None,
            row_groups: None,
            slicers: None,
        }
    }

    #[test]
    fn id_generation() {
        let id = str_to_id("some text to generate id from");
        assert!(id > 0, "generated id should be positive");
    }
}
