use google_sheets4::api::Sheet as GoogleSheet;
use google_sheets4::api::{
    CellData, CellFormat, Color, ColorStyle, DeveloperMetadata, DeveloperMetadataLocation,
    ExtendedValue, TextFormat,
};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hasher;

pub const METADATA_SERVICE_KEY: &str = "service";
pub const METADATA_HOSTD_ID_KEY: &str = "host";
pub(crate) type SheetId = i32;

#[derive(Debug, PartialEq)]
pub(crate) enum SheetType {
    Grid,
    Chart,
    Other,
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
                        red: Some(0.5),
                        green: Some(0.5),
                        blue: Some(0.5),
                    }),
                    ..Default::default()
                }),
                horizontal_alignment: Some("CENTER".to_string()),
                text_format: Some(TextFormat {
                    bold: Some(true),
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
    pub(super) title: String, // 50 characters
    pub(super) hidden: bool,
    pub(super) index: u8,
    pub(super) sheet_type: SheetType,
    // for Grid sheets - we use the same type (i32) as an upstream libs
    pub(super) frozen_row_count: Option<i32>,
    pub(super) row_count: Option<i32>,
    pub(super) column_count: Option<i32>,
    pub(super) headers: Vec<Header>,
    pub(super) service: String,
    pub(super) host_id: String,
    // TODO last_row_timestamp
}

impl Sheet {
    pub(crate) fn header_range_r1c1(&self) -> Option<String> {
        if self.sheet_type == SheetType::Grid {
            Some(format!(
                "{}!R1C1:R1C{}",
                self.title,
                self.column_count.unwrap()
            ))
        } else {
            None
        }
    }

    pub(crate) fn number_of_cells(&self) -> Option<i32> {
        if self.sheet_type == SheetType::Grid {
            Some(self.row_count.unwrap() * self.column_count.unwrap())
        } else {
            None
        }
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
        let headers = sheet_headers(&mut sh);
        let mut metadata: HashMap<String, String> = sh
            .developer_metadata
            .take()
            .unwrap_or(vec![])
            .into_iter()
            .map(|meta| (meta.metadata_key.unwrap(), meta.metadata_value.unwrap()))
            .collect();
        let properties = sh.properties.expect("sheet properties cannot be null");
        Self {
            sheet_id: properties.sheet_id.expect("sheet sheet_id cannot be null"),
            title: properties.title.expect("sheet title cannot be null"),
            hidden: properties.hidden.unwrap_or(false),
            index: properties.index.expect("sheet index cannot be null") as u8,
            sheet_type: properties
                .sheet_type
                .expect("sheet type cannot be null")
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
            service: metadata
                .remove(METADATA_SERVICE_KEY)
                .unwrap_or_else(|| "".to_string()),
            host_id: metadata
                .remove(METADATA_HOSTD_ID_KEY)
                .unwrap_or_else(|| "".to_string()),
            headers,
        }
    }
}

#[derive(Debug)]
pub(crate) struct VirtualSheet {
    pub(super) sheet: Sheet,
}

impl VirtualSheet {
    pub(crate) fn is_equal_to(&self, sheet: &Sheet) -> bool {
        self.sheet.title == sheet.title
            && self.sheet.sheet_type == sheet.sheet_type
            && self.sheet.headers == sheet.headers
            && self.sheet.service == sheet.service
            && self.sheet.host_id == sheet.host_id
    }

    pub(super) fn generate_developer_metadata(&self) -> Vec<DeveloperMetadata> {
        vec![
            DeveloperMetadata {
                location: Some(DeveloperMetadataLocation {
                    sheet_id: Some(self.sheet.sheet_id),
                    ..Default::default()
                }),
                metadata_key: Some(METADATA_SERVICE_KEY.to_string()),
                metadata_value: Some(self.sheet.service.clone()),
                visibility: Some("PROJECT".to_string()),
                metadata_id: Some(str_to_id(
                    format!(
                        "{}{}{}{}",
                        self.sheet.host_id,
                        self.sheet.service,
                        self.sheet.sheet_id,
                        METADATA_SERVICE_KEY
                    )
                    .as_str(),
                )),
            },
            DeveloperMetadata {
                location: Some(DeveloperMetadataLocation {
                    sheet_id: Some(self.sheet.sheet_id),
                    ..Default::default()
                }),
                metadata_key: Some(METADATA_HOSTD_ID_KEY.to_string()),
                metadata_value: Some(self.sheet.host_id.clone()),
                visibility: Some("PROJECT".to_string()),
                metadata_id: Some(str_to_id(
                    format!(
                        "{}{}{}{}",
                        self.sheet.host_id,
                        self.sheet.service,
                        self.sheet.sheet_id,
                        METADATA_HOSTD_ID_KEY
                    )
                    .as_str(),
                )),
            },
        ]
    }

    pub(crate) fn new_grid(
        title: String,
        service: String,
        host_id: String,
        headers: Vec<Header>,
    ) -> Self {
        Self::new(title, false, SheetType::Grid, service, host_id, headers)
    }

    fn new(
        title: String,
        hidden: bool,
        sheet_type: SheetType,
        service: String,
        host_id: String,
        headers: Vec<Header>,
    ) -> Self {
        let sheet = Sheet {
            sheet_id: str_to_id(format!("{}{}{}", host_id, service, title).as_str()),
            title, // TODO no more than 50 chars
            hidden,
            index: 0,
            sheet_type,
            // for Grid sheets - we use the same type (i32) as an upstream libs
            frozen_row_count: Some(1),
            row_count: Some(2), // for headers and 1 row for data is required otherwise "You can't freeze all visible rows on the sheet.","status":"INVALID_ARGUMENT""
            column_count: Some(headers.len() as i32),
            service,
            host_id,
            headers,
        };
        Self { sheet }
    }
}

pub(crate) fn filter_virtual_sheets(
    virtual_sheets: Vec<VirtualSheet>,
    sheets: &Vec<Sheet>,
) -> Vec<VirtualSheet> {
    let sheets: HashMap<&String, &Sheet> = sheets.iter().map(|s| (&s.title, s)).collect();
    virtual_sheets
        .into_iter()
        .filter(|vs| {
            sheets
                .get(&vs.sheet.title)
                .map(|sheet| vs.is_equal_to(sheet))
                != Some(true)
        })
        .collect()
}

pub(super) fn sheet_headers(sh: &mut GoogleSheet) -> Vec<Header> {
    let cells = sh
        .data
        .as_mut()
        .and_then(|v| v.pop())
        .and_then(|grid_data| grid_data.row_data)
        .and_then(|mut v| v.pop())
        .and_then(|row_data| row_data.values)
        .unwrap_or(vec![]);
    cells
        .into_iter()
        .map(|c| Header {
            title: stringify_cell_value(c.effective_value.unwrap()),
            note: c.note,
        })
        .collect()
}

fn stringify_cell_value(value: ExtendedValue) -> String {
    if let Some(s) = value.string_value {
        return s;
    }

    if let Some(n) = value.number_value {
        return n.to_string();
    }

    if let Some(b) = value.bool_value {
        return b.to_string();
    }

    if let Some(f) = value.formula_value {
        return f;
    }

    if let Some(e) = value.error_value {
        return e.type_.unwrap();
    }

    // TODO notify via general messenger??
    tracing::warn!("unhandled field of ExtendedValue");
    "undefined".to_string()
}

fn str_to_id(s: &str) -> i32 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_bytes());
    let bytes = hasher.finish().to_be_bytes();
    (u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as i32).abs()
}
