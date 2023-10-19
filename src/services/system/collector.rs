use crate::storage::{Datarow, Datavalue};
use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::time::Duration;
use sysinfo::{
    CpuExt, DiskExt, Pid, PidExt, Process as SysinfoProcess, ProcessExt, System, SystemExt, Uid,
    UserExt,
};

#[cfg(target_os = "linux")]
fn open_files(pid: Pid) -> Option<usize> {
    let dir = format!("/proc/{pid}/fd");
    let path = Path::new(&dir);
    Some(path.read_dir().ok()?.count())
}

#[cfg(not(target_os = "linux"))]
fn open_files(pid: Pid) -> Option<usize> {
    None
}

#[derive(Debug, Clone)]
struct ProcessInfo {
    pid: Pid,
    name: String,
    user_id: Option<Uid>,
    effective_user_id: Option<Uid>,
    cpu_percent: f32,
    memory_used: u64,
    memory_use: f32,
    disk_read: u64,
    disk_write: u64,
    start_time: NaiveDateTime,
    open_files: Option<usize>, // only for Linux and not for all processes
}

impl ProcessInfo {
    fn from(sysinfo_process: &SysinfoProcess, total_memory: f32) -> Self {
        let name = if let Some(name) = sysinfo_process.exe().file_name() {
            name.to_string_lossy().into_owned()
        } else if let Some(cmd) = sysinfo_process.cmd().first() {
            Path::new(&cmd)
                .file_name()
                .expect("assert: should be able to get basename for process command first arg")
                .to_string_lossy()
                .into_owned()
        } else {
            sysinfo_process.name().to_string()
        };
        Self {
            pid: sysinfo_process.pid(),
            name,
            user_id: sysinfo_process.user_id().cloned(),
            effective_user_id: sysinfo_process.effective_user_id().cloned(),
            cpu_percent: sysinfo_process.cpu_usage(),
            memory_used: sysinfo_process.memory(),
            memory_use: 100.0 * sysinfo_process.memory() as f32 / total_memory,
            disk_read: sysinfo_process.disk_usage().read_bytes,
            disk_write: sysinfo_process.disk_usage().written_bytes,
            start_time: NaiveDateTime::from_timestamp_opt(
                sysinfo_process
                    .start_time()
                    .try_into()
                    .expect("assert: it is possible to build datetime from process start time"),
                0,
            )
            .expect("assert: process start_time timestamp should be valid"),
            open_files: open_files(sysinfo_process.pid()),
        }
    }
}

fn top_cpu_process<'a>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| (p.cpu_percent * 100.0) as u32);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_read_process<'a>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_write_process<'a>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_memory_process<'a>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.memory_used);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_open_files_process<'a>(processes: &'a mut Vec<ProcessInfo>) -> Option<&'a ProcessInfo> {
    processes.sort_unstable_by_key(|p| p.open_files);
    processes.last()
}

fn process_to_values(process: &ProcessInfo, sys: &mut System) -> Vec<(String, Datavalue)> {
    let user = process
        .user_id
        .as_ref()
        .and_then(|uid| sys.get_user_by_id(uid))
        .map(|u| u.name())
        .unwrap_or("unknown");
    let effective_user = process
        .effective_user_id
        .as_ref()
        .and_then(|uid| sys.get_user_by_id(uid))
        .map(|u| u.name())
        .unwrap_or("unknown");
    let open_files = if let Some(open_files) = process.open_files {
        Datavalue::Integer(open_files as u64)
    } else {
        Datavalue::NotAvailable
    };
    vec![
        (
            format!("pid"),
            Datavalue::IntegerID(process.pid.as_u32() as u64),
        ),
        (format!("name"), Datavalue::Text(process.name.to_string())),
        (format!("user"), Datavalue::Text(user.to_string())),
        (
            format!("effective_user"),
            Datavalue::Text(effective_user.to_string()),
        ),
        (
            format!("start_time"),
            Datavalue::Datetime(process.start_time),
        ),
        (format!("memory_used"), Datavalue::Size(process.memory_used)),
        (
            format!("memory_use"),
            Datavalue::HeatmapPercent(process.memory_use as f64),
        ),
        (
            format!("cpu"),
            Datavalue::HeatmapPercent(process.cpu_percent as f64),
        ),
        (format!("disk_read"), Datavalue::Size(process.disk_read)),
        (format!("disk_write"), Datavalue::Size(process.disk_write)),
        (format!("open_files"), open_files),
    ]
}

pub(super) fn initialize() -> System {
    sysinfo::set_open_files_limit(0);
    let mut sys = System::new();
    sys.refresh_memory();
    sys.refresh_processes();
    sys.refresh_users_list();
    sys
}

pub(super) fn collect(
    mut sys: &mut System,
    mounts: &[String],
    names: &[String],
    scrape_time: NaiveDateTime,
) -> Result<Vec<Datarow>, String> {
    sys.refresh_users_list();
    sys.refresh_memory();
    sys.refresh_cpu();
    sys.refresh_processes();
    thread::sleep(Duration::from_secs(1));
    sys.refresh_memory();
    sys.refresh_cpu();
    sys.refresh_processes();
    let sysinfo_processes = sys.processes();
    let total_memory = sys.total_memory();
    let mut processes_infos = Vec::with_capacity(sysinfo_processes.len());
    for (_, p) in sysinfo_processes.into_iter() {
        processes_infos.push(ProcessInfo::from(p, total_memory as f32));
    }

    let boot_time = NaiveDateTime::from_timestamp_opt(
        sys.boot_time()
            .try_into()
            .expect("assert: it is possible to build datetime from system boot time"),
        0,
    )
    .expect("assert: system boot time timestamp should be valid");
    let basic = [
        ("boot_time".to_string(), Datavalue::Datetime(boot_time)),
        (
            "memory_available".to_string(),
            Datavalue::Size(sys.available_memory()),
        ),
        (
            "memory_use".to_string(),
            Datavalue::HeatmapPercent(100.0 * sys.used_memory() as f64 / total_memory as f64),
        ),
        (
            "swap_available".to_string(),
            Datavalue::Size(sys.free_swap()),
        ),
        (
            "swap_use".to_string(),
            Datavalue::HeatmapPercent(100.0 * sys.used_swap() as f64 / sys.total_swap() as f64),
        ),
        (
            "num_of_processes".to_string(),
            Datavalue::Integer(sysinfo_processes.len() as u64),
        ),
    ];
    let cpus = sys.cpus().into_iter().enumerate().map(|(i, c)| {
        (
            format!("cpu{i}"),
            Datavalue::HeatmapPercent(c.cpu_usage() as f64),
        )
    });

    let basic_values: Vec<(String, Datavalue)> = basic.into_iter().chain(cpus).collect();

    // 1 for basic, 5 for top_ stats
    let mut datarows = Vec::with_capacity(1 + mounts.len() + 5 + names.len());
    datarows.push(Datarow::new(
        "basic".to_string(),
        scrape_time,
        basic_values,
        None,
    ));

    sys.refresh_disks_list();
    let mut mounts_stat: HashMap<String, (u64, u64)> = sys
        .disks()
        .into_iter()
        .map(|d| {
            (
                d.mount_point().to_string_lossy().into_owned(),
                (d.available_space(), d.total_space()),
            )
        })
        .collect();
    for mount in mounts {
        let (available, total) = match mounts_stat.remove(mount) {
            Some(stat) => stat,
            None => {
                tracing::warn!("mount `{mount}` is not found to collect disk statistics");
                continue;
            }
        };
        datarows.push(Datarow::new(
            mount.clone(),
            scrape_time,
            vec![
                (
                    format!("disk_use"),
                    Datavalue::HeatmapPercent(100.0 * (total - available) as f64 / total as f64),
                ),
                (format!("disk_free"), Datavalue::Size(available)),
            ],
            None,
        ));
    }

    let top_cpu = top_cpu_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_cpu".to_string(),
        scrape_time,
        process_to_values(top_cpu, &mut sys),
        None,
    ));
    let top_memory = top_memory_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_memory".to_string(),
        scrape_time,
        process_to_values(top_memory, &mut sys),
        None,
    ));
    let top_read = top_disk_read_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_disk_read".to_string(),
        scrape_time,
        process_to_values(top_read, &mut sys),
        None,
    ));
    let top_write = top_disk_write_process(&mut processes_infos);
    datarows.push(Datarow::new(
        "top_disk_write".to_string(),
        scrape_time,
        process_to_values(top_write, &mut sys),
        None,
    ));
    if let Some(top_open_files) = top_open_files_process(&mut processes_infos) {
        datarows.push(Datarow::new(
            "top_open_files".to_string(),
            scrape_time,
            process_to_values(top_open_files, &mut sys),
            None,
        ));
    }

    for name in names {
        if let Some(p) = processes_infos.iter().find(|p| p.name.contains(name)) {
            datarows.push(Datarow::new(
                name.clone(),
                scrape_time,
                process_to_values(p, &mut sys),
                None,
            ));
        } else {
            tracing::warn!("process containing `{name}` in its name is not found to collect process statistics");
        }
    }
    Ok(datarows)
}
