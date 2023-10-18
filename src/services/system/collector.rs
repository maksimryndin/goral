use crate::storage::Datavalue;
use chrono::{DateTime, NaiveDateTime, Utc};
use psutil::process::{self, processes, Process as PsutilProcess};
use psutil::{cpu, disk, host, memory, Count, Percent};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{self, Display, Write};
use std::io::{self, BufRead, Write as OtherWrite};
use std::path::Path;
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use sysinfo::{
    Pid, PidExt, Process as SysinfoProcess, ProcessExt, System, SystemExt, Uid, UserExt,
};

#[derive(Debug, Clone)]
struct ProcessInfo {
    pid: u32,
    name: String,
    user_id: Option<Uid>,
    effective_user_id: Option<Uid>,
    memory_used: u64,
    memory_use: Percent,
    open_files: Option<usize>,
    disk_read: u64,
    disk_write: u64,
    start_time: NaiveDateTime,
    cpu_percent: Percent,
}

impl ProcessInfo {
    fn from(
        psutil_process: &mut PsutilProcess,
        sysinfo_processes: &HashMap<Pid, SysinfoProcess>,
    ) -> Result<Self, String> {
        let sysinfo_process = sysinfo_processes
            .get(&Pid::from_u32(psutil_process.pid()))
            .ok_or_else(|| format!("process {} not found", psutil_process.pid()))?;
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
        Ok(Self {
            pid: psutil_process.pid(),
            name,
            user_id: sysinfo_process.user_id().cloned(),
            effective_user_id: sysinfo_process.effective_user_id().cloned(),
            memory_used: psutil_process
                .memory_info()
                .map_err(|e| e.to_string())?
                .rss(),
            memory_use: psutil_process.memory_percent().map_err(|e| e.to_string())?,
            open_files: psutil_process.open_files().ok().map(|files| files.len()),
            start_time: NaiveDateTime::from_timestamp_opt(
                sysinfo_process
                    .start_time()
                    .try_into()
                    .expect("assert: it is possible to build datetime from process start time"),
                0,
            )
            .expect("assert: process start_time timestamp should be valid"),
            cpu_percent: psutil_process.cpu_percent().map_err(|e| e.to_string())?,
            disk_read: sysinfo_process.disk_usage().read_bytes,
            disk_write: sysinfo_process.disk_usage().written_bytes,
        })
    }
}

fn top_cpu_process<'a, 'b>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| (p.cpu_percent * 100.0) as u32);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_read_process<'a, 'b>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_disk_write_process<'a, 'b>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.disk_read);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_memory_process<'a, 'b>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.memory_used);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_open_files_process<'a, 'b>(processes: &'a mut Vec<ProcessInfo>) -> &'a ProcessInfo {
    processes.sort_unstable_by_key(|p| p.open_files);
    processes
        .last()
        .expect("assert: processes list should contain at least one process")
}

fn top_process_to_values(
    process: &ProcessInfo,
    prefix: &str,
    sys: &mut System,
) -> [(String, Datavalue); 5] {
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
    [
        (
            format!("{prefix}pid"),
            Datavalue::Integer(process.pid as u64),
        ),
        (
            format!("{prefix}name"),
            Datavalue::Text(process.name.to_string()),
        ),
        (format!("{prefix}user"), Datavalue::Text(user.to_string())),
        (
            format!("{prefix}effective_user"),
            Datavalue::Text(effective_user.to_string()),
        ),
        (
            format!("{prefix}start_time"),
            Datavalue::Datetime(process.start_time),
        ),
    ]
}

fn process_to_values(
    process: &ProcessInfo,
    prefix: &str,
    sys: &mut System,
) -> [(String, Datavalue); 10] {
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
        Datavalue::Text("NA".to_string())
    };
    [
        (
            format!("{prefix}_pid"),
            Datavalue::Text(process.pid.to_string()),
        ),
        (
            format!("{prefix}_name"),
            Datavalue::Text(process.name.to_string()),
        ),
        (format!("{prefix}_user"), Datavalue::Text(user.to_string())),
        (
            format!("{prefix}_effective_user"),
            Datavalue::Text(effective_user.to_string()),
        ),
        (
            format!("{prefix}_start_time"),
            Datavalue::Datetime(process.start_time),
        ),
        (
            format!("{prefix}_memory_used"),
            Datavalue::Integer(process.memory_used as u64),
        ),
        (
            format!("{prefix}_memory_use"),
            Datavalue::Percent(process.memory_use as f64),
        ),
        (
            format!("{prefix}_cpu"),
            Datavalue::Percent(process.cpu_percent as f64),
        ),
        (
            format!("{prefix}_disk_read"),
            Datavalue::Integer(process.disk_read as u64),
        ),
        (format!("{prefix}_open_files"), open_files),
    ]
}

pub(super) fn initialize() -> (System, cpu::CpuPercentCollector) {
    sysinfo::set_open_files_limit(0);
    let cpu_percent_collector = cpu::CpuPercentCollector::new().unwrap();
    let mut sys = System::new();
    sys.refresh_processes();
    sys.refresh_users_list();
    (sys, cpu_percent_collector)
}

pub(super) fn collect(
    mut sys: &mut System,
    cpu_percent_collector: &mut cpu::CpuPercentCollector,
    mounts: &[String],
    names: &[String],
) -> Result<Vec<(String, Datavalue)>, String> {
    sys.refresh_processes();
    sys.refresh_users_list();
    let psutil_processes = processes().expect("cannot get processes data");
    let sysinfo_processes = sys.processes();

    let mut processes_infos = Vec::with_capacity(psutil_processes.len());
    for p in psutil_processes.into_iter() {
        let mut proc = match p {
            Ok(p) => p,
            Err(_) => continue,
        };
        if let Ok(pi) = ProcessInfo::from(&mut proc, sysinfo_processes) {
            processes_infos.push(pi);
        } else {
            println!("cannot get proc info for pid {}", proc.pid());
        }
    }

    let pids = process::pids().map_err(|e| format!("cannot get pids `{}`", e))?;
    let swap_memory = memory::swap_memory().map_err(|e| format!("cannot get swap stat `{}`", e))?;
    let virtual_memory =
        memory::virtual_memory().map_err(|e| format!("cannot get memory stat `{}`", e))?;
    let boot_time = NaiveDateTime::from_timestamp_opt(
        sys.boot_time()
            .try_into()
            .expect("assert: it is possible to build datetime from system boot time"),
        0,
    )
    .expect("assert: system boot time timestamp should be valid");
    let general = [
        ("boot_time".to_string(), Datavalue::Datetime(boot_time)),
        (
            "memory_available".to_string(),
            Datavalue::Integer(virtual_memory.available()),
        ),
        (
            "memory_usage_percent".to_string(),
            Datavalue::Percent(virtual_memory.percent() as f64),
        ),
        (
            "swap_used".to_string(),
            Datavalue::Integer(swap_memory.used()),
        ),
        (
            "swap_usage_percent".to_string(),
            Datavalue::Percent(swap_memory.percent() as f64),
        ),
        //("processes_names".to_string(), Datavalue::Text(proc_infos.len()  as f64)), // pid name cmd uid euid
        //("users".to_string(), Datavalue::Text(proc_infos.len()  as f64)), // pid name cmd uid euid
        (
            "num_of_processes".to_string(),
            Datavalue::Integer(pids.len() as u64),
        ),
    ];
    let mut disk_stat = Vec::with_capacity(mounts.len() * 2);
    for mount in mounts {
        let stat = disk::disk_usage(mount).unwrap();
        disk_stat.push((
            format!("{mount}_disk_use_percent"),
            Datavalue::Number(stat.percent() as f64),
        ));
        disk_stat.push((
            format!("{mount}_disk_free"),
            Datavalue::Number(stat.free() as f64),
        ));
    }
    let cpu = cpu_percent_collector
        .cpu_percent_percpu()
        .map_err(|e| format!("cannot cpu percentages `{}`", e))?;
    let cpus = cpu
        .into_iter()
        .enumerate()
        .map(|(i, p)| (format!("cpu{i}"), Datavalue::Number(p as f64)));
    let top_cpu = top_cpu_process(&mut processes_infos);
    let top_cpu_values = top_process_to_values(top_cpu, "top_cpu_", &mut sys);
    let top_memory = top_memory_process(&mut processes_infos);
    let top_memory_values = top_process_to_values(top_memory, "top_memory_", &mut sys);
    let top_read = top_disk_read_process(&mut processes_infos);
    let top_read_values = top_process_to_values(top_read, "top_disk_read_", &mut sys);
    let top_write = top_disk_write_process(&mut processes_infos);
    let top_write_values = top_process_to_values(top_write, "top_disk_write_", &mut sys);
    let top_open_files = top_disk_write_process(&mut processes_infos);
    let top_open_files_values = top_process_to_values(top_open_files, "top_open_files_", &mut sys);

    let mut values: Vec<(String, Datavalue)> = general
        .into_iter()
        .chain(cpus)
        .chain(disk_stat.into_iter())
        .chain(top_cpu_values.into_iter())
        .chain(top_memory_values.into_iter())
        .chain(top_read_values.into_iter())
        .chain(top_open_files_values.into_iter())
        .collect();
    values.reserve(names.len() * 10);
    for name in names {
        if let Some(p) = processes_infos.iter().find(|p| p.name.contains(name)) {
            let p_values = process_to_values(p, &name, &mut sys);
            values.append(&mut p_values.into_iter().collect());
        } else {
            println!("process containing `{name}` in its name is not found");
        }
    }
    Ok(values)
}
