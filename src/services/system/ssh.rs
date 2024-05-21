use crate::google::datavalue::{Datarow, Datavalue};
use crate::notifications::{Notification, Sender};
use crate::services::{Data, TaskResult};
use chrono::{NaiveDateTime, Utc};
use lazy_static::lazy_static;
use logwatcher::{LogWatcher, LogWatcherAction, LogWatcherEvent};
use regex::Regex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender as TokioSender;
use tracing::Level;

pub const SSH_LOG: &str = "ssh";
pub const SSH_LOG_STATUS: &str = "status";
pub const SSH_LOG_STATUS_CONNECTED: &str = "connected";

pub(super) fn process_sshd_log(
    is_shutdown: Arc<AtomicBool>,
    sender: mpsc::Sender<TaskResult>,
    messenger: Sender,
    tx: TokioSender<()>,
) {
    tracing::info!("started ssh monitoring thread");

    let mut log_watcher = match LogWatcher::register("/var/log/auth.log")
        .or_else(|_| LogWatcher::register("/var/log/secure"))
    {
        Ok(f) => f,
        Err(_) => {
            let message =
                "cannot open auth log file, tried paths /var/log/auth.log and /var/log/secure"
                    .to_string();
            tracing::error!("{}, exiting ssh monitoring thread", message);
            messenger.send_nonblock(Notification::new(message, Level::ERROR));
            return;
        }
    };

    log_watcher.watch(&mut move |result| {
        let result = match result {
            Ok(event) => match event {
                LogWatcherEvent::Line(line) => match parse(&line) {
                    Some(line) => Ok(Data::Single(line)),
                    None => {
                        return LogWatcherAction::None;
                    }
                },
                LogWatcherEvent::LogRotation => {
                    tracing::info!("auth log file rotation");
                    return LogWatcherAction::None;
                }
            },
            Err(err) => {
                let message = format!("error watching ssh access log: {err}");
                Err(Data::Message(message))
            }
        };
        if sender.blocking_send(TaskResult { id: 0, result }).is_err() {
            if is_shutdown.load(Ordering::Relaxed) {
                return LogWatcherAction::Finish;
            }
            panic!(
                "assert: ssh monitoring messages queue shouldn't be closed before shutdown signal"
            );
        }
        LogWatcherAction::None
    });

    let _ = tx.send(());
    tracing::info!("exiting ssh monitoring thread");
}

fn parse(line: &str) -> Option<Datarow> {
    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"(?x)
            (?P<datetime>
                [A-Za-z]{3,9}\s\d{1,2}\s\d{2}:\d{2}:\d{2}
            )
            \s\S+\s
            sshd\[(?P<id>\d+)\]:\s
            (
                Disconnected\sfrom\s(authenticating|invalid)\suser\s(?P<username_rejected>\S+)|
                Accepted\spublickey\sfor\s(?P<username_accepted>\S+)\sfrom|
                pam_unix\(sshd:session\):\ssession\sclosed\sfor\suser\s(?P<username_terminated>\S+)|
                fatal:\sTimeout\sbefore\sauthentication\sfor
            )
            \s?
            ((?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\sport\s(?P<port>\d{2,5}))?
            \s?
            (ssh2:\s(?P<key>.+))?
            "
        )
        .expect("assert: datetime regex is properly constructed");
    }
    RE.captures(line).and_then(|cap| {
        let datetime = cap
            .name("datetime")
            .map(|datetime| {
                let captured = datetime.as_str();
                let captured = format!("{} {captured}", Utc::now().format("%Y"));
                NaiveDateTime::parse_from_str(&captured, "%Y %b %d %H:%M:%S")
                    .expect("assert: can parse auth log datetime")
            })
            .expect("assert: can get auth log datetime");
        let id = cap.name("id").and_then(|d| d.as_str().parse().ok())?;
        let ip = cap.name("ip").map(|d| d.as_str());
        let port = cap.name("port").and_then(|d| d.as_str().parse().ok());
        let key = cap.name("key").map(|d| d.as_str());

        let (username, status) = if let Some(username) = cap.name("username_rejected") {
            (Datavalue::Text(username.as_str().to_string()), "rejected")
        } else if let Some(username) = cap.name("username_accepted") {
            (
                Datavalue::Text(username.as_str().to_string()),
                SSH_LOG_STATUS_CONNECTED,
            )
        } else if let Some(username) = cap.name("username_terminated") {
            (Datavalue::Text(username.as_str().to_string()), "terminated")
        } else {
            (Datavalue::NotAvailable, "timeout")
        };

        Some(Datarow::new(
            SSH_LOG.to_string(),
            datetime,
            vec![
                ("id".to_string(), Datavalue::IntegerID(id)),
                ("user".to_string(), username),
                (
                    "ip".to_string(),
                    ip.map(|ip| Datavalue::Text(ip.to_string()))
                        .unwrap_or(Datavalue::NotAvailable),
                ),
                (
                    "port".to_string(),
                    port.map(Datavalue::IntegerID)
                        .unwrap_or(Datavalue::NotAvailable),
                ),
                (
                    SSH_LOG_STATUS.to_string(),
                    Datavalue::Text(status.to_string()),
                ),
                (
                    "pubkey".to_string(),
                    key.map(|key| Datavalue::Text(key.to_string()))
                        .unwrap_or(Datavalue::NotAvailable),
                ),
            ],
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_auth_lines() {
        let line = "May 21 11:22:15 server1 sshd[136055]: Disconnected from authenticating user root 139.59.37.55 port 48966 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(136055));
        assert_eq!(parsed.data[1].1, Datavalue::Text("root".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("139.59.37.55".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(48966));
        assert_eq!(parsed.data[4].1, Datavalue::Text("rejected".to_string()));
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 21 11:22:18 server1 sshd[136059]: Disconnected from invalid user jj 94.127.212.198 port 1122 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(136059));
        assert_eq!(parsed.data[1].1, Datavalue::Text("jj".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("94.127.212.198".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(1122));
        assert_eq!(parsed.data[4].1, Datavalue::Text("rejected".to_string()));
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 21 11:22:18 server1 sshd[136063]: Accepted publickey for ubuntu from 77.222.27.80 port 17827 ssh2: RSA SHA256:D726XJ0DkstyhsyH2rAbfYuIaeBOa3Su2l2WWbyXnXs";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(136063));
        assert_eq!(parsed.data[1].1, Datavalue::Text("ubuntu".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("77.222.27.80".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(17827));
        assert_eq!(parsed.data[4].1, Datavalue::Text("connected".to_string()));
        assert_eq!(
            parsed.data[5].1,
            Datavalue::Text("RSA SHA256:D726XJ0DkstyhsyH2rAbfYuIaeBOa3Su2l2WWbyXnXs".to_string())
        );

        let line = "May 21 10:09:13 los sshd[1511]: Accepted publickey for los from 192.168.64.1 port 63629 ssh2: ED25519 SHA256:tOfMBR3wtNPSvsy8dY6fMSIp+A9RllVkBTK8S+RiSkQ";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(1511));
        assert_eq!(parsed.data[1].1, Datavalue::Text("los".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("192.168.64.1".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(63629));
        assert_eq!(parsed.data[4].1, Datavalue::Text("connected".to_string()));
        assert_eq!(
            parsed.data[5].1,
            Datavalue::Text(
                "ED25519 SHA256:tOfMBR3wtNPSvsy8dY6fMSIp+A9RllVkBTK8S+RiSkQ".to_string()
            )
        );

        let line = "May 21 11:22:56 server1 sshd[136063]: pam_unix(sshd:session): session closed for user ubuntu";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(136063));
        assert_eq!(parsed.data[1].1, Datavalue::Text("ubuntu".to_string()));
        assert_eq!(parsed.data[2].1, Datavalue::NotAvailable);
        assert_eq!(parsed.data[3].1, Datavalue::NotAvailable);
        assert_eq!(parsed.data[4].1, Datavalue::Text("terminated".to_string()));

        let line = "May 21 11:22:59 server1 sshd[135885]: fatal: Timeout before authentication for 116.255.189.120 port 47014";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(135885));
        assert_eq!(parsed.data[1].1, Datavalue::NotAvailable);
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("116.255.189.120".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(47014));
        assert_eq!(parsed.data[4].1, Datavalue::Text("timeout".to_string()));

        let line = "May 21 19:26:17 server1 sshd[59895]: pam_unix(sshd:session): session opened for user los(uid=1000) by (uid=0)";
        assert!(parse(line).is_none());
    }
}
