use crate::google::datavalue::{Datarow, Datavalue};
use crate::notifications::{Notification, Sender};
use crate::services::{Data, TaskResult};
use chrono::{NaiveDateTime, Utc};
use lazy_static::lazy_static;
use logwatcher::{LogWatcher, LogWatcherAction, LogWatcherEvent};
use regex::Regex;
use std::collections::HashMap;
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
pub const SSH_LOG_STATUS_TERMINATED: &str = "terminated";

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

    let mut connections = HashMap::new();

    log_watcher.watch(&mut move |result| {
        let result = match result {
            Ok(event) => match event {
                LogWatcherEvent::Line(line) => match parse(&line) {
                    Some(mut datarow) => {
                        lookup_connection(&mut datarow, &mut connections);
                        let Datavalue::Text(ref status) = datarow.data[4].1 else {
                            panic!("assert: ssh status is parsed")
                        };
                        if status == SSH_LOG_STATUS_CONNECTED && connections.len() > 100 {
                            let message =
                                format!("there are {} active ssh connections", connections.len());
                            tracing::warn!("{}", message);
                            messenger.send_nonblock(Notification::new(message, Level::WARN));
                        }
                        Ok(Data::Single(datarow))
                    }
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

struct Connection {
    user_ip: String,
    user_port: u32,
    user_key: String,
}

fn lookup_connection(datarow: &mut Datarow, connections: &mut HashMap<u32, Connection>) {
    let Datavalue::Text(ref status) = datarow.data[4].1 else {
        panic!("assert: ssh status is parsed")
    };
    if status != SSH_LOG_STATUS_CONNECTED && status != SSH_LOG_STATUS_TERMINATED {
        return;
    }
    let Datavalue::IntegerID(id) = datarow.data[0].1 else {
        panic!("assert: ssh id is parsed")
    };

    // For terminated
    if let Some(Connection {
        user_ip,
        user_port,
        user_key,
    }) = connections.remove(&id)
    {
        datarow.data[2].1 = Datavalue::Text(user_ip);
        datarow.data[3].1 = Datavalue::IntegerID(user_port);
        datarow.data[5].1 = Datavalue::Text(user_key);
        return;
    }

    if status == SSH_LOG_STATUS_TERMINATED {
        return;
    }

    // For connected
    let Datavalue::Text(ref ip) = datarow.data[2].1 else {
        panic!("assert: connected ssh user has an ip")
    };
    let Datavalue::IntegerID(port) = datarow.data[3].1 else {
        panic!("assert: connected ssh user has a port")
    };
    let Datavalue::Text(ref key) = datarow.data[5].1 else {
        panic!("assert: connected ssh user has a pubkey")
    };

    connections.insert(
        id,
        Connection {
            user_ip: ip.to_string(),
            user_port: port,
            user_key: key.to_string(),
        },
    );
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
                (Disconnected\sfrom|Disconnecting|Connection\sclosed\sby)\sauthenticating\suser\s(?P<username_rejected>\S+)|
                (Disconnected\sfrom|Disconnecting|Connection\sclosed\sby)\sinvalid\suser\s(?P<username_invalid>\S+)|
                Accepted\spublickey\sfor\s(?P<username_accepted>\S+)\sfrom|
                pam_unix\(sshd:session\):\ssession\sclosed\sfor\suser\s(?P<username_terminated>\S+)|
                (?P<other_reason>fatal:\sTimeout\sbefore\sauthentication\sfor|Unable\sto\snegotiate\swith|Connection\sclosed\sby|Connection\sreset\sby|banner\sexchange:\sConnection\sfrom)
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
        let ip = cap.name("ip").map(|d| d.as_str().to_string());
        let port = cap.name("port").and_then(|d| d.as_str().parse().ok());
        let key = cap.name("key").map(|d| d.as_str().to_string());

        let (username, status) = if let Some(username) = cap.name("username_rejected") {
            (Datavalue::Text(username.as_str().to_string()), "rejected")
        } else if let Some(username) = cap.name("username_invalid") {
            (
                Datavalue::Text(username.as_str().to_string()),
                "invalid_user",
            )
        } else if let Some(username) = cap.name("username_accepted") {
            (
                Datavalue::Text(username.as_str().to_string()),
                SSH_LOG_STATUS_CONNECTED,
            )
        } else if let Some(username) = cap.name("username_terminated") {
            (
                Datavalue::Text(username.as_str().to_string()),
                SSH_LOG_STATUS_TERMINATED,
            )
        } else if let Some(other_reason) = cap.name("other_reason") {
            let other_reason = other_reason.as_str().to_lowercase();
            let reason = if other_reason.contains("timeout") {
                "timeout"
            } else if other_reason.contains("reset") || other_reason.contains("closed") {
                "rejected"
            } else if other_reason.contains("negotiate") || other_reason.contains("banner") {
                "wrong_params"
            } else {
                "rejected"
            };

            (Datavalue::NotAvailable, reason)
        } else {
            (Datavalue::NotAvailable, "rejected")
        };

        Some(Datarow::new(
            SSH_LOG.to_string(),
            datetime,
            vec![
                ("id".to_string(), Datavalue::IntegerID(id)),
                ("user".to_string(), username),
                (
                    "ip".to_string(),
                    ip.map(Datavalue::Text).unwrap_or(Datavalue::NotAvailable),
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
                    key.map(Datavalue::Text).unwrap_or(Datavalue::NotAvailable),
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

        let line = "May 25 19:08:00 household sshd[159380]: Disconnecting authenticating user ubuntu 122.224.37.86 port 53474: Too many authentication failures [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(159380));
        assert_eq!(parsed.data[1].1, Datavalue::Text("ubuntu".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("122.224.37.86".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(53474));
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
        assert_eq!(
            parsed.data[4].1,
            Datavalue::Text("invalid_user".to_string())
        );
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

        let line = "May 25 17:44:08 household sshd[159150]: Connection closed by invalid user user 111.70.3.198 port 52445 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(159150));
        assert_eq!(parsed.data[1].1, Datavalue::Text("user".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("111.70.3.198".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(52445));
        assert_eq!(
            parsed.data[4].1,
            Datavalue::Text("invalid_user".to_string())
        );
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 25 17:43:57 household sshd[159147]: Connection closed by authenticating user nobody 213.230.65.20 port 47128 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(159147));
        assert_eq!(parsed.data[1].1, Datavalue::Text("nobody".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("213.230.65.20".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(47128));
        assert_eq!(parsed.data[4].1, Datavalue::Text("rejected".to_string()));
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line =
            "May 25 17:44:00 household sshd[159149]: Connection closed by 1.62.154.219 port 62293";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(159149));
        assert_eq!(parsed.data[1].1, Datavalue::NotAvailable);
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("1.62.154.219".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(62293));
        assert_eq!(parsed.data[4].1, Datavalue::Text("rejected".to_string()));
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 25 16:03:47 household sshd[158910]: Connection reset by 104.248.136.93 port 6116 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(158910));
        assert_eq!(parsed.data[1].1, Datavalue::NotAvailable);
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("104.248.136.93".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(6116));
        assert_eq!(parsed.data[4].1, Datavalue::Text("rejected".to_string()));
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 21 19:26:17 server1 sshd[59895]: pam_unix(sshd:session): session opened for user los(uid=1000) by (uid=0)";
        assert!(parse(line).is_none());

        let line = "May 25 17:45:41 household sshd[159154]: Unable to negotiate with 185.196.8.151 port 34228: no matching key exchange method found. Their offer: diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group1-sha1 [preauth]";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(159154));
        assert_eq!(parsed.data[1].1, Datavalue::NotAvailable);
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("185.196.8.151".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(34228));
        assert_eq!(
            parsed.data[4].1,
            Datavalue::Text("wrong_params".to_string())
        );
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);

        let line = "May 25 15:33:34 household sshd[158862]: banner exchange: Connection from 162.243.135.24 port 53198: invalid format";
        let parsed = parse(line).unwrap();
        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(158862));
        assert_eq!(parsed.data[1].1, Datavalue::NotAvailable);
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("162.243.135.24".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(53198));
        assert_eq!(
            parsed.data[4].1,
            Datavalue::Text("wrong_params".to_string())
        );
        assert_eq!(parsed.data[5].1, Datavalue::NotAvailable);
    }

    #[test]
    fn lookup_connections() {
        let mut connections = HashMap::new();

        let line = "May 21 11:22:18 server1 sshd[136063]: Accepted publickey for ubuntu from 77.222.27.80 port 17827 ssh2: RSA SHA256:D726XJ0DkstyhsyH2rAbfYuIaeBOa3Su2l2WWbyXnXs";
        let mut parsed = parse(line).unwrap();
        lookup_connection(&mut parsed, &mut connections);
        assert_eq!(connections.len(), 1);

        let line = "May 21 11:22:56 server1 sshd[136063]: pam_unix(sshd:session): session closed for user ubuntu";
        let mut parsed = parse(line).unwrap();
        lookup_connection(&mut parsed, &mut connections);
        assert_eq!(connections.len(), 0);

        assert_eq!(parsed.data[0].1, Datavalue::IntegerID(136063));
        assert_eq!(parsed.data[1].1, Datavalue::Text("ubuntu".to_string()));
        assert_eq!(
            parsed.data[2].1,
            Datavalue::Text("77.222.27.80".to_string())
        );
        assert_eq!(parsed.data[3].1, Datavalue::IntegerID(17827));
        assert_eq!(parsed.data[4].1, Datavalue::Text("terminated".to_string()));
        assert_eq!(
            parsed.data[5].1,
            Datavalue::Text("RSA SHA256:D726XJ0DkstyhsyH2rAbfYuIaeBOa3Su2l2WWbyXnXs".to_string())
        );
    }
}
