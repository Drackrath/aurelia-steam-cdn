// Modified by Aurelia; see README.
use reqwest::Client;

use crate::Error;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ContentServer {
    pub r#type: String,
    pub source_id: u64,
    pub cell_id: Option<u32>,
    pub load: u32,
    pub weighted_load: u32,
    pub num_entries_in_client_list: u32,
    pub host: String,
    pub vhost: String,
    pub https_support: String,
    pub priority_class: u32,
}

#[derive(Debug, Deserialize)]
struct ContentServerDirectoryInner {
    pub servers: Vec<ContentServer>,
}

#[derive(Debug, Deserialize)]
struct ContentServerDirectory {
    pub response: ContentServerDirectoryInner,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CDNServer {
    pub r#type: String,
    pub https: bool,
    pub host: String,
    pub vhost: String,
    pub port: u16,
    pub cell_id: u32,
    pub load: u32,
    pub weighted_load: u32,
    pub auth_token: Option<String>,
}

pub async fn get_servers_for_steam_pipe(cell_id: u32) -> Result<Vec<CDNServer>, Error> {
    Ok(prefer_https(
        Client::new()
            .get("https://api.steampowered.com/IContentServerDirectoryService/GetServersForSteamPipe/v1/")
            .query(&[("cell_id", cell_id)])
            .send().await?
            .json::<ContentServerDirectory>().await?
            .response
            .servers
            .into_iter()
            .map(|server| {
                let https = supports_https(&server.https_support);
                CDNServer {
                    r#type: server.r#type,
                    https,
                    host: server.host,
                    vhost: server.vhost,
                    port: if https { 443 } else { 80 },
                    cell_id: server.cell_id.unwrap_or(0),
                    load: server.load,
                    weighted_load: server.weighted_load,
                    auth_token: None,
                }
            })
            .collect::<Vec<CDNServer>>(),
    ))
}

/// Whether `https_support` allows TLS.
pub fn supports_https(https_support: &str) -> bool {
    matches!(https_support, "mandatory" | "optional")
}

/// Keep HTTPS servers when any exist.
pub fn prefer_https(servers: Vec<CDNServer>) -> Vec<CDNServer> {
    if servers.iter().any(|s| s.https) {
        servers.into_iter().filter(|s| s.https).collect()
    } else {
        if !servers.is_empty() {
            tracing::warn!("No HTTPS content servers; using plain HTTP");
        }
        servers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(host: &str, https: bool) -> CDNServer {
        CDNServer {
            r#type: "CDN".into(),
            https,
            host: host.into(),
            vhost: host.into(),
            port: if https { 443 } else { 80 },
            cell_id: 0,
            load: 0,
            weighted_load: 0,
            auth_token: None,
        }
    }

    #[test]
    fn optional_and_mandatory_mean_https() {
        assert!(supports_https("mandatory"));
        assert!(supports_https("optional"));
        assert!(!supports_https("none"));
        assert!(!supports_https(""));
    }

    #[test]
    fn https_servers_win_when_present() {
        let kept = prefer_https(vec![server("a", false), server("b", true)]);
        assert_eq!(kept.iter().map(|s| s.host.as_str()).collect::<Vec<_>>(), ["b"]);
    }

    #[test]
    fn plain_http_only_when_nothing_else() {
        assert_eq!(prefer_https(vec![server("a", false)]).len(), 1);
    }
}
