use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use apw_core::model::Target;
use serde::Serialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

const START_TIMEOUT: Duration = Duration::from_secs(12);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(25);
const NAV_TIMEOUT: Duration = Duration::from_secs(35);
const POLL_MS: u64 = 500;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReservationResult {
    pub ok: bool,
    pub message: String,
    pub url: String,
}

#[derive(Debug)]
struct Browser {
    child: Child,
    _profile: TempDir,
    socket: Socket,
    next_id: u64,
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Browser {
    async fn start() -> Result<Self, String> {
        let chrome = find_chromium().ok_or("未找到 Google Chrome 或 Microsoft Edge")?;
        let profile = tempfile::Builder::new()
            .prefix("apple-store-auto-reservation-")
            .tempdir()
            .map_err(|e| format!("无法创建浏览器配置目录：{e}"))?;
        let profile_arg = format!("--user-data-dir={}", profile.path().display());
        let mut child = Command::new(chrome)
            .args([
                "--remote-debugging-port=0",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-sync",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-popup-blocking",
                profile_arg.as_str(),
                "about:blank",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("无法启动 Chrome/Edge：{e}"))?;

        let port_file = profile.path().join("DevToolsActivePort");
        let deadline = Instant::now() + START_TIMEOUT;
        let port = loop {
            if let Ok(contents) = std::fs::read_to_string(&port_file)
                && let Some(line) = contents.lines().next()
                && let Ok(port) = line.parse::<u16>()
            {
                break port;
            }
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                return Err(format!("浏览器启动后退出：{status}"));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                return Err("等待浏览器启动超时".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let targets_url = format!("http://127.0.0.1:{port}/json/list");
        let targets: Vec<Value> = tokio::time::timeout(Duration::from_secs(10), async {
            reqwest::get(&targets_url)
                .await
                .map_err(|e| format!("连接浏览器调试端口失败：{e}"))?
                .json()
                .await
                .map_err(|e| format!("读取浏览器调试目标失败：{e}"))
        })
        .await
        .map_err(|_| "读取浏览器调试目标超时".to_string())??;

        let ws = targets.into_iter()
            .find(|v| v.get("type").and_then(Value::as_str) == Some("page"))
            .and_then(|v| v.get("webSocketDebuggerUrl").and_then(Value::as_str).map(str::to_owned))
            .ok_or("浏览器没有可用页面目标")?;
        let (socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(ws))
            .await
            .map_err(|_| "连接浏览器页面超时".to_string())?
            .map_err(|e| format!("连接浏览器页面失败：{e}"))?;

        Ok(Self { child, _profile: profile, socket, next_id: 1 })
    }

    async fn command(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1).max(1);
        self.socket.send(Message::Text(json!({"id": id, "method": method, "params": params}).to_string().into()))
            .await
            .map_err(|e| format!("发送 {method} 失败：{e}"))?;
        let fut = async {
            while let Some(msg) = self.socket.next().await {
                let msg = msg.map_err(|e| format!("读取浏览器响应失败：{e}"))?;
                let Message::Text(text) = msg else { continue };
                let value: Value = serde_json::from_str(&text).map_err(|e| format!("解析浏览器响应失败：{e}"))?;
                if value.get("id").and_then(Value::as_u64) != Some(id) { continue; }
                if let Some(error) = value.get("error") { return Err(format!("浏览器命令 {method} 失败：{error}")); }
                return Ok(value.get("result").cloned().unwrap_or(Value::Null));
            }
            Err("浏览器调试连接已关闭".into())
        };
        tokio::time::timeout(COMMAND_TIMEOUT, fut).await.map_err(|_| format!("浏览器命令 {method} 超时"))?
    }

    async fn eval(&mut self, expr: &str, await_promise: bool) -> Result<Value, String> {
        let result = self.command("Runtime.evaluate", json!({"expression": expr, "awaitPromise": await_promise, "returnByValue": true})).await?;
        if result.get("exceptionDetails").is_some() {
            return Err(format!("页面脚本执行失败：{}", result.get("exceptionDetails").unwrap_or(&Value::Null)));
        }
        Ok(result.pointer("/result/value").cloned().unwrap_or(Value::Null))
    }

    async fn navigate(&mut self, url: &str) -> Result<(), String> {
        self.command("Page.enable", json!({})).await?;
        self.command("Page.navigate", json!({"url": url})).await?;
        let deadline = Instant::now() + NAV_TIMEOUT;
        loop {
            let state = self.eval("document.readyState", false).await?.as_str().unwrap_or("").to_string();
            if state == "interactive" || state == "complete" { return Ok(()); }
            if Instant::now() >= deadline { return Err("等待 Apple 页面加载超时".into()); }
            tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
        }
    }

    async fn click_text(&mut self, patterns: &[&str]) -> Result<bool, String> {
        let pats = serde_json::to_string(patterns).map_err(|e| e.to_string())?;
        let expr = format!(r#"(() => {{
          const pats = {pats}.map(x => x.toLowerCase());
          const norm = el => (el.innerText || el.textContent || el.getAttribute('aria-label') || el.value || '').replace(/\\s+/g,' ').trim().toLowerCase();
          const nodes = [...document.querySelectorAll('button,a,[role="button"],input[type="button"],input[type="submit"]')];
          const hit = nodes.find(el => {{ const t = norm(el); return t && pats.some(p => t === p || t.includes(p)); }});
          if (!hit) return false;
          hit.scrollIntoView({{block:'center',inline:'center'}}); hit.click(); return true;
        }})()"#);
        Ok(self.eval(&expr, false).await?.as_bool().unwrap_or(false))
    }

    async fn fill_field(&mut self, selector_script: &str, value: &str) -> Result<bool, String> {
        let val = serde_json::to_string(value).map_err(|e| e.to_string())?;
        let expr = format!(r#"(() => {{
          const value = {val};
          const input = ({selector_script});
          if (!input) return false;
          input.focus();
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value')?.set;
          if (setter) setter.call(input,value); else input.value = value;
          input.dispatchEvent(new Event('input',{{bubbles:true}}));
          input.dispatchEvent(new Event('change',{{bubbles:true}}));
          return true;
        }})()"#);
        Ok(self.eval(&expr, false).await?.as_bool().unwrap_or(false))
    }

    async fn fill_identity(&mut self, first: &str, last: &str, email: &str, phone: &str) -> Result<Vec<String>, String> {
        let mut filled = Vec::new();
        let fields = [
            ("first name", first, "[...document.querySelectorAll('input')].find(i=>/first.?name|given.?name|名/i.test((i.name||'')+' '+(i.id||'')+' '+(i.placeholder||'')+' '+(i.getAttribute('aria-label')||'')))"),
            ("last name", last, "[...document.querySelectorAll('input')].find(i=>/last.?name|family.?name|surname|姓/i.test((i.name||'')+' '+(i.id||'')+' '+(i.placeholder||'')+' '+(i.getAttribute('aria-label')||'')))"),
            ("email", email, "[...document.querySelectorAll('input')].find(i=>i.type==='email'||/email|电子邮件/i.test((i.name||'')+' '+(i.id||'')+' '+(i.placeholder||'')+' '+(i.getAttribute('aria-label')||'')))"),
            ("phone", phone, "[...document.querySelectorAll('input')].find(i=>i.type==='tel'||/phone|mobile|telephone|手机号|电话/i.test((i.name||'')+' '+(i.id||'')+' '+(i.placeholder||'')+' '+(i.getAttribute('aria-label')||'')))"),
        ];
        for (label, value, selector) in fields {
            if value.trim().is_empty() { continue; }
            if self.fill_field(selector, value).await? { filled.push(label.to_string()); }
        }
        Ok(filled)
    }
}

fn find_chromium() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let roots = ["PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"];
        let candidates = [
            "Google/Chrome/Application/chrome.exe",
            "Microsoft/Edge/Application/msedge.exe",
        ];
        for root in roots {
            if let Some(base) = std::env::var_os(root) {
                for rel in candidates {
                    let path = PathBuf::from(&base).join(rel);
                    if path.is_file() { return Some(path); }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        for path in ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"] {
            let p = PathBuf::from(path); if p.is_file() { return Some(p); }
        }
    }
    #[cfg(target_os = "linux")]
    {
        for name in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge"] {
            if let Some(path) = std::env::var_os("PATH").and_then(|p| std::env::split_paths(&p).map(|d| d.join(name)).find(|p| p.is_file())) { return Some(path); }
        }
    }
    None
}

pub async fn run(target: &Target, product_url: &str, first: &str, last: &str, phone: &str, email: &str) -> ReservationResult {
    let mut browser = match Browser::start().await {
        Ok(b) => b,
        Err(e) => return ReservationResult { ok: false, message: e, url: String::new() },
    };
    let product_url = product_url.to_string();

    if let Err(e) = browser.navigate(&product_url).await {
        return ReservationResult { ok: false, message: e, url: product_url };
    }

    for _ in 0..8 {
        let _ = browser.click_text(&["buy", "购买", "buy now", "立即购买", "select", "选择"]).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    for _ in 0..8 {
        let _ = browser.click_text(&["pick up", "取货", "check availability", "查看可取货门店", "store pickup", "门店取货"]).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    for _ in 0..5 {
        let _ = browser.click_text(&["continue", "继续", "proceed", "继续结账", "checkout", "结账"]).await;
        tokio::time::sleep(Duration::from_millis(700)).await;
    }

    let filled = match browser.fill_identity(first, last, email, phone).await {
        Ok(v) => v,
        Err(e) => return ReservationResult { ok:false, message:e, url:product_url },
    };

    let store_hint = target.store_title.to_lowercase();
    let _ = browser.eval(&format!(r#"(() => {{
        const wanted = {:?};
        const all = [...document.querySelectorAll('label,button,[role="option"],[role="radio"],option')];
        const node = all.find(e => ((e.innerText||e.textContent||'').toLowerCase().includes(wanted)));
        if (node) {{ node.click(); return true; }} return false;
    }})()"#, store_hint), false).await;

    // 安全边界：自动化只负责打开 Apple 商品流程和填写预设资料，不执行最终提交/下单。
    std::mem::forget(browser);
    ReservationResult {
        ok:true,
        message:format!("已打开 Apple 商品流程并填写：{}；浏览器已保留，请人工核对后完成后续操作", if filled.is_empty(){"无匹配字段".into()} else {filled.join("、")}),
        url:product_url,
    }
}
