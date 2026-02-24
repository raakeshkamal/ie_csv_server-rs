use regex::Regex;
use once_cell::sync::Lazy;

static SECURITY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(.*?)\s*/\s*ISIN\s+([A-Z]{2}[A-Z0-9]{9}[0-9])").unwrap()
});

pub fn extract_security_and_isin(text: &str) -> (String, Option<String>) {
    if let Some(caps) = SECURITY_RE.captures(text) {
        let name = caps.get(1).map_or("", |m| m.as_str()).trim().to_string();
        let isin = caps.get(2).map(|m| m.as_str().to_string());
        (name, isin)
    } else {
        (text.trim().to_string(), None)
    }
}
