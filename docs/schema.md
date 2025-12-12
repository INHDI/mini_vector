# Schema Mini SOC (giản lược ECS)

Schema chuẩn cho mọi event trong mini_vector. Transform cần chuẩn hoá về các key dưới; nếu nguồn không có thì để trống/null (không bịa giá trị trừ khi mặc định đã nêu).

## Trường lõi
- `@timestamp`: thời gian RFC3339 UTC; thiếu thì đặt `now()`.
- `soc_tenant`: **bắt buộc** mã tenant.
- `log_type`: loại log (vd. `linux_syslog`, `web_access`, `windows_event`).
- `host`: hostname phát sinh log.
- `program`: tên tiến trình/ứng dụng.
- `severity`: mức đã chuẩn hoá (`EMERG|ALERT|CRIT|ERROR|WARN|INFO|DEBUG`).
- `message`: thông điệp chính.
- `raw_log`: bản gốc (nếu có) để truy vết.

## Trường bảo mật/chung
- `src_ip`, `src_port`
- `dest_ip`, `dest_port`
- `user`: người/phiên đăng nhập.
- `action`: hành động (vd. `login`, `http_request`).
- `result`: kết quả (`success/failure`, mã HTTP…).
- `rule_id`, `rule_name`: thông tin rule/detection.

## Quy tắc chuẩn hoá
- Có sẵn thì map sang key chuẩn; không có thì giữ trống/null.
- `message` giữ nguyên; `severity` chuẩn về bộ giá trị trên.
- Luôn đảm bảo `soc_tenant` (inject qua config/pipeline nếu thiếu).
- Giữ `raw_log` khi parser cung cấp (syslog line, HTTP body gốc).

## Quy ước index (multi-tenant)
Mặc định: `logs-{tenant}-{log_type}-YYYY.MM.DD`
- Ví dụ: `logs-acme-linux_syslog-2025.03.01`
- Phương án khác: index theo log_type, `soc_tenant` là field phục vụ phân quyền/truy vấn.

## Hướng dẫn multi-tenant
- `soc_tenant` bắt buộc trên mọi event. Nếu upstream không gửi, thêm qua `add_field` hoặc `normalize_schema.default_tenant`.
- HTTP source: có thể copy header vào `soc_tenant` bằng remap/script/VRL.
- Index mặc định: `logs-{tenant}-{log_type}-YYYY.MM.DD`; nếu dùng index chung, vẫn giữ `soc_tenant` để lọc/ủy quyền.

## Ví dụ event chuẩn
```json
{
  "@timestamp": "2025-03-01T10:00:00Z",
  "soc_tenant": "acme",
  "log_type": "web_access",
  "host": "edge-1",
  "program": "nginx",
  "severity": "INFO",
  "message": "GET /index.html 200",
  "src_ip": "203.0.113.10",
  "dest_ip": "10.0.0.5",
  "dest_port": 443,
  "action": "GET",
  "result": "200",
  "raw_log": "203.0.113.10 - - [01/Mar/2025:10:00:00 +0000] \"GET /index.html HTTP/1.1\" 200 512 \"-\" \"curl/8.0\""
}
```
