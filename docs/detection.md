## Pipeline detection (dựa trên VRL)

### Định dạng rule (`configs/rules.yml`)
Mỗi rule:
```yaml
- id: R001
  name: SSH brute force
  description: Nhiều đăng nhập SSH thất bại
  severity: high
  condition: '.log_type == "linux_syslog" && contains(.message, "Failed password")'
```
- `condition`: biểu thức VRL trả về boolean.
- Bắt buộc: `id`, `name`, `condition`. Tuỳ chọn: `description`, `severity` (mặc định `medium`).

### Cấu hình transform
```yaml
transforms:
  detect:
    type: detect
    rules_path: configs/rules.yml
    alert_outputs: ["alerts"]   # gửi bản sao alert tới sink/transform tên "alerts"
    inputs: ["normalize"]
```

### Trường alert khi match
- `alert=true`
- `rule_id`, `rule_name`
- `alert_severity`
- `rule_description` (nếu có)

### Điều phối alert
- Nối output `detect` tới sink alert qua `inputs`:
```yaml
sinks:
  alerts:
    type: opensearch
    inputs: ["detect"]
    endpoints: ["https://opensearch:9200"]
    bulk:
      index: "alerts-%Y.%m.%d"   # hoặc "alerts-{tenant}-%Y.%m.%d" nếu đa tenant
```
- Webhook HTTP: dùng sink `http` với endpoint riêng, `inputs: ["detect"]`.

### Reload
- Rule nạp lúc khởi động và reload cùng config (SIGHUP / `/reload`).

### Metrics
- `events_in/out` theo component.
- `alerts_fired{rule_id=...}` tăng mỗi lần match rule.
- `events_failed{reason="rule_error"}` khi VRL lỗi.
