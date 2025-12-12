# Kiến trúc Mini SOC – Tách biệt Parse Log & Rule Alert

Tài liệu này giải thích **logic kiến trúc Mini SOC** theo hướng Vector, với mục tiêu:

- Tách rõ **phần nhận & parse log** (ingest / parsing) khỏi **phần viết rule & bắt alert** (detection / alerting).
- Chuẩn hóa một **schema log chung** cho toàn hệ thống.
- Định hướng để sau này có thể:
  - Thiết kế **giao diện web** cho parse log (lưu cấu hình vào DB, triển khai qua API).
  - Thiết kế **giao diện web** cho viết rule, test rule, quản lý rule version.

---

## 1. Hai lớp logic chính trong Mini SOC

Trong Mini SOC, chúng ta coi toàn bộ pipeline xử lý log được chia thành **hai lớp logic hoàn toàn khác nhau**:

### 1.1. Lớp A – Nhận log & Parse log (Ingest / Parsing)

Nhiệm vụ:

- Nhận log từ các nguồn:
  - `syslog` (UDP/TCP), `file` (tail log), `http`, `tcp` generic, …
- Chuyển log thô (raw log) thành **event đã được chuẩn hóa** theo schema chung.

Đặc trưng:

- Do **kỹ sư nền tảng / log engineer** phụ trách.
- Thay đổi khi:
  - Thêm nguồn log mới.
  - Đổi định dạng log (nginx -> apache, mở rộng format syslog, …).
- Không nên chứa bất kỳ “logic an ninh” nào (rule, điều kiện phát hiện tấn công).

Output của Lớp A:

> **Normalized event** – một bản ghi log đã được chuẩn hóa, có đủ các trường chuẩn như:  
> `@timestamp`, `soc_tenant`, `log_type`, `host`, `program`, `severity`, `message`,  
> cùng các field security như `src_ip`, `dest_ip`, `user`, `action`, `result`, …

### 1.2. Lớp B – Viết rule & bắt alert (Detection / Alerting)

Nhiệm vụ:

- Nhìn vào **normalized event** (đã parse xong) và quyết định:
  - Event có phải là **alert** hay không.
  - Thuộc rule nào (`rule_id`, `rule_name`).
  - Mức độ (`alert_severity`).

Đặc trưng:

- Do **analyst / detection engineer** phụ trách.
- Thay đổi rất thường xuyên:
  - Thêm rule mới, chỉnh rule, tắt rule.
  - Test rule trên log lịch sử, tune điều kiện.
- Không được đụng đến logic parse (regex, VRL parse_syslog, cắt chuỗi,…).

Output của Lớp B:

> **Alert event** – chính event đó, nhưng được gắn thêm các trường:
> `alert`, `rule_id`, `rule_name`, `alert_severity`, `rule_description`, …

---

## 2. Ranh giới kỹ thuật: Normalize Schema

Trong mini_vector, ranh giới giữa hai lớp được quy ước như sau:

- **Trước** `NormalizeSchemaTransform` → thuộc **Lớp A – Parse**.
- **Sau** `NormalizeSchemaTransform` → thuộc **Lớp B – Detection**.

Cụ thể:

- LỚP A gồm:
  - `sources/*` – nhận log từ syslog/file/http/tcp…
  - `add_field` – thêm field như `soc_tenant`, môi trường, …
  - `json_parse`, `regex_parse`, `remap` (VRL) – đọc định dạng thô và biến thành field có cấu trúc.
  - `normalize_schema` – map các field tạm sang schema chuẩn chính thức.

- LÓP B gồm:
  - `detect` – engine đọc `rules.yml` và quyết định event có phải alert không.
  - `route` – tách log thường và alert ra các sink khác nhau.
  - `alerts_sink` – sink lưu alert (OpenSearch index `alerts-*`).
  - `logs_sink` – sink lưu log thường (OpenSearch index `logs-*`).

Ý nghĩa:

- Nếu đổi parser (VD: regex nginx khác) → chỉ sửa **Lớp A**.  
  Rule detection **không cần viết lại**, miễn schema chuẩn giữ nguyên.
- Nếu muốn thêm rule mới → chỉ sửa **Lớp B**, không cần đụng vào ingest.

---

## 3. Ví dụ pipeline tách 2 lớp

Ví dụ pipeline cho `linux_syslog`:

1. **Lớp A – Parse**

   - Source `syslog` (UDP/TCP).
   - Transform:
     - `add_tenant`: thêm `soc_tenant = "acme"`.
     - `parse_syslog` (VRL `parse_syslog!`).
     - `normalize`: `NormalizeSchemaTransform`, set:
       - `log_type = "linux_syslog"`.
       - Map các field `host`, `program`, `message`, `severity`, `@timestamp`.

   Output sau `normalize`: event đã chuẩn hóa.

2. **Lớp B – Detection**

   - Transform `detect`:
     - Đọc rule từ `configs/rules.yml`.
     - Với mỗi event:
       - Evaluate điều kiện (VRL) cho từng rule.
       - Nếu match → set `alert = true`, `rule_id`, `rule_name`, `alert_severity`, …
   - Transform `route_alerts`:
     - Nếu `.alert == true` → gửi sang `alerts_sink`.
     - Ngược lại → gửi sang `logs_sink`.
   - Sinks:
     - `logs_sink`: index `logs-acme-linux_syslog-*`.
     - `alerts_sink`: index `alerts-acme-*`.

---

## 4. Định hướng tương lai: UI & API

Dựa trên việc tách Lớp A và Lớp B:

- **Parse Service (Lớp A)**:
  - Quản lý “parse profile” cho từng `log_type` / `nguồn log`.
  - Lưu cấu hình parse vào DB (regex, VRL, mapping field).
  - Cho phép test parse trên web (raw log → normalized event).

- **Rule Service (Lớp B)**:
  - Quản lý rule detection (id, name, description, severity, condition).
  - Lưu rule vào DB.
  - Cho phép test rule trên web (normalized event → match / no match).
  - Publish rule xuống engine (mini_vector) dưới dạng `rules.yml` hoặc API.

Mini_vector chỉ đóng vai trò **engine runtime**: nhận config (parse + rule), chạy pipeline và xuất log/alert.

```markdown ends
