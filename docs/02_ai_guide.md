# Hướng dẫn cho AI trong dự án Mini SOC / mini_vector

Tài liệu này mô tả cách AI (LLM) cần hiểu và làm việc với dự án Mini SOC / mini_vector.

Mục tiêu:

- AI hiểu rõ **sự tách biệt** giữa:
  - Lớp **parse log** (ingest / parsing).
  - Lớp **rule detection** (viết rule, bắt alert).
- Khi người dùng hỏi, AI không trộn 2 lớp này vào nhau.
- AI có thể:
  - Gợi ý thiết kế pipeline parse.
  - Gợi ý rule detection dựa trên schema chuẩn.
  - Gợi ý kiến trúc backend cho UI & API parse/rule.

---

## 1. Khung tư duy bắt buộc

Khi xử lý bất kỳ câu hỏi nào về Mini SOC / mini_vector, AI phải luôn tự nhắc:

> “Đây là vấn đề của **parse pipeline** hay **rule detection**?”

- Nếu liên quan đến:
  - Nguồn log (syslog/file/http/tcp).
  - Regex, VRL parse syslog/nginx/windows.
  - Map field vào `@timestamp`, `host`, `program`, `message`, `severity`, `log_type`, `soc_tenant`…
  
  → Đó là **Lớp Parse (A)**.

- Nếu liên quan đến:
  - Rule tấn công (SSH brute force, web 5xx, admin login ngoài giờ, …).
  - Điều kiện `.log_type == "linux_syslog" && .program == "sshd" && contains(.message, "Failed password")`.
  - `rule_id`, `rule_name`, `alert_severity`, `alert`, `alerts index`.

  → Đó là **Lớp Detection (B)**.

AI **không được** trộn logic parse vào rule, hoặc ngược lại.

---

## 2. Cách AI hỗ trợ Lớp A – Parse log

Khi người dùng hỏi về parse log, AI nên:

1. Xác định loại log: syslog, nginx, windows event, JSON của agent, …
2. Đề xuất:
   - Cách dùng `json_parse`, `regex_parse`, hoặc `remap` (VRL).
   - Cách map field vào schema chuẩn (theo `docs/schema.md`).
3. Không đưa ra rule detection tại bước này, chỉ tập trung:
   - “Làm sao từ raw log thành normalized event tốt nhất?”

Khi viết config mẫu, AI phải:

- Đặt các bước parse **trước** `normalize_schema`.
- Đảm bảo output của `normalize_schema` có `log_type` đúng và các field chuẩn.

AI có thể được hỏi để sinh:

- Mẫu config YAML `*_parse.yml`.
- Mẫu regex/nginx, VRL parse syslog/nginx, etc.
- Mẫu test unit cho transform parse.

---

## 3. Cách AI hỗ trợ Lớp B – Rule & Alert

Khi người dùng hỏi về rule/alert, AI phải:

1. Giả định event đã ở dạng **normalized** (đã qua `normalize_schema`).
2. Chỉ sử dụng các field chuẩn (không phụ thuộc vào định dạng raw cũ).
3. Giúp người dùng:
   - Thiết kế rule bằng VRL (trong `rules.yml`).
   - Viết điều kiện theo schema: `log_type`, `severity`, `src_ip`, `dest_ip`, `user`, `action`, `result`, …
   - Thiết kế test case cho rule.

Ví dụ hướng dẫn AI viết rule:

- Người dùng: “Viết rule phát hiện SSH brute force trên linux_syslog.”
- AI phải:
  - Suy nghĩ trên normalized field: `log_type = "linux_syslog"`, `program = "sshd"`, `message` chứa text.
  - Không bàn về regex format syslog thô.
  - Đề xuất rule dạng VRL cho detect transform, ví dụ:
    - Dò nhiều `Failed password` từ cùng một `src_ip` trong 5 phút (trong thực tế cần engine stateful hoặc dùng OpenSearch query).

---

## 4. Cách AI tư vấn kiến trúc UI & API

Khi người dùng nói về “làm giao diện parse log” hoặc “giao diện viết rule”, AI phải:

- Nhớ rằng parse & rule được quản lý bởi **service riêng**:

  - **Parse Service**:
    - Quản lý profile parse (một profile = pipeline parse cho một loại log).
    - Lưu vào DB.
    - Cung cấp API:
      - test parse: raw log → normalized event.
      - publish profile → generate config mini_vector.

  - **Rule Service**:
    - Quản lý rule detection (theo schema chuẩn).
    - Lưu vào DB.
    - Cung cấp API:
      - test rule: normalized event → match / no_match.
      - publish rule set → generate `rules.yml` hoặc API cho mini_vector.

AI có thể được yêu cầu:

- Thiết kế bảng DB (`parse_profiles`, `rules`, …).
- Đề xuất luồng CI/CD cho config (DB → YAML → mini_vector reload).
- Đề xuất cách test tự động (unit test, integration test).

---

## 5. Hạn chế & nguyên tắc

- Không trộn logic parse vào rule.
- Không cho phép rule phụ thuộc vào “cách raw log được viết”, mà luôn dựa trên schema chuẩn.
- Khi sinh code/config:
  - Rõ ràng đâu là phần thuộc Lớp A (parse).
  - Rõ ràng đâu là phần thuộc Lớp B (detect/alert).
- Khi chưa rõ, AI nên tự tách câu trả lời thành:
  - Phần A: đề xuất cho parse.
  - Phần B: đề xuất cho rule.

```markdown ends
