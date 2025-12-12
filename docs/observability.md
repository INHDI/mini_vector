## Quan sát (Observability)

### Metrics chính
- `events_in`, `events_out`, `events_dropped`, `events_failed` kèm label `component`, và `reason` nếu có.
- Batch (ở sink dùng batcher): `batches_sent`, `batches_failed`, `batch_size_event`, `batch_size_bytes`.
- Rớt do đầy buffer: `events_dropped{reason="buffer_full"}` tại fan-out khi `when_full=drop_new`.
- Nguồn: `events_out`, `events_dropped{reason="read_error"}`. Sink: `events_failed` cho lỗi không recover được.

### Endpoint
- `/metrics` (Prometheus), cổng `METRICS_PORT` (mặc định 9100).
- `/health`: `UP` nếu đã load config và OpenSearch có lần thành công trong 60s gần nhất; `DOWN` nếu không.
