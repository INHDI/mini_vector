## Benchmark & sizing (hướng dẫn nhanh)

Script: `scripts/bench_http.py`
- Gửi N sự kiện JSON vào HTTP source song song.
- Ví dụ: `python scripts/bench_http.py --url http://127.0.0.1:9000/ingest --count 20000 --concurrency 8`

Kịch bản baseline
- Pipeline: HTTP source -> normalize_schema -> console sink (hoặc blackhole).
- Batch: mặc định (1) hoặc nâng lên 256 với sink có batch.
- Đo `events/sec`, quan sát `/metrics` (`events_out`).

Gợi ý sizing (ước lượng)
- Máy 1 vCPU / 1–2 GB RAM, pipeline đơn giản console có thể đạt vài chục nghìn events/sec; VRL/regex nặng hơn sẽ giảm throughput.
- Tăng batch cho HTTP/OpenSearch để giảm overhead; theo dõi `batches_sent`, `events_failed`.

Cần ghi lại
- Events/sec theo concurrency và batch size.
- CPU, bộ nhớ tại mức tải mục tiêu.
- Counter lỗi/rớt (buffer_full, http_error, mapping_error).
