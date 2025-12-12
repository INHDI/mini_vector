## Triển khai mini_vector

### Build & chạy local
- Build: `cargo build --release`
- Chạy: `./target/release/mini_vector ./mini_vector.yml`
- Metrics/health: `http://localhost:${METRICS_PORT:-9100}/metrics` và `/health`.

### Docker
- Build: `docker build -t mini_vector .`
- Chạy (mount config): `docker run --rm -p 9000:9000 -p 9100:9100 -v $(pwd)/mini_vector.yml:/app/mini_vector.yml mini_vector /app/mini_vector.yml`
- Endpoint:
  - Ingest theo config (vd. HTTP source 9000)
  - `/metrics`, `/health` trên `METRICS_PORT` (mặc định 9100)

### Biến môi trường
- `METRICS_PORT`: cổng metrics/health (mặc định 9100).
- `RELOAD_PORT`: cổng HTTP `/reload` (mặc định 9400).
- OpenSearch sink cấu hình trong YAML (endpoints/auth/tls). Nếu dùng secret, truyền qua env rồi tham chiếu trong YAML.

### Health
- `/health` trả `UP` nếu đã load config và OpenSearch có lần thành công trong 60s gần nhất; ngược lại `DOWN`.
- `/metrics` cung cấp counter events, batch, lỗi/rớt.

### Ghi chú
- Mount YAML từ host hoặc inject qua ConfigMap/secret (k8s, v.v.).
- Đảm bảo đồng bộ thời gian và kết nối tới OpenSearch.
