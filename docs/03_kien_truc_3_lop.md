# Kiến trúc 3 lớp: mini_vector, Parse Service, Rule Service

Mục đích:

- Minh họa cách mini_vector (engine) tương tác với:
  - Parse Service (quản lý cấu hình parse).
  - Rule Service (quản lý rule detect).
- Chuẩn hóa mô hình cho việc phát triển UI + backend về sau.

---

## 1. Sơ đồ component tổng thể

```mermaid
flowchart LR
    subgraph IngestLayer["LỚP A - INGEST & PARSE (mini_vector)"]
        SourceSyslog["Syslog Source\n(UDP/TCP)"]
        SourceFile["File Source\n(tail log)"]
        SourceHTTP["HTTP Source"]
        ParsePipeline["Parse Pipeline\n(json_parse / regex_parse / remap)"]
        Normalize["NormalizeSchemaTransform\n(schema chuẩn)"]
    end

    subgraph DetectLayer["LỚP B - DETECTION (mini_vector)"]
        Detect["DetectTransform\n(load rules.yml)"]
        RouteAlerts["RouteTransform\n(alert -> alerts_sink,\nlog thường -> logs_sink)"]
        LogsSink["logs_sink\n(OpenSearch logs-*)"]
        AlertsSink["alerts_sink\n(OpenSearch alerts-*)"]
    end

    subgraph ParseService["Parse Service\n(Backend + DB + UI)"]
        ParseUI["Parse UI\n(web)"]
        ParseAPI["Parse API\n(REST/gRPC)"]
        ParseDB["Parse Profiles DB"]
    end

    subgraph RuleService["Rule Service\n(Backend + DB + UI)"]
        RuleUI["Rule UI\n(web)"]
        RuleAPI["Rule API\n(REST/gRPC)"]
        RuleDB["Rules DB"]
    end

    %% Dòng log
    SourceSyslog --> ParsePipeline
    SourceFile --> ParsePipeline
    SourceHTTP --> ParsePipeline
    ParsePipeline --> Normalize --> Detect --> RouteAlerts
    RouteAlerts --> LogsSink
    RouteAlerts --> AlertsSink

    %% Cấu hình từ Parse Service
    ParseUI <---> ParseAPI
    ParseAPI <---> ParseDB
    ParseAPI -->|Generate config YAML/JSON| IngestLayer

    %% Cấu hình từ Rule Service
    RuleUI <---> RuleAPI
    RuleAPI <---> RuleDB
    RuleAPI -->|Generate rules.yml / API| DetectLayer

    %% Monitoring / Observability (tuỳ chọn)
    LogsSink -->|Analyst & Dashboards| RuleUI
    AlertsSink -->|Alert View| RuleUI
```