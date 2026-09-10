# Bản đồ tài liệu Kest

Đọc theo thứ tự dưới đây để tránh nhầm đặc tả cũ với trạng thái đã implement.

## Trạng thái hiện tại

| Tài liệu | Nội dung |
| --- | --- |
| [SOURCE_ONBOARDING_AND_PLATFORM_USAGE.md](SOURCE_ONBOARDING_AND_PLATFORM_USAGE.md) | Platform dev có gì, vai trò từng component và quy trình thêm source mới |
| [NIFI_CDC_AND_TRINO_SQL.md](NIFI_CDC_AND_TRINO_SQL.md) | Vận hành CDC raw/Bronze bằng Debezium–NiFi và truy vấn bằng Trino |
| [CYBERMARKET_END_TO_END.md](CYBERMARKET_END_TO_END.md) | Kiến trúc, business rule, CDC và batch riêng của CyberMarket |
| [DELIVERY_OPS_END_TO_END.md](DELIVERY_OPS_END_TO_END.md) | Source logistics, 24 Airflow task, RisingWave và 35 bảng Iceberg |
| [DEV_TO_PRODUCTION_GUIDE.md](DEV_TO_PRODUCTION_GUIDE.md) | Khác biệt dev/production, target planes, config và lộ trình triển khai |

`README.md` ở root là quick start. `workload/README.md` là runbook ngắn cho
CyberMarket.

## Đặc tả đầu vào

Hai file sau là requirements snapshot. Chúng được giữ để truy vết yêu cầu và
quyết định thiết kế; danh sách giới hạn trong đó chỉ áp dụng cho phase ban đầu:

| Tài liệu | Phạm vi lịch sử |
| --- | --- |
| [DEV_INFRASTRUCTURE_REQUIREMENTS.md](DEV_INFRASTRUCTURE_REQUIREMENTS.md) | Baseline hạ tầng local trước khi thêm workload/platform profiles |
| [CYBERMARKET_SOURCE_REQUIREMENTS.md](CYBERMARKET_SOURCE_REQUIREMENTS.md) | Schema nguồn và yêu cầu Bronze history ban đầu |

Khi mô tả trạng thái hiện tại khác requirements snapshot, Compose, source code và
nhóm tài liệu “Trạng thái hiện tại” là nguồn đúng.

## Ranh giới giữa hai scenario

CyberMarket dùng bucket `mini-cybet`, Lakekeeper warehouse `kest-local` và
PostgreSQL `postgres-source`. DeliveryOps dùng bucket `mini-cybet-delivery`,
warehouse `delivery-ops` và PostgreSQL `postgres-delivery`. Hai scenario dùng
chung các service platform nhưng không dùng chung source data, CDC slot hay
Iceberg namespace.
