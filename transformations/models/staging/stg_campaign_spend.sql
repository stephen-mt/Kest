select * except (revision)
from (
  select *, row_number() over (partition by spend_id order by source_updated_at desc, batch_id desc, record_hash desc) revision
  from {{ source('raw', 'campaign_spend') }}
)
where revision = 1 and not is_deleted
