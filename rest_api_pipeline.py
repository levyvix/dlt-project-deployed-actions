import os
import dlt
from dlt.sources.rest_api import rest_api_source

os.environ["EXTRACT__WORKERS"] = "10"                 # thread pool size for extract (default 5)
os.environ["EXTRACT__MAX_PARALLEL_ITEMS"] = "50"      # how many callables queued+running in extract
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000" # buffer size for intermediary files (bigger -> fewer writes)
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = str(50_000)   # rotate after this many items per file
os.environ["DATA_WRITER__FILE_MAX_BYTES"] = str(2_000_000) # rotate after ~2MB compressed (adjust)
os.environ["NORMALIZE__WORKERS"] = "6"
os.environ["NORMALIZE__START_METHOD"] = "spawn"
os.environ["LOAD__WORKERS"] = "20"
os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "true"

jaffle_source = rest_api_source({
    'client': {
        'base_url':'https://jaffle-shop.scalevector.ai/api/v1',
        'paginator':{
            'type':'page_number',
            'base_page':1,
            'total_path':None,
            'maximum_page':20
        },
    },
    "resource_defaults": {
        "primary_key": "id",
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "per_page": 500,
            },
        },
    },
    'resources': [
        {
            'name':'customers',
            'endpoint':'customers',
        },
        {
            'name':'orders',
            'endpoint':'orders',
        },
    ],
}, parallelized=True)

# ---------- Pipeline ----------
pipe = dlt.pipeline(
    pipeline_name = 'test_rest_optimized',
    destination='duckdb',
    dataset_name='rest_test_optimized',
)

# run
if __name__ == "__main__":
    info = pipe.run(jaffle_source)
    print("LOAD INFO:", info)
