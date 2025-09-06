import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR / "Extractor"))
sys.path.append(str(BASE_DIR / "Loader"))
sys.path.append(str(BASE_DIR / "Archive"))


from prefect import flow, task

from Extractor.main_extractor import MainExtractor
from Loader.products import load_products
from Loader.sales import load_sales
from Loader.users import load_users
from Extractor.archive import archive_data

@task(retries=3, retry_delay_seconds=60)
def extract_task():
    extractor = MainExtractor()
    extractor.extract_all()

@task(retries=2, retry_delay_seconds=30)
def load_products_task():
    load_products()

@task(retries=2, retry_delay_seconds=30)
def load_users_task():
    load_users()

@task(retries=2, retry_delay_seconds=30)
def load_sales_task():
    load_sales()

@task(retries=2, retry_delay_seconds=30)
def archive_task():
    archive_data()

@flow(name="FDE Data Pipeline")
def fde_pipeline():
    extract_task()
    products_future = load_products_task.submit()
    users_future = load_users_task.submit()
    products_future.result()
    users_future.result()
    load_sales_task()
    archive_task()

if __name__ == "__main__":
    fde_pipeline()
