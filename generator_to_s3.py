# src/generator_to_s3.py
import io
import csv
import time
import random
import argparse
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Iterable, Optional
import os
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# ---- reference lists ----
CURRENCIES = ["USD"]  # keep USD only so AMOUNT_USD stays consistent
REGIONS    = ["US-EAST", "US-WEST", "US-CENTRAL"]
MERCHANTS  = ["AlphaShop", "BetaMart", "GammaFoods", "DeltaRide", "ElectroMart"]
PAYMENTS   = ["CARD", "WALLET", "BANK_TRANSFER", "UPI"]
STATUSES   = ["APPROVED", "DECLINED", "REFUNDED"]

CSV_HEADERS_11 = [
    "TXN_ID",
    "TXN_TS_UTC",
    "REGION",
    "MERCHANT",
    "CUSTOMER_ID",
    "PAYMENT_METHOD",
    "CURRENCY",
    "AMOUNT_USD",
    "STATUS",
    "IS_REFUND",
    "INGESTED_AT",
]


def make_row() -> List[str]:
    """Create one transaction row (11 cols) as strings."""
    txn_id = str(uuid4())
    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # Snowflake-friendly
    region = random.choice(REGIONS)
    merchant = random.choice(MERCHANTS)
    customer_id = f"CUST-{random.randint(100000, 999999)}"
    pay_method = random.choice(PAYMENTS)
    currency = "USD"

    # status + refund logic
    amount = round(random.uniform(1.00, 250.00), 2)
    r = random.random()
    if r < 0.85:
        status = "APPROVED"
        is_refund = "false"
    elif r < 0.95:
        status = "DECLINED"
        is_refund = "false"
        amount = 0.00
    else:
        status = "REFUNDED"
        is_refund = "true"
        # if you want refunds negative, uncomment:
        # amount = -amount

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return [
        txn_id,
        ts_utc,
        region,
        merchant,
        customer_id,
        pay_method,
        currency,
        f"{amount:.2f}",
        status,
        is_refund,
        ingested_at,
    ]


def build_csv_bytes(batch_size: int) -> bytes:
    """Return a CSV (with header) containing `batch_size` rows."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(CSV_HEADERS_11)
    for _ in range(batch_size):
        w.writerow(make_row())
    return buf.getvalue().encode("utf-8")


def make_key(prefix: str, filename: str, partition: bool) -> str:
    """
    Construct S3 object key:
    - flat:        <prefix>/<filename>
    - partitioned: <prefix>/date=YYYY-MM-DD/hour=HH/<filename>
    """
    if not prefix.endswith("/"):
        prefix += "/"
    if not partition:
        return f"{prefix}{filename}"
    now = datetime.utcnow()
    return f"{prefix}date={now.strftime('%Y-%m-%d')}/hour={now.strftime('%H')}/{filename}"


def upload_bytes_to_s3(
    s3_client, bucket: str, key: str, content: bytes, retries: int = 3
) -> None:
    """Upload with simple retry logic."""
    attempt = 0
    while True:
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=content)
            return
        except (BotoCoreError, ClientError) as e:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(min(2 ** attempt, 8))  # backoff


def maybe_write_local_copy(outdir: Optional[str], filename: str, content: bytes) -> None:
    if not outdir:
        return
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, filename), "wb") as f:
        f.write(content)


def main():
    p = argparse.ArgumentParser(description="Generate FinTech CSV batches and push to S3.")
    p.add_argument("--bucket", required=True, help="S3 bucket name (e.g., fintech-bhavana)")
    p.add_argument(
        "--prefix",
        default="Data/transactions",
        help="S3 key prefix (case sensitive). Example: Data/transactions",
    )
    p.add_argument("--region", default="us-east-2", help="AWS region for S3 client")
    p.add_argument("--batch-size", type=int, default=200, help="Rows per CSV file")
    p.add_argument("--interval", type=int, default=15, help="Seconds between uploads")
    p.add_argument(
        "--partition",
        action="store_true",
        help="If set, keys use date/hour partitions under the prefix",
    )
    p.add_argument(
        "--outdir",
        default=None,
        help="Optional local folder to also write each CSV (debugging/backup)",
    )
    args = p.parse_args()

    # Build S3 client (uses your default AWS credentials chain)
    s3 = boto3.client("s3", region_name=args.region)

    print(
        f"→ Streaming CSV batches to s3://{args.bucket}/{args.prefix} "
        f"every {args.interval}s | batch={args.batch_size} | "
        f"partitioned={'YES' if args.partition else 'NO'}"
    )
    if args.outdir:
        print(f"  (also writing local copies under: {args.outdir})")

    try:
        while True:
            # Build batch
            content = build_csv_bytes(args.batch_size)

            # Name file
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            filename = f"transactions_{ts}_{uuid4().hex[:6]}.csv"

            # Compute key and upload
            key = make_key(args.prefix, filename, args.partition)
            upload_bytes_to_s3(s3, args.bucket, key, content)
            maybe_write_local_copy(args.outdir, filename, content)

            print(f"[OK] Uploaded {args.batch_size:4d} rows → s3://{args.bucket}/{key}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()