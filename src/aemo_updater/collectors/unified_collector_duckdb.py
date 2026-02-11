#!/usr/bin/env python3
"""
DuckDB-backed Unified AEMO Data Collector

Subclass of UnifiedAEMOCollector that writes to a DuckDB database
instead of parquet files. Provides:
  - merge_and_save_duckdb(): DELETE+INSERT pattern (replaces 60-line merge_and_save)
  - collect_30min_scada(): SQL aggregation (replaces full scada5 read into pandas)
  - demand_less_snsg: SQL UPDATE (replaces merge+combine_first pattern)

Usage:
  python -m aemo_updater.collectors.unified_collector_duckdb              # continuous
  python -m aemo_updater.collectors.unified_collector_duckdb --once       # single cycle
  python -m aemo_updater.collectors.unified_collector_duckdb --backfill 200 --once  # backfill
"""

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

from .unified_collector import UnifiedAEMOCollector, classify_duid_fuel

logger = logging.getLogger(__name__)

# Log file path (used by main() for dual logging)
LOG_FILE = Path('/Users/davidleitch/aemo_production/aemo-data-updater/logs/duckdb_collector.log')


class DuckDBCollector(UnifiedAEMOCollector):
    """DuckDB-backed AEMO collector. Inherits all collection methods,
    replaces only the storage layer."""

    # Critical data types that trigger SMS alerts on repeated failure
    CRITICAL_TYPES = ['prices5', 'scada5']
    # Consecutive failures before alerting
    ALERT_THRESHOLD = 3
    # Minimum time between alerts for the same issue (seconds)
    ALERT_COOLDOWN = 3600  # 1 hour

    def __init__(self, config=None):
        super().__init__(config)

        # DuckDB database path
        db_path = self.config.get(
            'duckdb_path',
            os.getenv('AEMO_DUCKDB_PATH',
                       str(self.data_path / 'aemo.duckdb'))
        )
        self.db_path = Path(db_path)
        self.conn = None
        logger.info(f"DuckDB database: {self.db_path}")

        # Verify tables exist (temporary connection)
        self._open_conn()
        tables = {t[0] for t in self.conn.execute("SHOW TABLES").fetchall()}
        expected = set(self.output_files.keys()) | {'duid_mapping'}
        missing = expected - tables - {'demand_less_snsg'}  # demand_less_snsg is a column, not table
        if missing:
            logger.warning(f"Missing DuckDB tables: {missing}")
            logger.warning("Run migrate_to_duckdb.py first to create the database")
        self._close_conn()

        # Table name mapping (output_files key -> DuckDB table name)
        # They're the same names, but we keep the mapping explicit
        self.table_names = {k: k for k in self.output_files}

        # Health monitoring state
        self._failure_counts = {k: 0 for k in self.output_files}
        self._last_alert_times = {}  # {issue_key: datetime}
        self._twilio_client = None

    def _init_twilio(self):
        """Lazy-init Twilio client for collector health alerts."""
        if self._twilio_client is not None:
            return self._twilio_client
        try:
            from twilio.rest import Client
            sid = os.getenv('TWILIO_ACCOUNT_SID')
            token = os.getenv('TWILIO_AUTH_TOKEN')
            if sid and token:
                self._twilio_client = Client(sid, token)
                return self._twilio_client
        except Exception as e:
            logger.warning(f"Could not initialize Twilio for health alerts: {e}")
        self._twilio_client = False  # sentinel: tried and failed
        return None

    def _send_collector_alert(self, message: str, issue_key: str = 'general'):
        """Send SMS alert for collector health issues with cooldown."""
        now = datetime.now()
        last_alert = self._last_alert_times.get(issue_key)
        if last_alert and (now - last_alert).total_seconds() < self.ALERT_COOLDOWN:
            logger.debug(f"Alert suppressed (cooldown): {issue_key}")
            return

        client = self._init_twilio()
        if not client:
            logger.error(f"Cannot send alert (no Twilio): {message}")
            return

        from_number = os.getenv('TWILIO_PHONE_NUMBER')
        to_number = os.getenv('MY_PHONE_NUMBER')
        if not from_number or not to_number:
            logger.error("Missing TWILIO_PHONE_NUMBER or MY_PHONE_NUMBER")
            return

        try:
            client.messages.create(body=message, from_=from_number, to=to_number)
            self._last_alert_times[issue_key] = now
            logger.info(f"Health alert SMS sent: {message}")
        except Exception as e:
            logger.error(f"Failed to send health alert SMS: {e}")

    def merge_and_save(self, df: pd.DataFrame, output_file: Path,
                       key_columns: List[str]) -> bool:
        """DuckDB replacement for merge_and_save. Uses DELETE+INSERT pattern.

        Args:
            df: New data to merge
            output_file: Path object (used to derive table name from self.output_files)
            key_columns: Key columns for deduplication
        Returns:
            True on success
        """
        if df.empty:
            return False

        # Derive table name from output_file path
        table_name = output_file.stem  # e.g., prices5.parquet -> prices5

        try:
            key_list = ', '.join(key_columns)

            # Use explicit column list to handle schema mismatches
            # (e.g., demand30 table has demand_less_snsg but collector doesn't supply it)
            col_list = ', '.join(df.columns)

            self.conn.register('_new_data', df)
            try:
                self.conn.execute('BEGIN TRANSACTION')

                # Delete existing rows matching new data's keys
                self.conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE ({key_list}) IN (SELECT {key_list} FROM _new_data)
                """)

                # Insert new rows into matching columns only
                self.conn.execute(f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM _new_data")

                self.conn.execute('COMMIT')

                logger.info(f"DuckDB: merged {len(df)} rows into {table_name}")
                return True

            except Exception as e:
                self.conn.execute('ROLLBACK')
                raise e
            finally:
                self.conn.unregister('_new_data')

        except Exception as e:
            logger.error(f"Error merging to DuckDB table {table_name}: {e}")
            return False

    def collect_30min_scada(self) -> pd.DataFrame:
        """Collect 30-min SCADA by aggregating scada5 in DuckDB.

        This replaces the pandas-based method that reads the entire scada5
        parquet file (72M rows, 236MB) into memory. The SQL version queries
        only recent data directly from the DuckDB table.
        """
        try:
            # Find the latest scada30 timestamp
            result = self.conn.execute(
                "SELECT MAX(settlementdate) FROM scada30"
            ).fetchone()
            last_30min_time = result[0] if result and result[0] else None

            if last_30min_time:
                logger.info(f"Last scada30 timestamp: {last_30min_time}")
                time_filter = f"AND settlementdate > '{last_30min_time}'"
            else:
                logger.info("No existing scada30 data, processing all scada5")
                time_filter = ""

            # Find 30-minute endpoints in new data
            endpoints = self.conn.execute(f"""
                SELECT DISTINCT settlementdate
                FROM scada5
                WHERE EXTRACT(MINUTE FROM settlementdate) IN (0, 30)
                {time_filter}
                ORDER BY settlementdate
            """).fetchall()

            if not endpoints:
                logger.debug("No 30-minute endpoints found in new data")
                return pd.DataFrame()

            logger.info(f"Found {len(endpoints)} potential 30-minute endpoints")

            # Aggregate: for each 30-min endpoint, average the 6 intervals
            # (endpoint-25min, endpoint-20min, ..., endpoint) for each DUID
            result_df = self.conn.execute(f"""
                SELECT
                    e.endpoint AS settlementdate,
                    s.duid,
                    AVG(s.scadavalue) AS scadavalue
                FROM (
                    SELECT DISTINCT settlementdate AS endpoint
                    FROM scada5
                    WHERE EXTRACT(MINUTE FROM settlementdate) IN (0, 30)
                    {time_filter}
                ) e
                JOIN scada5 s ON
                    s.settlementdate > (e.endpoint - INTERVAL '30 minutes')
                    AND s.settlementdate <= e.endpoint
                GROUP BY e.endpoint, s.duid
                ORDER BY e.endpoint, s.duid
            """).df()

            if result_df.empty:
                logger.info("No complete 30-minute periods found")
                return pd.DataFrame()

            logger.info(f"Aggregated {len(result_df)} records for {len(endpoints)} endpoints")
            return result_df

        except Exception as e:
            logger.error(f"Error in DuckDB collect_30min_scada: {e}")
            return pd.DataFrame()

    def run_single_update(self) -> Dict[str, bool]:
        """Run a single update cycle. Overrides parent to handle
        demand_less_snsg via SQL UPDATE instead of parquet merge."""
        logger.info("=== Starting DuckDB update cycle ===")
        start_time = datetime.now()

        results = {}

        # ----- 5-minute collections -----
        logger.info("Collecting 5-minute data...")

        for data_type, collect_fn, keys in [
            ('prices5', self.collect_5min_prices, ['settlementdate', 'regionid']),
            ('scada5', self.collect_5min_scada, ['settlementdate', 'duid']),
            ('transmission5', self.collect_5min_transmission, ['settlementdate', 'interconnectorid']),
            ('curtailment5', self.collect_5min_curtailment, ['settlementdate', 'duid']),
            ('curtailment_regional5', self.collect_5min_regional_curtailment, ['settlementdate', 'regionid']),
            ('curtailment_duid5', self.collect_5min_duid_curtailment, ['settlementdate', 'duid']),
            ('bdu5', self.collect_5min_bdu, ['settlementdate', 'regionid']),
        ]:
            try:
                df = collect_fn()
                results[data_type] = self.merge_and_save(
                    df, self.output_files[data_type], keys
                )
                # Check for new DUIDs on scada
                if data_type == 'scada5' and not df.empty:
                    new_duids = self._check_new_duids(df)
                    if new_duids:
                        logger.info(f"Discovered {len(new_duids)} new DUIDs")
                # Check price alerts
                if data_type == 'prices5' and not df.empty:
                    try:
                        from .twilio_price_alerts import check_price_alerts
                        alert_df = df.copy()
                        alert_df = alert_df.rename(columns={'regionid': 'REGIONID', 'rrp': 'RRP'})
                        alert_df = alert_df.set_index('settlementdate')
                        check_price_alerts(alert_df)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Error collecting {data_type}: {e}")
                results[data_type] = False

        # ----- 30-minute collections -----
        self.cycle_count += 1
        logger.info("Collecting 30-minute data...")

        # Trading data (prices30 + transmission30)
        try:
            trading_data = self.collect_30min_trading()
            results['prices30'] = self.merge_and_save(
                trading_data['prices30'],
                self.output_files['prices30'],
                ['settlementdate', 'regionid']
            )
            results['transmission30'] = self.merge_and_save(
                trading_data['transmission30'],
                self.output_files['transmission30'],
                ['settlementdate', 'interconnectorid']
            )
        except Exception as e:
            logger.error(f"Error collecting 30-min trading: {e}")
            results['prices30'] = False
            results['transmission30'] = False

        # SCADA 30-min aggregation (optimized SQL version)
        try:
            scada30_df = self.collect_30min_scada()
            results['scada30'] = self.merge_and_save(
                scada30_df,
                self.output_files['scada30'],
                ['settlementdate', 'duid']
            )
        except Exception as e:
            logger.error(f"Error collecting 30-min SCADA: {e}")
            results['scada30'] = False

        # Rooftop, demand
        for data_type, collect_fn, keys in [
            ('rooftop30', self.collect_30min_rooftop, ['settlementdate', 'regionid']),
            ('demand30', self.collect_30min_demand, ['settlementdate', 'regionid']),
        ]:
            try:
                df = collect_fn()
                results[data_type] = self.merge_and_save(
                    df, self.output_files[data_type], keys
                )
            except Exception as e:
                logger.error(f"Error collecting {data_type}: {e}")
                results[data_type] = False

        # demand_less_snsg — SQL UPDATE instead of parquet merge
        try:
            demand_less_snsg_df = self.collect_30min_demand_less_snsg()
            if not demand_less_snsg_df.empty:
                self.conn.register('_snsg_data', demand_less_snsg_df)
                try:
                    self.conn.execute("""
                        UPDATE demand30
                        SET demand_less_snsg = _snsg_data.demand_less_snsg
                        FROM _snsg_data
                        WHERE demand30.settlementdate = _snsg_data.settlementdate
                          AND demand30.regionid = _snsg_data.regionid
                    """)
                    logger.info(f"Updated {len(demand_less_snsg_df)} demand_less_snsg records")
                    results['demand_less_snsg'] = True
                finally:
                    self.conn.unregister('_snsg_data')
            else:
                results['demand_less_snsg'] = True
        except Exception as e:
            logger.error(f"Error collecting demand_less_snsg: {e}")
            results['demand_less_snsg'] = False

        # Predispatch
        try:
            predispatch_df = self.collect_predispatch()
            if not predispatch_df.empty:
                results['predispatch'] = self.merge_and_save(
                    predispatch_df,
                    self.output_files['predispatch'],
                    ['run_time', 'settlementdate', 'regionid']
                )
            else:
                results['predispatch'] = True
        except Exception as e:
            logger.error(f"Error collecting predispatch: {e}")
            results['predispatch'] = False

        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        success_count = sum(results.values())
        total_collections = len(results)

        logger.info(f"=== DuckDB update cycle complete in {duration:.1f}s ===")
        logger.info(f"Results: {success_count}/{total_collections} successful")
        for data_type, success in results.items():
            status = "+" if success else "o"
            logger.info(f"  {data_type}: {status}")

        return results

    def _open_conn(self, max_retries=5, base_delay=2.0):
        """Open DuckDB connection with retry on lock conflict."""
        for attempt in range(max_retries):
            try:
                self.conn = duckdb.connect(str(self.db_path))
                self.conn.execute("PRAGMA force_compression='Dictionary'")
                return
            except duckdb.IOException as e:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.warning(
                        f"DuckDB lock conflict (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {wait:.0f}s: {e}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"DuckDB lock conflict after {max_retries} attempts: {e}")
                    self._send_collector_alert(
                        f"AEMO collector BLOCKED: Cannot acquire DuckDB lock after "
                        f"{max_retries} attempts. Another process may be holding an "
                        f"exclusive connection. Check dashboard processes.",
                        issue_key='lock_conflict'
                    )
                    raise

    def _close_conn(self):
        """Close DuckDB connection, releasing file lock for other processes."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _check_new_duids(self, df: pd.DataFrame) -> List[str]:
        """Check for new DUIDs and auto-insert classified ones into duid_mapping."""
        new_duids = super()._check_new_duids(df)
        if new_duids and self.conn:
            self._auto_insert_duid_mapping(new_duids)
        return new_duids

    def _auto_insert_duid_mapping(self, new_duids: List[str]):
        """Insert auto-classified DUIDs into duid_mapping table."""
        rows = []
        for duid in new_duids:
            fuel, confidence = classify_duid_fuel(duid)
            if fuel in ('Unknown', 'Rooftop Solar', 'Distributed Gen'):
                continue
            if confidence == 'low':
                continue
            rows.append({
                'region': '',
                'site name': f'[auto-classified {datetime.now().strftime("%Y-%m-%d")}]',
                'owner': '',
                'duid': duid,
                'capacity_mw': 0.0,
                'storage_mwh': 0.0,
                'fuel': fuel,
            })

        if not rows:
            return

        insert_df = pd.DataFrame(rows)
        try:
            self.conn.register('_new_duids', insert_df)
            try:
                col_list = ', '.join(f'"{c}"' for c in insert_df.columns)
                self.conn.execute(
                    f"INSERT INTO duid_mapping ({col_list}) "
                    f"SELECT {col_list} FROM _new_duids"
                )
                logger.info(
                    f"Auto-inserted {len(rows)} classified DUIDs into duid_mapping: "
                    + ", ".join(r['duid'] for r in rows)
                )
            finally:
                self.conn.unregister('_new_duids')
        except Exception as e:
            logger.error(f"Failed to auto-insert DUIDs into duid_mapping: {e}")

    def _update_failure_tracking(self, results: Dict[str, bool]):
        """Update failure counts and send SMS alerts for critical types."""
        for data_type, success in results.items():
            if success:
                self._failure_counts[data_type] = 0
            else:
                self._failure_counts[data_type] = self._failure_counts.get(data_type, 0) + 1

        # Check critical types for sustained failures
        for dt in self.CRITICAL_TYPES:
            count = self._failure_counts.get(dt, 0)
            if count >= self.ALERT_THRESHOLD:
                self._send_collector_alert(
                    f"AEMO collector: {dt} has failed {count} consecutive cycles "
                    f"(~{count * 4.5:.0f} min). Data may be stale.",
                    issue_key=f'failure_{dt}'
                )

    def run_continuous(self, update_interval_minutes: float = 4.5):
        """Run continuous collection, releasing the DB lock between cycles.

        DuckDB holds an exclusive file lock while a write connection is open.
        By closing the connection during the sleep period, the dashboard and
        comparison scripts can read the database concurrently.
        """
        logger.info("Starting DuckDB continuous collection...")
        logger.info(f"Update interval: {update_interval_minutes} minutes")

        update_interval_seconds = update_interval_minutes * 60
        cycle_count = 0

        while True:
            try:
                cycle_count += 1
                logger.info(f"--- Cycle {cycle_count} ---")

                self._open_conn()
                results = self.run_single_update()

                if any(results.values()):
                    successful_types = [k for k, v in results.items() if v]
                    logger.info(f"Updated: {', '.join(successful_types)}")

                # Track failures and alert on critical issues
                self._update_failure_tracking(results)

            except duckdb.IOException as e:
                logger.error(f"DuckDB lock error in cycle {cycle_count}: {e}")
                # _open_conn already sent an alert if retries exhausted
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                self._send_collector_alert(
                    f"AEMO collector unexpected error in cycle {cycle_count}: {e}",
                    issue_key='unexpected_error'
                )
            finally:
                self._close_conn()

            logger.info(f"Waiting {update_interval_minutes} minutes for next cycle...")
            time.sleep(update_interval_seconds)

    def close(self):
        """Close DuckDB connection."""
        self._close_conn()
        logger.info("DuckDB connection closed")


def main():
    """Run the DuckDB collector."""
    import argparse

    # Set up dual logging: stdout + file
    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)

    # Add file handler so logs persist regardless of how collector is started
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(LOG_FILE))
    file_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='DuckDB-backed AEMO collector')
    parser.add_argument('--once', action='store_true', help='Run single cycle then exit')
    parser.add_argument('--backfill', type=int, default=0,
                        help='Backfill mode: download last N files per type (default 5)')
    args = parser.parse_args()

    config = {}
    if args.backfill > 0:
        config['max_files_per_cycle'] = args.backfill
        logger.info(f"Backfill mode: downloading last {args.backfill} files per type")

    collector = DuckDBCollector(config=config)

    if args.once or args.backfill > 0:
        collector._open_conn()
        try:
            collector.run_single_update()
        finally:
            collector._close_conn()
    else:
        collector.run_continuous()

    collector.close()


if __name__ == '__main__':
    main()
