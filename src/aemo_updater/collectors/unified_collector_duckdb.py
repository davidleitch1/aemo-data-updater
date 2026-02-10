#!/usr/bin/env python3
"""
DuckDB-backed Unified AEMO Data Collector

Subclass of UnifiedAEMOCollector that writes to a DuckDB database
instead of parquet files. Provides:
  - merge_and_save_duckdb(): DELETE+INSERT pattern (replaces 60-line merge_and_save)
  - collect_30min_scada(): SQL aggregation (replaces full scada5 read into pandas)
  - demand_less_snsg: SQL UPDATE (replaces merge+combine_first pattern)

Usage:
  python -m aemo_updater.collectors.unified_collector_duckdb        # continuous
  python -m aemo_updater.collectors.unified_collector_duckdb --once  # single cycle
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

from .unified_collector import UnifiedAEMOCollector

logger = logging.getLogger(__name__)


class DuckDBCollector(UnifiedAEMOCollector):
    """DuckDB-backed AEMO collector. Inherits all collection methods,
    replaces only the storage layer."""

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

    def _open_conn(self):
        """Open DuckDB connection."""
        self.conn = duckdb.connect(str(self.db_path))
        self.conn.execute("PRAGMA force_compression='Dictionary'")

    def _close_conn(self):
        """Close DuckDB connection, releasing file lock for other processes."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def run_continuous(self, update_interval_minutes: float = 4.5):
        """Run continuous collection, releasing the DB lock between cycles.

        DuckDB holds an exclusive file lock while a write connection is open.
        By closing the connection during the sleep period, the dashboard and
        comparison scripts can read the database concurrently.
        """
        import time

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

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
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

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )

    parser = argparse.ArgumentParser(description='DuckDB-backed AEMO collector')
    parser.add_argument('--once', action='store_true', help='Run single cycle then exit')
    args = parser.parse_args()

    collector = DuckDBCollector()

    if args.once:
        collector.run_single_update()
    else:
        collector.run_continuous()

    collector.close()


if __name__ == '__main__':
    main()
