import logging
import pytz
from datetime import timedelta

from app.synced_db_connection import get_synced_db_client

logger = logging.getLogger(__name__)

ROTATION_LOOKBACK_DAYS = 7

async def get_move_history(carrier: str, plan_date: str, driver_ids: list, timeZone: str) -> list:
    """
    Get aggregated move history for the given carrier and plan date.
    Returns moves grouped by driver, date, and zone with counts.
    """
    try:
        synced_db = get_synced_db_client()
        pool = await synced_db.get_pool()

        start_date = (plan_date - timedelta(days=ROTATION_LOOKBACK_DAYS)).astimezone(pytz.UTC).replace(tzinfo=None)
        end_date = plan_date.astimezone(pytz.UTC).replace(tzinfo=None)

        async with pool.acquire() as conn:
            query = """
                WITH move_data AS (
                    SELECT
                        m.driver,
                        DATE(m.move_completed_date AT TIME ZONE 'UTC' AT TIME ZONE $5) as work_date,
                        -- Classify zones based on distance
                        CASE 
                            WHEN COALESCE(e.distance, 0) <= 30 THEN 'local'
                            WHEN COALESCE(e.distance, 0) <= 80 THEN 'medium'
                            WHEN COALESCE(e.distance, 0) <= 150 THEN 'outofzone'
                            ELSE 'longhaul'
                        END as zone
                    FROM moves m
                    LEFT JOIN LATERAL (
                        SELECT ev.distance
                        FROM events ev
                        WHERE ev.carrier = $1
                            AND ev.driver = m.driver
                            AND ev.moveid = m._id
                        ORDER BY ev.distance DESC
                        LIMIT 1
                    ) e ON TRUE
                    WHERE m.carrier = $1
                        AND m.move_completed_date BETWEEN $2 AND $3
                        AND m.is_deleted = false
                        AND m.vendor_type = 'DRIVER'
                        AND m.driver = ANY($4::text[])
                        -- Filter out weekends (0 = Sunday, 6 = Saturday)
                        AND EXTRACT(DOW FROM m.move_completed_date AT TIME ZONE 'UTC' AT TIME ZONE $5) NOT IN (0, 6)
                )
                SELECT 
                    driver,
                    MAX(work_date) as last_worked_date,
                    ARRAY_AGG(DISTINCT work_date ORDER BY work_date DESC) as worked_dates,
                    COUNT(DISTINCT work_date) as days_worked,
                    -- Zone counts
                    SUM(CASE WHEN zone = 'local' THEN 1 ELSE 0 END)::int as local_count,
                    SUM(CASE WHEN zone = 'medium' THEN 1 ELSE 0 END)::int as medium_count,
                    SUM(CASE WHEN zone = 'outofzone' THEN 1 ELSE 0 END)::int as outofzone_count,
                    SUM(CASE WHEN zone = 'longhaul' THEN 1 ELSE 0 END)::int as longhaul_count
                FROM move_data
                GROUP BY driver;
            """

            rows = await conn.fetch(query, carrier, start_date, end_date, driver_ids, timeZone)
            rows = [dict(row) for row in rows]
            return rows

    except Exception as e:
        logger.error(f"Error getting move history: {str(e)}")
        return []
