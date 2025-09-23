from collections import Counter, defaultdict
import os
import logging
from typing import Dict, List, Tuple, Optional, AsyncGenerator
from app.synced_db_connection import get_synced_db_client
from app.postgres_connection import PostgresConnection
from datetime import datetime, timedelta, timezone
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EventPatternAnalyzer:
    """Optimized event pattern analyzer with SQL-based processing."""
    
    def __init__(self):
        self.pull_deliver_patterns = defaultdict(Counter)
        self.deliver_return_patterns = defaultdict(Counter)
        self.processed_loads = 0

    async def generate_recommendations_sql(self, carrier_id: str, min_frequency: int = 1) -> List[dict]:
        """
        Generate recommendations using optimized SQL query - processes all logic in database.
        Returns the same format as the original generate_recommendations method.
        
        Performance optimizations:
        - Uses RECURSIVE CTE for efficient event processing
        - MATERIALIZED CTE for valid loads to avoid repeated computation
        - Single pass pattern identification with proper indexing
        - Pre-computed aggregates using window functions
        - JSONB output for efficient data transfer
        
        Recommended indexes for optimal performance:
        - CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_carrier_date 
          ON public.events(carrier, "arrived") 
          WHERE customerid IS NOT NULL AND customerid != '';
        - CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_loadid_order 
          ON public.events(loadid, order_index, type);
        - CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_type 
          ON public.events(type) 
          WHERE type IN ('PULLCONTAINER', 'DELIVERLOAD', 'DROPCONTAINER', 'LIFTOFF', 'RETURNCONTAINER');
        """
        logger.info(f"Generating PULL-DELIVER, DELIVER-RETURN patterns and HOOK_RETURN patterns using SQL for carrier: {carrier_id}")
        
        sql_query = """
        WITH RECURSIVE load_events AS (
            -- Get events with pre-filtering and minimal columns
            SELECT 
                loadid,
                type,
                customerid,
                order_index,
                -- Use DENSE_RANK instead of ROW_NUMBER for better performance with proper index
                DENSE_RANK() OVER (PARTITION BY loadid ORDER BY order_index) as seq_num
            FROM public.events
            WHERE 
                carrier = $1
                AND "arrived" >= CURRENT_DATE - INTERVAL '365 days'
                AND customerid IS NOT NULL 
                AND customerid != ''
                AND type = ANY(ARRAY['PULLCONTAINER', 'DELIVERLOAD', 'DROPCONTAINER', 'LIFTOFF', 'RETURNCONTAINER'])
        ),

        -- Materialize valid loads to avoid repeated computation
        valid_loads AS MATERIALIZED (
            SELECT 
                loadid,
                bool_or(type IN ('DROPCONTAINER', 'LIFTOFF')) as has_intermediate
            FROM load_events
            GROUP BY loadid
            HAVING 
                COUNT(*) > 4
                AND bool_or(type IN ('DROPCONTAINER', 'LIFTOFF'))
        ),

        -- Single pass to identify all patterns with proper deduplication
        event_patterns AS (
            SELECT DISTINCT
                e1.loadid,
                e1.type as event1_type,
                e1.customerid as event1_customer,
                e1.order_index as event1_order,
                e2.type as event2_type,
                e2.customerid as event2_customer,
                e2.order_index as event2_order,
                e3.type as event3_type,
                e3.customerid as event3_customer,
                e3.order_index as event3_order
            FROM load_events e1
            INNER JOIN valid_loads vl ON e1.loadid = vl.loadid
            INNER JOIN load_events e2 ON (
                e1.loadid = e2.loadid 
                AND e2.order_index = e1.order_index + 1
            )
            INNER JOIN load_events e3 ON (
                e1.loadid = e3.loadid 
                AND e3.order_index > e2.order_index
                AND (
                    -- Pattern 1: PULL -> DROP/LIFT -> DELIVER
                    (e1.type = 'PULLCONTAINER' 
                     AND e2.type IN ('DROPCONTAINER', 'LIFTOFF')
                     AND e3.type = 'DELIVERLOAD')
                    OR
                    -- Pattern 2: DELIVER -> DROP/LIFT -> RETURN
                    (e1.type = 'DELIVERLOAD' 
                     AND e2.type IN ('DROPCONTAINER', 'LIFTOFF')
                     AND e3.type = 'RETURNCONTAINER')
                )
            )
        ),

        -- Extract and deduplicate patterns efficiently - get FIRST matching event
        pattern_extraction AS (
            SELECT DISTINCT ON (loadid, event1_order, event1_type)
                CASE 
                    WHEN event1_type = 'PULLCONTAINER' THEN 'PULL_DELIVER'
                    WHEN event1_type = 'DELIVERLOAD' THEN 'DELIVER_RETURN'
                END as recommendation_type,
                event1_customer as from_customer_id,
                event3_customer as to_customer_id,
                event2_type as outcome_type,
                event2_customer as outcome_customer_id
            FROM event_patterns
            WHERE 
                event1_customer != event3_customer  -- Avoid self-loops
                AND event1_customer IS NOT NULL 
                AND event3_customer IS NOT NULL
                AND event2_customer IS NOT NULL
            ORDER BY loadid, event1_order, event1_type, event3_order ASC
        ),

        -- Aggregate patterns with single GROUP BY
        pattern_aggregates AS (
            SELECT
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id,
                COUNT(*) as pattern_count,
                -- Pre-compute aggregates for final decision
                SUM(COUNT(*)) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_total,
                SUM(CASE WHEN outcome_type = 'DROPCONTAINER' THEN COUNT(*) ELSE 0 END) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_drops,
                SUM(CASE WHEN outcome_type = 'LIFTOFF' THEN COUNT(*) ELSE 0 END) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_lifts,
                -- Rank patterns within each route for most common selection
                ROW_NUMBER() OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                    ORDER BY COUNT(*) DESC
                ) as pattern_rank
            FROM pattern_extraction
            GROUP BY 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id
        ),

        -- Find most common specific patterns (event_type + customer_id) for each route
        most_common_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id,
                pattern_count,
                -- Rank patterns by frequency within each route
                ROW_NUMBER() OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                    ORDER BY pattern_count DESC
                ) as pattern_rank
            FROM pattern_aggregates
            WHERE route_total >= $2  -- Min frequency filter
        ),

        -- Get the most common overall pattern for each route
        route_most_common AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type as most_common_type,
                outcome_customer_id as most_common_customer,
                pattern_count as most_common_frequency
            FROM most_common_patterns
            WHERE pattern_rank = 1
        ),

        -- Get most common DROP and LIFT customers separately
        drop_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_customer_id as drop_customer,
                pattern_count as drop_frequency
            FROM most_common_patterns
            WHERE outcome_type = 'DROPCONTAINER'
            AND pattern_rank = 1
        ),
        
        lift_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_customer_id as lift_customer,
                pattern_count as lift_frequency
            FROM most_common_patterns
            WHERE outcome_type = 'LIFTOFF'
            AND pattern_rank = 1
        ),

        -- Final route recommendations with proper logic
        route_recommendations AS (
            SELECT 
                ra.recommendation_type,
                ra.from_customer_id,
                ra.to_customer_id,
                ra.route_drops,
                ra.route_lifts,
                ra.route_total,
                rmc.most_common_type,
                rmc.most_common_customer,
                rmc.most_common_frequency,
                dp.drop_customer,
                dp.drop_frequency,
                lp.lift_customer,
                lp.lift_frequency
            FROM pattern_aggregates ra
            LEFT JOIN route_most_common rmc ON (
                ra.recommendation_type = rmc.recommendation_type
                AND ra.from_customer_id = rmc.from_customer_id
                AND ra.to_customer_id = rmc.to_customer_id
            )
            LEFT JOIN drop_patterns dp ON (
                ra.recommendation_type = dp.recommendation_type
                AND ra.from_customer_id = dp.from_customer_id
                AND ra.to_customer_id = dp.to_customer_id
            )
            LEFT JOIN lift_patterns lp ON (
                ra.recommendation_type = lp.recommendation_type
                AND ra.from_customer_id = lp.from_customer_id
                AND ra.to_customer_id = lp.to_customer_id
            )
            WHERE ra.route_total >= $2
        )

        -- Final output with corrected logic and deduplication
        SELECT DISTINCT
            $1 as carrier,
            recommendation_type,
            from_customer_id,
            to_customer_id,
            -- Apply recommendation logic: choose event type based on counts
            CASE 
                WHEN route_drops > route_lifts THEN 'DROPCONTAINER'
                WHEN route_lifts > route_drops THEN 'LIFTOFF'
                ELSE most_common_type
            END as recommended_event_type,
            -- Select customer based on recommended event type with null fallback
            CASE 
                WHEN route_drops > route_lifts AND drop_customer IS NOT NULL THEN drop_customer
                WHEN route_lifts > route_drops AND lift_customer IS NOT NULL THEN lift_customer
                WHEN most_common_customer IS NOT NULL THEN most_common_customer
                WHEN drop_customer IS NOT NULL THEN drop_customer
                WHEN lift_customer IS NOT NULL THEN lift_customer
                ELSE from_customer_id
            END as recommended_customer_id,
            -- Calculate recommended frequency count for the specific pattern
            CASE 
                WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                ELSE most_common_frequency
            END as recommended_frequency_count,
            jsonb_build_object(
                'total_dropcontainer', route_drops,
                'total_liftoff', route_lifts,
                'total_events', route_total,
                'recommended_frequency_count', 
                CASE 
                    WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                    WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                    ELSE most_common_frequency
                END
            ) as event_counts
        FROM route_recommendations
        ORDER BY 
            CASE 
                WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                ELSE most_common_frequency
            END DESC;
        """
        
        postgres = get_synced_db_client()
        pool = await postgres.get_pool()
        
        recommendations = []
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                records = await conn.fetch(sql_query, carrier_id, min_frequency)
                
                for record in records:
                    # The event_counts is already a JSONB object from the query
                    event_counts = record['event_counts']
                    if isinstance(event_counts, str):
                        event_counts = json.loads(event_counts)
                    
                    # Validate that recommended_customer_id is not null
                    recommended_customer_id = record['recommended_customer_id']
                    if recommended_customer_id is None:
                        logger.warning(f"Skipping recommendation with null recommended_customer_id: {record}")
                        continue
                    
                    recommendations.append({
                        "recommendation_type": record['recommendation_type'],
                        "from_customer_id": record['from_customer_id'],
                        "to_customer_id": record['to_customer_id'],
                        "recommended_event_type": record['recommended_event_type'],
                        "recommended_customer_id": recommended_customer_id,
                        "event_counts": event_counts
                    })
        
        logger.info(f"Generated {len(recommendations)} PULL-DELIVER and DELIVER-RETURN patterns recommendations using SQL")
        
        # Add HOOK_RETURN patterns separately to avoid duplication
        hook_recommendations = await self.generate_hook_return_patterns(carrier_id, min_frequency)
        recommendations.extend(hook_recommendations)
        
        logger.info(f"Total recommendations: {len(recommendations)}")
        return recommendations

    async def generate_hook_return_patterns(self, carrier_id: str, min_frequency: int = 1) -> List[dict]:
        """
        Generate HOOK_RETURN patterns separately to avoid duplication with existing patterns.
        Pattern: HOOKCONTAINER -> DROP/LIFTOFF -> HOOK/LIFTON -> RETURNCONTAINER
        """
        logger.info(f"Generating HOOK_RETURN patterns for carrier: {carrier_id}")
        
        sql_query = """
        WITH RECURSIVE load_events AS (
            SELECT
                loadid,
                type,
                customerid,
                order_index,
                DENSE_RANK() OVER (PARTITION BY loadid ORDER BY order_index) as seq_num
            FROM public.events
            WHERE
                carrier = $1
                AND "arrived" >= CURRENT_DATE - INTERVAL '365 days'
                AND customerid IS NOT NULL
                AND customerid != ''
                AND type = ANY(ARRAY['HOOKCONTAINER', 'DROPCONTAINER', 'LIFTOFF', 'LIFTON', 'RETURNCONTAINER'])
        ),

        valid_loads AS MATERIALIZED (
            SELECT 
                loadid
            FROM load_events
            GROUP BY loadid
            HAVING 
                COUNT(*) > 4
                AND bool_or(type IN ('DROPCONTAINER', 'LIFTOFF'))
        ),

        hook_patterns AS (
            SELECT DISTINCT
                e1.loadid,
                e1.type as event1_type,
                e1.customerid as event1_customer,
                e1.order_index as event1_order,
                e2.type as event2_type,
                e2.customerid as event2_customer,
                e2.order_index as event2_order,
                e3.type as event3_type,
                e3.customerid as event3_customer,
                e3.order_index as event3_order,
                e4.type as event4_type,
                e4.customerid as event4_customer,
                e4.order_index as event4_order
            FROM load_events e1
            INNER JOIN valid_loads vl ON e1.loadid = vl.loadid
            INNER JOIN load_events e2 ON (
                e1.loadid = e2.loadid 
                AND e2.order_index > e1.order_index
                AND e2.type IN ('DROPCONTAINER', 'LIFTOFF')
            )
            INNER JOIN load_events e3 ON (
                e1.loadid = e3.loadid 
                AND e3.order_index > e2.order_index
                AND e3.type IN ('HOOKCONTAINER', 'LIFTON')
            )
            INNER JOIN load_events e4 ON (
                e1.loadid = e4.loadid 
                AND e4.order_index > e3.order_index
                AND e1.type = 'HOOKCONTAINER' 
                AND e4.type = 'RETURNCONTAINER'
            )
        ),

        pattern_extraction AS (
            SELECT DISTINCT ON (loadid, event1_order, event1_type, event2_order)
                'HOOK_RETURN' as recommendation_type,
                event1_customer as from_customer_id,
                event4_customer as to_customer_id,
                event2_type as outcome_type,
                event2_customer as outcome_customer_id
            FROM hook_patterns
            WHERE 
                event1_customer != event4_customer
                AND event1_customer IS NOT NULL 
                AND event4_customer IS NOT NULL
                AND event2_customer IS NOT NULL
            ORDER BY loadid, event1_order, event1_type, event2_order, event4_order ASC
        ),

        pattern_aggregates AS (
            SELECT
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id,
                COUNT(*) as pattern_count,
                SUM(COUNT(*)) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_total,
                SUM(CASE WHEN outcome_type = 'DROPCONTAINER' THEN COUNT(*) ELSE 0 END) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_drops,
                SUM(CASE WHEN outcome_type = 'LIFTOFF' THEN COUNT(*) ELSE 0 END) OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                ) as route_lifts,
                ROW_NUMBER() OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                    ORDER BY COUNT(*) DESC
                ) as pattern_rank
            FROM pattern_extraction
            GROUP BY 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id
        ),

        most_common_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type,
                outcome_customer_id,
                pattern_count,
                ROW_NUMBER() OVER (
                    PARTITION BY recommendation_type, from_customer_id, to_customer_id
                    ORDER BY pattern_count DESC
                ) as pattern_rank
            FROM pattern_aggregates
            WHERE route_total >= $2
        ),

        route_most_common AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_type as most_common_type,
                outcome_customer_id as most_common_customer,
                pattern_count as most_common_frequency
            FROM most_common_patterns
            WHERE pattern_rank = 1
        ),

        drop_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_customer_id as drop_customer,
                pattern_count as drop_frequency
            FROM most_common_patterns
            WHERE outcome_type = 'DROPCONTAINER'
            AND pattern_rank = 1
        ),
        
        lift_patterns AS (
            SELECT 
                recommendation_type,
                from_customer_id,
                to_customer_id,
                outcome_customer_id as lift_customer,
                pattern_count as lift_frequency
            FROM most_common_patterns
            WHERE outcome_type = 'LIFTOFF'
            AND pattern_rank = 1
        ),

        route_recommendations AS (
            SELECT 
                ra.recommendation_type,
                ra.from_customer_id,
                ra.to_customer_id,
                ra.route_drops,
                ra.route_lifts,
                ra.route_total,
                rmc.most_common_type,
                rmc.most_common_customer,
                rmc.most_common_frequency,
                dp.drop_customer,
                dp.drop_frequency,
                lp.lift_customer,
                lp.lift_frequency
            FROM pattern_aggregates ra
            LEFT JOIN route_most_common rmc ON (
                ra.recommendation_type = rmc.recommendation_type
                AND ra.from_customer_id = rmc.from_customer_id
                AND ra.to_customer_id = rmc.to_customer_id
            )
            LEFT JOIN drop_patterns dp ON (
                ra.recommendation_type = dp.recommendation_type
                AND ra.from_customer_id = dp.from_customer_id
                AND ra.to_customer_id = dp.to_customer_id
            )
            LEFT JOIN lift_patterns lp ON (
                ra.recommendation_type = lp.recommendation_type
                AND ra.from_customer_id = lp.from_customer_id
                AND ra.to_customer_id = lp.to_customer_id
            )
            WHERE ra.route_total >= $2
        )

        SELECT DISTINCT
            $1 as carrier,
            recommendation_type,
            from_customer_id,
            to_customer_id,
            CASE 
                WHEN route_drops > route_lifts THEN 'DROPCONTAINER'
                WHEN route_lifts > route_drops THEN 'LIFTOFF'
                ELSE most_common_type
            END as recommended_event_type,
            CASE 
                WHEN route_drops > route_lifts AND drop_customer IS NOT NULL THEN drop_customer
                WHEN route_lifts > route_drops AND lift_customer IS NOT NULL THEN lift_customer
                WHEN most_common_customer IS NOT NULL THEN most_common_customer
                WHEN drop_customer IS NOT NULL THEN drop_customer
                WHEN lift_customer IS NOT NULL THEN lift_customer
                ELSE from_customer_id
            END as recommended_customer_id,
            CASE 
                WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                ELSE most_common_frequency
            END as recommended_frequency_count,
            jsonb_build_object(
                'total_dropcontainer', route_drops,
                'total_liftoff', route_lifts,
                'total_events', route_total,
                'recommended_frequency_count', 
                CASE 
                    WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                    WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                    ELSE most_common_frequency
                END
            ) as event_counts
        FROM route_recommendations
        ORDER BY 
            CASE 
                WHEN route_drops > route_lifts THEN COALESCE(drop_frequency, 0)
                WHEN route_lifts > route_drops THEN COALESCE(lift_frequency, 0)
                ELSE most_common_frequency
            END DESC;
        """
        
        postgres = get_synced_db_client()
        pool = await postgres.get_pool()
        
        recommendations = []
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                records = await conn.fetch(sql_query, carrier_id, min_frequency)
                
                for record in records:
                    event_counts = record['event_counts']
                    if isinstance(event_counts, str):
                        event_counts = json.loads(event_counts)
                    
                    # Validate that recommended_customer_id is not null
                    recommended_customer_id = record['recommended_customer_id']
                    if recommended_customer_id is None:
                        logger.warning(f"Skipping HOOK_RETURN recommendation with null recommended_customer_id: {record}")
                        continue
                    
                    recommendations.append({
                        "recommendation_type": record['recommendation_type'],
                        "from_customer_id": record['from_customer_id'],
                        "to_customer_id": record['to_customer_id'],
                        "recommended_event_type": record['recommended_event_type'],
                        "recommended_customer_id": recommended_customer_id,
                        "event_counts": event_counts
                    })
        
        logger.info(f"Generated {len(recommendations)} HOOK_RETURN recommendations")
        return recommendations

    async def save_recommendations(self, recommendations: List[dict], carrier_id: str) -> None:
        """
        Save recommendations with proper async database handling, processing and saving in chunks.
        """
        if not recommendations:
            logger.info("No recommendations to save.")
            return

        logger.info(f"Saving {len(recommendations)} recommendations in chunks...")
        postgres = PostgresConnection()
        pool = await postgres.get_pool()

        insert_query = """
        INSERT INTO public.yard_recommendations (
            carrier, recommendation_type, from_customer_id, to_customer_id, 
            recommended_event_type, recommended_customer_id, event_counts
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (carrier, recommendation_type, from_customer_id, to_customer_id) DO UPDATE SET
            recommended_event_type = EXCLUDED.recommended_event_type,
            recommended_customer_id = EXCLUDED.recommended_customer_id,
            event_counts = EXCLUDED.event_counts,
            updated_at = NOW();
        """

        CHUNK_SIZE = 5000

        async with pool.acquire() as conn:
            cleared = False
            for i in range(0, len(recommendations), CHUNK_SIZE):
                chunk = recommendations[i:i+CHUNK_SIZE]
                values = [
                    (
                        carrier_id, rec['recommendation_type'], rec['from_customer_id'], rec['to_customer_id'],
                        rec['recommended_event_type'], rec['recommended_customer_id'],
                        json.dumps(rec['event_counts'])
                    ) for rec in chunk
                ]
                async with conn.transaction():
                    if not cleared and i == 0:
                        await conn.execute("DELETE FROM public.yard_recommendations WHERE carrier = $1;", carrier_id)
                        cleared = True
                    await conn.executemany(insert_query, values)
                logger.info(f"Saved chunk {i//CHUNK_SIZE + 1} ({len(chunk)} recommendations)")
        logger.info("Successfully saved all recommendations.")

async def process_yard_recommendations(carrier_id: str, save_to_db: bool = True, min_frequency: int = 1):
    """Main processing function using optimized SQL-based logic."""
    try:
        logger.info(f"Starting yard recommendations processing for carrier: {carrier_id}")
        
        analyzer = EventPatternAnalyzer()
        
        # Generate recommendations using optimized SQL query
        all_recommendations = await analyzer.generate_recommendations_sql(carrier_id, min_frequency)
        
        logger.info(f"Total recommendations generated: {len(all_recommendations)}")
        
        if save_to_db and all_recommendations:
            await analyzer.save_recommendations(all_recommendations, carrier_id)
        else:
            logger.info("Skipping database save as requested or no recommendations were generated.")
            
        logger.info("Process completed successfully!")
        return {"yard_recommendations_counts": len(all_recommendations), "yard_recommendations": all_recommendations}
        
    except Exception as e:
        logger.error(f"Process failed with error: {e}", exc_info=True)
        raise