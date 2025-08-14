import pandas as pd
import json
import os
import smtplib
import asyncio
import logging
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from sqlalchemy import create_engine
from typing import Dict, Any, Tuple
from app.postgres_connection import PostgresConnection
from app.synced_db_connection import PostgresConnection as SyncedPostgresConnection
from geopy.distance import geodesic
import io
from datetime import datetime
import xlsxwriter
from settings import settings
from app.modules.optimizer.constants import DISTANCE_MULTIPLIER

# Configure module logger
logger = logging.getLogger(__name__)

async def generate_sanity_check_data(plan_date: str, version: int, carrier: str) -> Tuple[Dict[str, pd.DataFrame], str]:
    """
    Generate sanity check data and return it as a dictionary of DataFrames.
    
    Args:
        plan_date (str): The plan date in 'YYYY-MM-DD' format
        version (int): The version number
        carrier (str): The carrier ID
        
    Returns:
        Tuple[Dict[str, pd.DataFrame], str]: Dictionary of DataFrames containing sanity check data and the Excel file path
    """
    try:
        logger.info(f"Starting sanity check data generation for carrier {carrier}, plan_date {plan_date}, version {version}")
        
        # --- PARAMETERS ---
        try:
            # Convert string date to datetime object
            plan_date_dt = datetime.strptime(plan_date, '%Y-%m-%d')
            ref_date_minus_14 = (plan_date_dt - pd.Timedelta(days=14)).strftime('%Y-%m-%d')
            logger.info(f"Calculated ref_date_minus_14: {ref_date_minus_14}")
        except Exception as e:
            logger.error(f"Error calculating ref_date_minus_14: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        dfs = {}
        
        # --- CREATE DATABASE CONNECTION ---
        try:
            postgres = PostgresConnection()
            pool = await postgres.get_pool()
            logger.info("Successfully created postgres connection pool")
        except Exception as e:
            logger.error(f"Error creating postgres connection: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        try:
            synced_postgres = SyncedPostgresConnection()
            synced_pool = await synced_postgres.get_pool()
            logger.info("Successfully created synced postgres connection pool")
        except Exception as e:
            logger.error(f"Error creating synced postgres connection: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        # Get scheduled loads
        try:
            query_scheduled = """
            SELECT *
            FROM optimizer_loads
            where carrier = $1 AND plan_date::DATE = $2::DATE AND version = $3 and is_deleted = false;
            """
            logger.info(f"Executing scheduled loads query with params: carrier={carrier}, plan_date={plan_date}, version={version}")
            async with pool.acquire() as conn:
                # schedule_df = pd.DataFrame(await conn.fetch(query_scheduled, carrier, plan_date_dt, version))
                rows = await conn.fetch(query_scheduled, carrier, plan_date_dt, version)
                column_names = rows[0].keys() if rows else []
                schedule_df = pd.DataFrame(rows, columns=column_names)
                
            logger.info(f"Successfully fetched {len(schedule_df)} scheduled loads")
        except Exception as e:
            logger.error(f"Error fetching scheduled loads: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            refs = schedule_df['reference_number'].to_list()
            refs = tuple(refs) if refs else ('dummy_ref',)
            logger.info(f"Processed {len(refs)} reference numbers")
        except Exception as e:
            logger.error(f"Error processing reference numbers: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        # Get unscheduled loads
        try:
            query_unscheduled = """
            SELECT reference_number
            FROM optimizer_loads
            where carrier = $1 AND plan_date::DATE = $2::DATE AND version = $3 and is_deleted = true;
            """
            logger.info(f"Executing unscheduled loads query with params: carrier={carrier}, plan_date={plan_date}, version={version}")
            async with pool.acquire() as conn:
                rows = await conn.fetch(query_unscheduled, carrier, plan_date_dt, version)
                column_names = rows[0].keys() if rows else []
                delete_schedule_df = pd.DataFrame(rows, columns=column_names)
            logger.info(f"Successfully fetched {len(delete_schedule_df)} unscheduled loads")
        except Exception as e:
            logger.error(f"Error fetching unscheduled loads: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            delete_refs = delete_schedule_df['reference_number'].to_list()
            delete_refs = tuple(delete_refs) if delete_refs else ('dummy_ref',)
            logger.info(f"Processed {len(delete_refs)} delete reference numbers")
        except Exception as e:
            logger.error(f"Error processing delete reference numbers: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        # Load appointment times
        try:
            with open('app/data/appointment_times.json', 'r') as f:
                APPOINTMENT_TIMES = json.load(f)
            appt = APPOINTMENT_TIMES
            port_codes = set([port['portCode'] for port in APPOINTMENT_TIMES if 'mandatory_appt' in port])
            logger.info(f"Successfully loaded appointment times with {len(port_codes)} port codes")
        except Exception as e:
            logger.error(f"Error loading appointment times: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        # Returns not in the plan
        try:
            query_returns = """
            SELECT reference_number, readytoreturndate date, returnfromtime, emptyoriginname
            FROM loads
            WHERE carrier = $1 AND type_of_load = 'IMPORT' AND readytoreturndate::DATE <= $2::DATE
            AND readytoreturndate::DATE > $3::DATE AND actual_delivery_date is not null
            and emptyoriginname is not null and emptyoriginname != 'TBD ***MESSAGE DISPATCH***'
            AND status = 'DROPCONTAINER_DEPARTED' AND returnfromtime is null 
            AND reference_number != ALL($4) AND reference_number != ALL($5)

            UNION ALL

            SELECT reference_number, cutoff date, returnfromtime, emptyoriginname
            FROM loads
            WHERE carrier = $1 AND type_of_load = 'EXPORT' AND cutoff::DATE = $2::DATE AND actual_delivery_date is not null 
            and emptyoriginname is not null and emptyoriginname != 'TBD ***MESSAGE DISPATCH***'
            AND status = 'DROPCONTAINER_DEPARTED' AND returnfromtime is null 
            AND reference_number != ALL($4) AND reference_number != ALL($5)
            ORDER BY date DESC;
            """
            logger.info(f"Executing returns query with params: carrier={carrier}, plan_date={plan_date}, ref_date_minus_14={ref_date_minus_14}")
            async with synced_pool.acquire() as conn:
                rows = await conn.fetch(
                    query_returns,
                    carrier,
                    plan_date_dt,
                    datetime.strptime(ref_date_minus_14, '%Y-%m-%d'),
                    tuple(refs),
                    tuple(delete_refs)
                )
                column_names = rows[0].keys() if rows else []
                missing_return = pd.DataFrame(rows, columns=column_names)
            logger.info(f"Successfully fetched {len(missing_return)} returns not in plan")
            dfs['Returns not in the plan'] = missing_return
        except Exception as e:
            logger.error(f"Error fetching returns not in plan: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        # Moves currently in progress
        moves = schedule_df['move_id'].to_list()
        moves = tuple(moves) if moves else ('dummy_move',)

        query_progress = """
        SELECT reference_number, enroute
        FROM events
        WHERE carrier = $1 AND moveid = ANY($2) and enroute is not null
        order by enroute DESC;
        """
        async with synced_pool.acquire() as conn:
            rows = await conn.fetch(query_progress, carrier, moves)
            column_names = rows[0].keys() if rows else []
            in_progress = pd.DataFrame(rows, columns=column_names)

        dfs['Moves currently in progress'] = in_progress

        # Assumptive moves - Logic check for container availability
        # Extract move types for each reference_number
        def extract_types(moves):
            # moves is a list of dicts, each with a 'type' key
            if isinstance(moves, list):
                return list({m.get('type') for m in moves if m.get('type') in {'PULLCONTAINER', 'DELIVERLOAD', 'RETURNCONTAINER'}})
            return []
        # Convert move data from string to list for all rows
        if isinstance(schedule_df['move'][0], str):
            try:
                # Apply json.loads to all string values in the 'move' column
                schedule_df['move'] = schedule_df['move'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
            except Exception as e:
                logger.error(f"Error converting move data to list: {str(e)}")
        else:
            print(type(schedule_df['move'][0]))
        # print(schedule_df['move'][0])
        move_types_df = pd.DataFrame()

        if not schedule_df.empty:
            move_types_df = (
                schedule_df
                .groupby('reference_number')['move']
                .apply(lambda move_lists: sum(move_lists if isinstance(move_lists.iloc[0], list) else [move_lists], []))
                .apply(extract_types)
                .reset_index()
                .rename(columns={'move': 'types'})
            )
        logger.info("Extracted move types")

        assumptive_moves_list = []
        # Check container availability based on move types
        for row in move_types_df.iterrows():
            if 'PULLCONTAINER' in row[1]['types']:
                query = """
                SELECT reference_number, status
                FROM loads
                WHERE carrier = $1 AND reference_number = $2 AND status != 'AVAILABLE';
                """
                async with synced_pool.acquire() as conn:
                    rows = await conn.fetch(query, carrier, row[1]['reference_number'])
                    column_names = rows[0].keys() if rows else []
                    pull_moves = pd.DataFrame(rows, columns=column_names)
                    assumptive_moves_list.append(pull_moves)

            elif 'PULLCONTAINER' not in row[1]['types'] and 'DELIVERLOAD' in row[1]['types']:
                query = """
                SELECT reference_number, status
                FROM loads
                WHERE carrier = $1 AND reference_number = $2 AND actual_pickup_date is null;
                """
                async with synced_pool.acquire() as conn:
                    rows = await conn.fetch(query, carrier, row[1]['reference_number'])
                    column_names = rows[0].keys() if rows else []
                    pull_deliver_moves = pd.DataFrame(rows, columns=column_names)
                    assumptive_moves_list.append(pull_deliver_moves)

            elif 'DELIVERLOAD' not in row[1]['types'] and 'RETURNCONTAINER' in row[1]['types']:
                query = """
                SELECT reference_number, status
                FROM loads
                WHERE carrier = $1 AND reference_number = $2 AND actual_delivery_date is null;
                """
                async with synced_pool.acquire() as conn:
                    rows = await conn.fetch(query, carrier, row[1]['reference_number'])
                    column_names = rows[0].keys() if rows else []
                    deliver_return_moves = pd.DataFrame(rows, columns=column_names)
                    assumptive_moves_list.append(deliver_return_moves)
        if assumptive_moves_list:
            assumptive_moves = pd.concat(assumptive_moves_list, ignore_index=True)
        else:
            assumptive_moves = pd.DataFrame()

        dfs['Moves assuming a previous move was done'] = assumptive_moves
        logger.info("Checked container availability")

        missing_appt_list = []

        # If an appointment is required, check if there is an appointment
        for row in schedule_df.iterrows():
            if not row[1]['is_deleted']:
                query = """
                SELECT company_name, type
                FROM events
                WHERE carrier = $1 AND moveid = $2 AND reference_number = $3;
                """
                async with synced_pool.acquire() as conn:
                    rows = await conn.fetch(query, carrier, row[1]['move_id'], row[1]['reference_number'])
                    column_names = rows[0].keys() if rows else []
                    results = pd.DataFrame(rows, columns=column_names)

                if not results.empty:
                    company_name = results.iloc[0]['company_name']
                    move_type = results.iloc[0]['type']
                    for app in appt:
                        if app['portCode'] == company_name and 'mandatory_appt' in app:
                            if move_type == 'PULLCONTAINER':
                                query = """
                                SELECT reference_number
                                FROM loads
                                WHERE carrier = $1 AND reference_number = $2 and pickuptimes_0_pickupfromtime is null and isdeleted = false;
                                """
                                async with synced_pool.acquire() as conn:
                                    results2 = pd.DataFrame(await conn.fetch(query, carrier, row[1]['reference_number']))
                                    missing_appt_list.append(results2)

                            elif move_type == 'DELIVERLOAD':
                                query = """
                                SELECT reference_number
                                FROM loads
                                WHERE carrier = $1 AND reference_number = $2 and deliverytimes_0_deliveryfromtime is null and isdeleted = false;
                                """
                                async with synced_pool.acquire() as conn:
                                    results2 = pd.DataFrame(await conn.fetch(query, carrier, row[1]['reference_number']))
                                    missing_appt_list.append(results2)
        if missing_appt_list:
            missing_appt = pd.concat(missing_appt_list, ignore_index=True)
        else:
            missing_appt = pd.DataFrame()

        dfs['Missing appointments'] = missing_appt
        logger.info("Checked appointment requirements")

        # Check for exports not returned between ERD and CUT
        erd_cutoff_list = []
        for row in schedule_df.iterrows():
            if not row[1]['is_deleted']:
                query = """
                SELECT reference_number, containeravailableday, cutoff
                FROM loads
                WHERE carrier = $1 AND reference_number = $2 AND type_of_load = 'EXPORT' 
                AND (cutoff::DATE < $3 OR containeravailableday::DATE > $3)
                AND isdeleted = false;
                """
                async with synced_pool.acquire() as conn:
                    rows = await conn.fetch(
                        query,
                        carrier,
                        row[1]['reference_number'],
                        plan_date_dt
                    )
                    column_names = rows[0].keys() if rows else []
                    results = pd.DataFrame(rows, columns=column_names)
                    erd_cutoff_list.append(results)
        
        if erd_cutoff_list:
            erd_cutoff_df = pd.concat(erd_cutoff_list, ignore_index=True)
        else:
            erd_cutoff_df = pd.DataFrame()
        
        dfs['Exports not returned between ERD and CUT'] = erd_cutoff_df
        logger.info("Checked exports for ERD/CUT violations")

        def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float, unit = 'mi') -> float:
            """Calculate the distance in miles between two geographic coordinates.

            Args:
                lat1: Latitude of the first location
                lng1: Longitude of the first location
                lat2: Latitude of the second location
                lng2: Longitude of the second location
                unit: Unit of distance to return (mi or km)

            Returns:
                float: Distance in miles between the coordinates

            Raises:
                ValueError: If any coordinate is invalid
            """
            try:
                if unit == 'km':
                    return geodesic((lat1, lng1), (lat2, lng2)).kilometers * DISTANCE_MULTIPLIER
                return geodesic((lat1, lng1), (lat2, lng2)).miles * DISTANCE_MULTIPLIER

            except ValueError as e:
                raise ValueError(f"Invalid coordinates provided: {e}") from e
        
        # Group by driver and sort by move_start_time
        multi_move_drivers = pd.DataFrame()
        if not schedule_df.empty:
            multi_move_drivers = schedule_df.groupby('driver').filter(lambda x: len(x) > 1)
            multi_move_drivers = multi_move_drivers.sort_values(['driver', 'move_start_time'])

        # For each driver with more than one move, get previous move's last location and next move's start location
        result = []
        if not multi_move_drivers.empty:
            for driver, group in multi_move_drivers.groupby('driver'):
                group = group.sort_values('move_start_time')
                for i in range(1, len(group)):
                    prev_row = group.iloc[i-1]
                    next_row = group.iloc[i]
                    # Extract last location of previous move and start location of next move
                    # Assuming 'move' column is a list of dicts with 'city' or 'location' keys
                    def get_last_location_info(move):
                        if isinstance(move, list) and move:
                            last = move[-1]
                            return {
                                'city': last.get('city') or last.get('location'),
                                'company_name': last.get('company_name')
                            }
                        return {'city': None, 'company_name': None}

                    def get_start_location_info(move):
                        if isinstance(move, list) and move:
                            first = move[0]
                            return {
                                'city': first.get('city') or first.get('location'),
                                'company_name': first.get('company_name')
                            }
                        return {'city': None, 'company_name': None}

                    prev_last_loc = get_last_location_info(prev_row['move'])
                    next_start_loc = get_start_location_info(next_row['move'])
                    # Calculate distance between last location of previous move and start location of next move using address lat/lng
                    distance = None
                    if prev_last_loc and next_start_loc:
                        try:
                            # Extract lat/lng from the address field of each move
                            def get_lat_lng_from_address(move, idx):
                                if isinstance(move, list) and move and 'address' in move[idx]:
                                    address = move[idx]['address']
                                    lat = address.get('lat')
                                    lng = address.get('lng')
                                    return lat, lng
                                return None, None

                            prev_lat, prev_lng = get_lat_lng_from_address(prev_row['move'], -1)
                            next_lat, next_lng = get_lat_lng_from_address(next_row['move'], 0)

                            if None not in (prev_lat, prev_lng, next_lat, next_lng):
                                distance = calculate_distance(prev_lat, prev_lng, next_lat, next_lng)
                        except Exception as e:
                            logger.error(f"Error calculating distance for driver {driver}: {e}")
                    result.append({
                        'driver': driver,
                        'prev_move_reference': prev_row['reference_number'],
                        'next_move_reference': next_row['reference_number'],
                        'prev_last_location': [prev_last_loc['city'], prev_last_loc['company_name']],
                        'next_start_location': [next_start_loc['city'], next_start_loc['company_name']],
                        'distance_miles': distance
                    })
        dual_trans_df = pd.DataFrame(result)
        dfs['Dual transactions'] = dual_trans_df
        logger.info("Checked for dual transactions and calculated distances")

        # --- PARAMETERIZED QUERIES ---
        queries_1 = {
            "Missing loads with LFDs": """
                SELECT siv.reference_number
                FROM schedule_input_v2 AS siv
                WHERE DATE(siv."lastFreeDay") = $1
                  AND DATE(siv.plan_date) = $1
                  AND siv.status = 'AVAILABLE'
                  AND carrier = $2
                  AND siv.reference_number NOT IN (
                      SELECT spv.reference_number
                      FROM schedule_plan_v2 AS spv
                      WHERE spv.carrier = $2
                        AND DATE(spv.plan_date) = $1
                  );
            """,
            "Missing loads with appts": """
                SELECT siv.reference_number
                FROM schedule_input_v2 AS siv
                WHERE DATE(siv.plan_date) = $1
                AND carrier = $2
                  AND (
                      DATE(siv."pickupFromTime") = $1 OR
                      DATE(siv."deliveryFromTime") = $1 OR
                      DATE(siv."returnFromTime") = $1
                  )
                  AND siv.status = 'AVAILABLE'
                  AND siv.reference_number NOT IN (
                      SELECT spv.reference_number
                      FROM schedule_plan_v2 AS spv
                      WHERE spv.carrier = $2
                        AND DATE(spv.plan_date) = $1
                  );
            """,
            "Planned Loads": """
                SELECT *
                FROM optimizer_loads
                where carrier = $1 AND plan_date::DATE = $2 AND version = $3 and is_deleted = false;
            """,
            "Unplanned Loads": """
                SELECT *
                FROM optimizer_loads
                where carrier = $1 AND plan_date::DATE = $2 AND version = $3 and is_deleted = true;
            """
        }

        # --- FETCH DATA INTO DATAFRAMES ---
        async with pool.acquire() as conn:
            for sheet_name, query in queries_1.items():
                if sheet_name in ["Missing loads with LFDs", "Missing loads with appts"]:
                    rows = await conn.fetch(query, plan_date_dt, carrier)
                    column_names = rows[0].keys() if rows else []
                    df = pd.DataFrame(rows, columns=column_names)
                else:  # "Planned Loads" and "Unplanned Loads" need 3 parameters
                    rows = await conn.fetch(query, carrier, plan_date_dt, version)
                    column_names = rows[0].keys() if rows else []
                    df = pd.DataFrame(rows, columns=column_names)
                logger.info(f"Fetched {len(df)} rows for '{sheet_name}'")
                dfs[sheet_name] = df
        
        # Create an Excel file in memory instead of saving to disk
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            for name, df in dfs.items():
                sheet_name = name[:31]  # Excel sheet names have a 31 character limit
                # Remove timezone information from datetime columns to avoid Excel error
                df_copy = df.copy()
                for col in df_copy.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64']).columns:
                    df_copy[col] = df_copy[col].dt.tz_localize(None)
                df_copy.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Get the Excel data from the buffer
        excel_buffer.seek(0)
        excel_data = excel_buffer.getvalue()
        carrier_map = {
            '63039f613d347315e2a02a2d': 'TriPoint',
            '653a6813f7eb901615236816': 'QualityContainer',
            '641a10875b159a160742327e': 'RoadEx',
            '6478bad770a34316adb76c24': 'AlphaCargo',
            '623a1a0ae85bec6eacd5096d': 'DileTrucking',
            '5a39472b4a819b31e9496084': 'Loyalty'
        }
        carrier_name = carrier_map.get(carrier, carrier)
        
        excel_filename = f"sanity_check_{carrier_name}_{plan_date}_{version}.xlsx"
        logger.info(f"Generated in-memory Excel file: {excel_filename}")
        return dfs, excel_data, excel_filename

    except Exception as e:
        logger.error(f"Failed to generate sanity check data: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(f"Failed to generate sanity check data: {str(e)}")

async def send_email(excel_data: bytes, excel_filename: str, plan_date: str, carrier: str) -> None:
    """
    Send an email with the sanity check report as an attachment.
    
    Args:
        excel_data (bytes): The Excel file data as bytes
        excel_filename (str): The filename for the Excel attachment
        plan_date (str): The plan date in 'YYYY-MM-DD' format
        carrier_name (str): The name of the carrier
    """
    try:
        logger.info(f"Starting email send for carrier {carrier}, plan_date {plan_date}")
        
        # SMTP Server Configuration from environment variables
        SMTP_SERVER = settings.SMTP_SERVER
        SMTP_PORT = settings.SMTP_PORT
        MANDRILL_USER = settings.MANDRILL_USER
        MANDRILL_PASSWORD = settings.MANDRILL_PASSWORD

        if not all([SMTP_SERVER, SMTP_PORT, MANDRILL_USER, MANDRILL_PASSWORD]):
            raise ValueError("Missing required email configuration. Please set SMTP_SERVER, SMTP_PORT, MANDRILL_USER, and MANDRILL_PASSWORD in environment variables.")

        TO_RECIPIENTS = ["ingrid@portpro.io"]
        CC_RECIPIENTS = ["jemish.dungarani@portpro.io","ali@portpro.io","david.liu@portpro.io","binayak@portpro.io","ryan@portpro.io","shelby@portpro.io","corey@portpro.io"]

        # Map carrier IDs to readable names
        carrier_map = {
            '63039f613d347315e2a02a2d': 'TriPoint',
            '653a6813f7eb901615236816': 'QualityContainer',
            '641a10875b159a160742327e': 'RoadEx',
            '6478bad770a34316adb76c24': 'AlphaCargo',
            '623a1a0ae85bec6eacd5096d': 'DileTrucking',
            '5a39472b4a819b31e9496084': 'Loyalty'
        }
        
        # Use the carrier map to get the readable name, or use the original ID if not found
        carrier_name = carrier_map.get(carrier, carrier)
        try:
            # Create the email message
            msg = MIMEMultipart()
            msg['From'] = MANDRILL_USER
            msg['To'] = ", ".join(TO_RECIPIENTS)
            msg['CC'] = ", ".join(CC_RECIPIENTS)
            msg['Subject'] = f"Sanity Check Report - {carrier_name} - {plan_date}"
            logger.info("Successfully created email message")
        except Exception as e:
            logger.error(f"Error creating email message: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        body = f"""
Dear Team,

Please find the attached Sanity Check Report for {carrier_name} for plan date {plan_date}.
This is an automatically generated report. If you have any questions or need further clarification, please reach out to us.

Best regards,  
Dispatch Optimization Team
"""

        try:
            # Attach the body to the email
            msg.attach(MIMEText(body, 'plain'))
            logger.info("Successfully attached email body")
        except Exception as e:
            logger.error(f"Error attaching email body: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            # Attach the Excel file from memory
            part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            part.set_payload(excel_data)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{excel_filename}"')
            msg.attach(part)
            logger.info("Successfully attached Excel file")
        except Exception as e:
            logger.error(f"Error attaching Excel file: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        all_recipients = TO_RECIPIENTS + CC_RECIPIENTS

        try:
            # Connect to the SMTP server
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.starttls()  # Secure the connection
            server.login(MANDRILL_USER, MANDRILL_PASSWORD)
            logger.info("Successfully connected to SMTP server")
        except Exception as e:
            logger.error(f"Error connecting to SMTP server: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        try:
            # Send the email
            server.sendmail(MANDRILL_USER, all_recipients, msg.as_string())
            server.quit()
            logger.info("Successfully sent email")
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise Exception(f"Failed to send email: {str(e)}")

async def run_sanity_check_automation(plan_date: str, version: int, carrier: str) -> None:
    """
    Run sanity check in the background.
    
    Args:
        plan_date (str): The plan date in 'YYYY-MM-DD' format
        version (int): The version number
        carrier (str): The carrier ID
    """
    # Skip sanity check for specific carrier
    if carrier == "67bd6724758c40d7df2ac360" or carrier == "6786a36700956c1a792163f5":
        logger.info(f"Skipping sanity check automation for carrier {carrier} because it is a test carrier")
        return
    
    try:
        logger.info(f"Starting sanity check automation for carrier {carrier}, plan_date {plan_date}, version {version}")
        dfs, excel_data, excel_filename = await generate_sanity_check_data(plan_date, version, carrier)
        await send_email(excel_data, excel_filename, plan_date, carrier)
        logger.info("Successfully completed sanity check automation")
    except Exception as e:
        logger.error(f"Failed to run sanity check in background: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")