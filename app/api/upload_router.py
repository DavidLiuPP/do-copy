from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import JSONResponse
import logging
import csv
import json
from io import StringIO
from app.modules.upload_model.upload_file import file_upload
from app.synced_write_db_connection import get_direct_db_connection
from datetime import datetime

router = APIRouter()
logger = logging.getLogger(__name__)

file_types = [
    "schedule",
    "appointment"
]

@router.post("/upload_file")
async def upload_file(request: Request):
    """
    Upload a file to S3

    Parameters:
        request (Request): The incoming request containing the file
    """
    try:
        form = await request.form()
        file_type = form.get("file_type")
        
        if file_type not in file_types:
            return JSONResponse(
                content={"message": "Invalid file type"},
                status_code=400
            )

        result = await file_upload(form)

        response = { "result": result, "status": "success" }
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error(e)
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )


@router.post("/upload_driver_features")
async def upload_driver_features(request: Request, file: UploadFile = File(...)):
    """
    Upload driver features from a CSV file

    Parameters:
        file (CSV): The CSV file containing driver features (Required)
    """
    try:
        conn = await get_direct_db_connection()
        try:
            form = await request.form()
            is_delete_existing = form.get("is_delete_existing")

            # Read CSV contents
            content = await file.read()
            decoded = content.decode("utf-8")
            csv_reader = csv.DictReader(StringIO(decoded))


            if not file.filename.endswith(".csv"):
                return JSONResponse(
                    content={"message": "Only CSV files are supported"},
                    status_code=400
                )
            rows = list(csv_reader)

            carrier = None
            for row in rows:
                if carrier is None:
                    carrier = row.get("carrier")
                elif carrier != row.get("carrier"):
                    return JSONResponse(
                        content={"message": "All rows must have the same carrier value"},
                        status_code=400
                    )
                
            if is_delete_existing == 'true' or is_delete_existing == True or is_delete_existing == 'TRUE':
                await conn.execute("DELETE FROM driver_features where carrier = $1", carrier)

            driver_features = []
            error_rows = []
            int_fields = [
                "avg_driver_pay",
                "start_lat",
                "start_lng",
                "end_lat",
                "end_lng",
                "per_mile_pay",
                "owner_score",
                "min_mileage",
                "max_mileage"
            ]   
            for row in rows:
                # Optional: Convert fields to appropriate types
                if row.get("driver_id") is None or row.get("driver_id") == '':
                    row["reason"] = "Missing driver_id"
                    error_rows.append(row)
                    continue

                valid = True
                int_values = {}
                for field in int_fields:
                    value = row.get(field)
                    if value and value != 'NULL':
                        try:
                            int_values[field] = float(value)
                        except (ValueError, TypeError):
                            row["reason"] = f"Invalid integer for {field}"
                            error_rows.append(row)
                            valid = False
                            break
                    else:
                        int_values[field] = None
                if not valid:
                    continue

                if row.get("work_start") and row.get("work_start") != 'NULL':
                    try:
                        datetime.strptime(row.get("work_start"), "%H:%M:%S").time()
                    except ValueError:
                        row["reason"] = "Invalid time format for work_start"
                        error_rows.append(row)
                        continue
                else:
                    row["work_start"] = None

                if row.get("work_end") and row.get("work_end") != 'NULL':
                    try:
                        datetime.strptime(row.get("work_end"), "%H:%M:%S").time()
                    except ValueError:
                        row["reason"] = "Invalid time format for work_end"
                        error_rows.append(row)
                        continue
                else:
                    row["work_end"] = None

                if row.get("depot_location") and row.get("depot_location") != 'NULL':
                    try:
                        parsed_json = json.loads(row.get("depot_location"))
                        if not isinstance(parsed_json, dict):
                            row["reason"] = "depot_location must be a JSON object"
                            error_rows.append(row)
                            continue
                        row["depot_location"] = parsed_json
                    except json.JSONDecodeError:
                        row["reason"] = "Invalid JSON for depot_location"
                        error_rows.append(row)
                        continue
                else:
                    row["depot_location"] = None

                if row.get("whitelisted_warehouses") and row.get("whitelisted_warehouses") != 'NULL':
                    try:
                        row["whitelisted_warehouses"] = json.loads(row.get("whitelisted_warehouses"))
                    except json.JSONDecodeError:
                        row["reason"] = "Invalid JSON for whitelisted_warehouses"
                        error_rows.append(row)
                        continue
                else:
                    row["whitelisted_warehouses"] = None

                driver_features.append({
                    "driver_id": row.get("driver_id"),
                    "carrier": carrier,  # Overriding with token's carrier
                    "working_type": row.get("working_type") if row.get("working_type") and row.get("working_type") != 'NULL' else None,
                    "vacation_sick": row.get("vacation_sick").lower() == 'true' if isinstance(row.get("vacation_sick"), str) else False,
                    "avg_driver_pay": int_values["avg_driver_pay"],
                    "work_start": row.get("work_start") if row.get("work_start") and row.get("work_start") != 'NULL' else None,
                    "work_end": row.get("work_end") if row.get("work_end") and row.get("work_end") != 'NULL' else None,
                    "start_lat": int_values["start_lat"],
                    "start_lng": int_values["start_lng"],
                    "end_lat": int_values["end_lat"],
                    "end_lng": int_values["end_lng"],
                    "per_mile_pay": int_values["per_mile_pay"],
                    "min_mileage": int_values["min_mileage"],
                    "max_mileage": int_values["max_mileage"],
                    "owner_score": int_values["owner_score"],
                    "driver_table_id": row.get("driver_table_id") if row.get("driver_table_id") and row.get("driver_table_id") != 'NULL' else None,
                    "depot_customer_id": row.get("depot_customer_id") if row.get("depot_customer_id") and row.get("depot_customer_id") != 'NULL' else None,
                    "depot_location": row.get("depot_location") if row.get("depot_location") and row.get("depot_location") != 'NULL' else None,
                    "b_train": row.get("b_train").lower() == 'true' if isinstance(row.get("b_train"), str) else False,
                    "whitelisted_warehouses": row["whitelisted_warehouses"]
                  })

            if not driver_features:
                return JSONResponse(
                    content={"message": "No valid data in CSV"},
                    status_code=400
                )

            bulk_insert_values = []
            for feature in driver_features:
                row_values = []
                for value in feature.values():
                    if value is None:
                        formatted_value = 'NULL'
                    elif isinstance(value, (bool, int, float)):
                        formatted_value = str(value)
                    elif isinstance(value, (dict, list)):
                        json_string = json.dumps(value, separators=(',', ':'))
                        escaped_string = json_string.replace("'", "''")
                        formatted_value = f"'{escaped_string}'"
                    else:
                        escaped_string = str(value).replace("'", "''")
                        formatted_value = f"'{escaped_string}'"
                    
                    row_values.append(formatted_value)
                bulk_insert_values.append(f"({', '.join(row_values)})")

            if bulk_insert_values:
                fields = driver_features[0].keys()
                query_part = ', '.join([f'"{field}"' for field in fields])
                
                SQL = f"""
                    INSERT INTO driver_features ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(SQL)

            return JSONResponse(content={"result": "Added successfully", "error_rows": error_rows, "status": "success"}, status_code=200)

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Error uploading driver features: {e}")
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )
    


@router.post("/upload_waiting_time")
async def upload_waiting_time(request: Request, file: UploadFile = File(...)):
    """
    Upload waiting time from a CSV file

    Parameters:
        file (CSV): The CSV file containing waiting time (Required)
    """
    try:
        conn = await get_direct_db_connection()
        try:
            form = await request.form()
            is_delete_existing = form.get("is_delete_existing")

            if not file.filename.endswith(".csv"):
                return JSONResponse(
                    content={"message": "Only CSV files are supported"},
                    status_code=400
                )

            # Read CSV contents
            content = await file.read()
            decoded = content.decode("utf-8")
            csv_reader = csv.DictReader(StringIO(decoded))
            rows = list(csv_reader)

            carrier = None
            for row in rows:
                if carrier is None:
                    carrier = row.get("carrier")
                elif carrier != row.get("carrier"):
                    return JSONResponse(
                        content={"message": "All rows must have the same carrier value"},
                        status_code=400
                    )
           
            if is_delete_existing == 'true' or is_delete_existing == True or is_delete_existing == 'TRUE':
                await conn.execute("DELETE FROM waiting_time where carrier = $1", carrier)

            waiting_time = []
            error_rows = []
            float_fields = [ "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" ]
         
            for row in rows:
                # Optional: Convert fields to appropriate types
                if row.get("customer_id") is None or row.get("customer_id") == '' or row.get("waiting_time") is None or row.get("waiting_time") == '' or row.get("location") is None or row.get("location") == '':
                    row["reason"] = f"Missing customer_id, waiting_time, or location"
                    error_rows.append(row)
                    continue

                try:
                    int(row.get("waiting_time"))
                except ValueError:
                    row["reason"] = "Invalid integer for waiting_time"
                    error_rows.append(row)
                    continue

                valid = True
                float_values = {}
                for field in float_fields:
                    value = row.get(field)
                    if value and value != 'NULL':
                        try:
                            float_values[field] = float(value)
                        except (ValueError, TypeError):
                            row["reason"] = f"Invalid float for {field} field"
                            error_rows.append(row)
                            valid = False
                            break
                    else:
                        float_values[field] = None

                if not valid:
                    continue    
                
                waiting_time.append({
                    "customer_id": row.get("customer_id"),
                    "carrier": row.get("carrier"),
                    "location": row.get("location") if row.get("location") and row.get("location") != 'NULL' else None,
                    "waiting_time": row.get("waiting_time") if row.get("waiting_time") and row.get("waiting_time") != 'NULL' else None,
                    "00": float_values["00"] if float_values["00"] else row.get("00") if row.get("00") and row.get("00") != 'NULL' else None,
                    "01": float_values["01"] if float_values["01"] else row.get("01") if row.get("01") and row.get("01") != 'NULL' else None,
                    "02": float_values["02"] if float_values["02"] else row.get("02") if row.get("02") and row.get("02") != 'NULL' else None,
                    "03": float_values["03"] if float_values["03"] else row.get("03") if row.get("03") and row.get("03") != 'NULL' else None,
                    "04": float_values["04"] if float_values["04"] else row.get("04") if row.get("04") and row.get("04") != 'NULL' else None,
                    "05": float_values["05"] if float_values["05"] else row.get("05") if row.get("05") and row.get("05") != 'NULL' else None,
                    "06": float_values["06"] if float_values["06"] else row.get("06") if row.get("06") and row.get("06") != 'NULL' else None,
                    "07": float_values["07"] if float_values["07"] else row.get("07") if row.get("07") and row.get("07") != 'NULL' else None,
                    "08": float_values["08"] if float_values["08"] else row.get("08") if row.get("08") and row.get("08") != 'NULL' else None,
                    "09": float_values["09"] if float_values["09"] else row.get("09") if row.get("09") and row.get("09") != 'NULL' else None,
                    "10": float_values["10"] if float_values["10"] else row.get("10") if row.get("10") else None,
                    "11": float_values["11"] if float_values["11"] else row.get("11") if row.get("11") else None,
                    "12": float_values["12"] if float_values["12"] else row.get("12") if row.get("12") else None,
                    "13": float_values["13"] if float_values["13"] else row.get("13") if row.get("13") else None,
                    "14": float_values["14"] if float_values["14"] else row.get("14") if row.get("14") else None,
                    "15": float_values["15"] if float_values["15"] else row.get("15") if row.get("15") else None,
                    "16": float_values["16"] if float_values["16"] else row.get("16") if row.get("16") else None,
                    "17": float_values["17"] if float_values["17"] else row.get("17") if row.get("17") else None,
                    "18": float_values["18"] if float_values["18"] else row.get("18") if row.get("18") else None,
                    "19": float_values["19"] if float_values["19"] else row.get("19") if row.get("19") else None,
                    "20": float_values["20"] if float_values["20"] else row.get("20") if row.get("20") else None,
                    "21": float_values["21"] if float_values["21"] else row.get("21") if row.get("21") else None,
                    "22": float_values["22"] if float_values["22"] else row.get("22") if row.get("22") else None,
                    "23": float_values["23"] if float_values["23"] else row.get("23") if row.get("23") else None,
                    "type": row.get("type") if row.get("type") and row.get("type") != 'NULL' else None,
                    "is_liveunload": row.get("is_liveunload").lower() == 'true' if isinstance(row.get("is_liveunload"), str) else False,
                    })

            if not waiting_time:
                return JSONResponse(
                    content={"message": "No valid data in CSV", "error_rows": error_rows},
                    status_code=400
                )

            bulk_insert_values = []
            for feature in waiting_time:
                row_values = []
                for value in feature.values():
                    if value is None:
                        formatted_value = 'NULL'
                    elif isinstance(value, (bool, int, float)):
                        formatted_value = str(value)
                    elif isinstance(value, (dict, list)):
                        json_string = json.dumps(value, separators=(',', ':'))
                        escaped_string = json_string.replace("'", "''")
                        formatted_value = f"'{escaped_string}'"
                    else:
                        escaped_string = str(value).replace("'", "''")
                        formatted_value = f"'{escaped_string}'"
                    
                    row_values.append(formatted_value)
                bulk_insert_values.append(f"({', '.join(row_values)})")

            if bulk_insert_values:
                fields = waiting_time[0].keys()
                query_part = ', '.join([f'"{field}"' for field in fields])
                
                SQL = f"""
                    INSERT INTO waiting_time ({query_part})
                    VALUES {', '.join(bulk_insert_values)}
                """
                await conn.execute(SQL)

            return JSONResponse(content={"result": "Added successfully", "error_rows": error_rows, "status": "success"}, status_code=200)

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Error uploading waiting time: {e}")
        return JSONResponse(
            content={"message": str(e), "status": "error"}, status_code=500
        )