import pytz
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from app.services.common_service import get_time_zone
from app.postgres_services.schedule_plan_service import get_scheduled_plan
from app.mongo_services.load_service import get_active_loads
from app.postgres_services.store_schedule_input import get_latest_input_by_referenceNo_from_db

logger = logging.getLogger(__name__)

# Verify scheduled plan
async def verify_scheduled_plan(carrier: str, plan_date: str) -> Dict[str, Any]:
    """
    Verify the scheduled plan with the original events and return the delta
    """
    try:
        try:
            datetime.strptime(plan_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Invalid plan_date format. Expected YYYY-MM-DD")
        
        timeZone = await get_time_zone(carrier)
        
        # get scheduled plan from db
        converted_plan_date = pytz.timezone(timeZone).localize(datetime.strptime(plan_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0))
        scheduled_plan = await get_scheduled_plan(carrier, converted_plan_date)
        if not scheduled_plan:
            raise Exception(f"No scheduled plan found for carrier {carrier} on {plan_date}")

        appointment_from_time = converted_plan_date
        appointment_to_time = converted_plan_date + timedelta(days=1)

        # get loads with appointment times
        loads = await get_active_loads(carrier, 2000)
        pickup_loads = [
            load['reference_number'] 
            for load in loads 
            if load.get('driverOrder') and any(
                event.get('type') == 'PULLCONTAINER' 
                and event.get('departed')
                and datetime.fromisoformat(event.get('departed')) >= appointment_from_time 
                and datetime.fromisoformat(event.get('departed')) < appointment_to_time 
                for event in load['driverOrder']
            )
        ]
        delivery_loads = [
            load['reference_number'] for load in loads 
            if load.get('driverOrder') 
            and any(
                event.get('type') == 'DELIVERLOAD' 
                and event.get('departed')
                and datetime.fromisoformat(event.get('departed')) >= appointment_from_time 
                and datetime.fromisoformat(event.get('departed')) < appointment_to_time 
                for event in load['driverOrder']
            )
        ]

        return_loads = [
            load['reference_number'] for load in loads
            if load.get('driverOrder') 
            and any(
                event.get('type') == 'RETURNCONTAINER' 
                and event.get('departed')
                and datetime.fromisoformat(event.get('departed')) >= appointment_from_time 
                and datetime.fromisoformat(event.get('departed')) < appointment_to_time 
                for event in load['driverOrder']
            )
        ]

        recommended_pickup_loads = [load['reference_number'] for load in scheduled_plan if load['profile_type'] == 'PULLCONTAINER']
        recommended_delivery_loads = [load['reference_number'] for load in scheduled_plan if load['profile_type'] == 'DELIVERLOAD'] 
        recommended_return_loads = [load['reference_number'] for load in scheduled_plan if load['profile_type'] == 'RETURNCONTAINER']

        total_recommended_pickup_loads = len(recommended_pickup_loads)
        total_recommended_delivery_loads = len(recommended_delivery_loads)
        total_recommended_return_loads = len(recommended_return_loads)

        total_scheduled_pickup_loads = len(pickup_loads)
        total_scheduled_delivery_loads = len(delivery_loads)
        total_scheduled_return_loads = len(return_loads)

        # Full comparison of scheduled plan and loads
        total_recommended_scheduled_loads = total_recommended_pickup_loads + total_recommended_delivery_loads + total_recommended_return_loads
        total_scheduled_loads = total_scheduled_pickup_loads + total_scheduled_delivery_loads + total_scheduled_return_loads

        # Calculate delta as number of reference numbers present in both scheduled_plan and loads
        delta_pickup_loads = len(set(recommended_pickup_loads) & set(pickup_loads))
        delta_delivery_loads = len(set(recommended_delivery_loads) & set(delivery_loads))
        delta_return_loads = len(set(recommended_return_loads) & set(return_loads))

        # Full report for pickup, delivery and return
        full_pickup_report = []
        full_delivery_report = []
        full_return_report = []
        

        # Extract reference_number from scheduled_plan
        scheduled_plan_reference_numbers = [load['reference_number'] for load in scheduled_plan]

        # Extract reference_number from loads
        loads_reference_numbers = [load['reference_number'] for load in loads]

        # Combine both arrays and remove duplicates using set
        all_scheduled_plan_reference_numbers = list(set(scheduled_plan_reference_numbers + loads_reference_numbers))

        all_latest_input = await get_latest_input_by_referenceNo_from_db(carrier=carrier, plan_date=plan_date, reference_numbers=all_scheduled_plan_reference_numbers)
        for load in scheduled_plan:
            actual_load = next((l for l in loads if l['reference_number'] == load['reference_number']), None)
                
            event_detail = {
                "reference_number": load['reference_number'],
                "type_of_load": load['type_of_load'],
                "recommended_appointment": load['recommended_appointment_from'],
                "profile_name": load['profile_name'],
                "container_size": load['container_size'],
                "container_type": load['container_type'],
                "container_owner": load['container_owner'],
                "free_return_date": (
                    datetime.fromisoformat(actual_load['lastFreeDay']) if actual_load and isinstance(actual_load.get('lastFreeDay', ''), str) and actual_load.get('lastFreeDay', '') else ''
                ),
                "customer_name": actual_load.get('callerName', '') if actual_load else '',
            }

            if not isinstance(all_latest_input, list):
                all_latest_input = []  

            if all_latest_input and len(all_latest_input) > 0 and actual_load is not None:
                latest_input = next((input for input in all_latest_input if input['reference_number'] == actual_load['reference_number']), None)
                
                if latest_input is not None:
                    considered_in_input = "Yes"
                else:
                    considered_in_input = "No"
            else:
                considered_in_input = "No"


            event_detail['considered_in_input'] = considered_in_input;
            pickup_event = next((event for event in actual_load['driverOrder'] if event.get('type') == 'PULLCONTAINER'), None) if actual_load else None
            delivery_event = next((event for event in actual_load['driverOrder'] if event.get('type') == 'DELIVERLOAD'), None) if actual_load else None
            return_event = next((event for event in actual_load['driverOrder'] if event.get('type') == 'RETURNCONTAINER'), None) if actual_load else None

            if load['profile_type'] == 'PULLCONTAINER':
                if (actual_load and pickup_event
                    and pickup_event.get('departed')
                    and datetime.fromisoformat(pickup_event.get('departed')) >= appointment_from_time 
                    and datetime.fromisoformat(pickup_event.get('departed')) < appointment_to_time):
                    event_detail['actual_event_time'] = datetime.fromisoformat(pickup_event.get('departed'))


                event_detail['actual_appointment_time'] = (
                    datetime.fromisoformat(actual_load['pickupTimes'][0]['pickupFromTime'])
                    if actual_load and isinstance(actual_load.get('pickupTimes'), list)
                    and len(actual_load['pickupTimes']) > 0
                    and 'pickupFromTime' in actual_load['pickupTimes'][0]
                    else ''
                )
                full_pickup_report.append(event_detail)
            
            elif load['profile_type'] == 'DELIVERLOAD':
                if (actual_load and delivery_event
                    and delivery_event.get('departed')
                    and datetime.fromisoformat(delivery_event.get('departed')) >= appointment_from_time 
                    and datetime.fromisoformat(delivery_event.get('departed')) < appointment_to_time):
                    event_detail['actual_event_time'] = datetime.fromisoformat(delivery_event.get('departed'))
                
                event_detail['actual_appointment_time'] = (
                    datetime.fromisoformat(actual_load['deliveryTimes'][0]['deliveryFromTime'])
                    if actual_load and isinstance(actual_load.get('deliveryTimes'), list) 
                        and len(actual_load['deliveryTimes']) > 0 
                        and 'deliveryFromTime' in actual_load['deliveryTimes'][0]
                    else ''
                )
                full_delivery_report.append(event_detail)
            
            elif load['profile_type'] == 'RETURNCONTAINER':
                if (actual_load and return_event
                    and return_event.get('departed')
                    and datetime.fromisoformat(return_event.get('departed')) >= appointment_from_time 
                    and datetime.fromisoformat(return_event.get('departed')) < appointment_to_time):
                    event_detail['actual_event_time'] = datetime.fromisoformat(return_event.get('departed'))
                
                event_detail['actual_appointment_time'] = (
                    datetime.fromisoformat(actual_load['returnFromTime'])
                    if actual_load and 'returnFromTime' in actual_load and actual_load['returnFromTime'] 
                    else ''
                )
                full_return_report.append(event_detail)
        
        for load in loads:
            if load['reference_number'] not in {load['reference_number'] for load in scheduled_plan}:
                event_detail = {
                    "reference_number": load['reference_number'],
                    "type_of_load": load['type_of_load'],
                    "recommended_appointment": None,
                    "container_size": load.get('containerSizeName', ''),
                    "container_type": load.get('containerTypeName', ''),
                    "container_owner": load.get('containerOwnerName', ''),
                    "free_return_date": (
                        datetime.fromisoformat(load.get('lastFreeDay', '')) if isinstance(load.get('lastFreeDay', ''), str) and load.get('lastFreeDay') else ''
                    ),
                    "customer_name": load.get('callerName', '')
                }
                if not isinstance(all_latest_input, list):
                    all_latest_input = []  

                if all_latest_input and len(all_latest_input) > 0 and load is not None:
                    latest_input = next((input for input in all_latest_input if input['reference_number'] == load['reference_number']), None)
                    
                    if latest_input is not None:
                        considered_in_input = "Yes"
                    else:
                        considered_in_input = "No"
                else:
                    considered_in_input = "No"


                event_detail['considered_in_input'] = considered_in_input;

                pickup_event = next((event for event in load['driverOrder'] if event.get('type') == 'PULLCONTAINER'), None)
                delivery_event = next((event for event in load['driverOrder'] if event.get('type') == 'DELIVERLOAD'), None)
                return_event = next((event for event in load['driverOrder'] if event.get('type') == 'RETURNCONTAINER'), None)

                if (pickup_event 
                    and pickup_event.get('departed')
                    and datetime.fromisoformat(pickup_event.get('departed')) >= appointment_from_time 
                    and datetime.fromisoformat(pickup_event.get('departed')) < appointment_to_time):
                    
                    event_detail['actual_event_time'] = datetime.fromisoformat(pickup_event.get('departed'))
                    event_detail['profile_name'] = load['shipperName'];
                    event_detail['actual_appointment_time'] = (
                        datetime.fromisoformat(load['pickupTimes'][0].get('pickupFromTime', '')) 
                        if load and load.get('pickupTimes') and load['pickupTimes'][0].get('pickupFromTime') 
                        else ''
                    )

                    full_pickup_report.append(event_detail)
                
                if (delivery_event
                      and delivery_event.get('departed')
                      and datetime.fromisoformat(delivery_event.get('departed')) >= appointment_from_time 
                      and datetime.fromisoformat(delivery_event.get('departed')) < appointment_to_time):
                    
                    event_detail['actual_event_time'] = datetime.fromisoformat(delivery_event.get('departed'))
                    event_detail['profile_name'] = load['consigneeName'];
                    event_detail['actual_appointment_time'] = (
                        datetime.fromisoformat(load['deliveryTimes'][0].get('deliveryFromTime', ''))
                        if load and load.get('deliveryTimes') and load['deliveryTimes'][0].get('deliveryFromTime') 
                        else ''
                    )
                    full_delivery_report.append(event_detail)
                
                if (return_event
                      and return_event.get('departed')
                      and datetime.fromisoformat(return_event.get('departed')) >= appointment_from_time 
                      and datetime.fromisoformat(return_event.get('departed')) < appointment_to_time):
                    
                    event_detail['actual_event_time'] = datetime.fromisoformat(return_event.get('departed'))
                    event_detail['profile_name'] = load.get('emptyOriginName', '');
                    event_detail['actual_appointment_time'] = (
                        datetime.fromisoformat(load['returnFromTime']) 
                        if load and 'returnFromTime' in load and load['returnFromTime'] 
                        else ''
                    )
                    full_return_report.append(event_detail)

        # profile name wise report
        profile_wise_report = {}
        
        # Get unique profile names from all reports
        unique_profile_names = list(set(
            [event['profile_name'] for event in full_pickup_report if 'profile_name' in event] +
            [event['profile_name'] for event in full_delivery_report if 'profile_name' in event] +
            [event['profile_name'] for event in full_return_report if 'profile_name' in event]
        ))

        for profile_name in unique_profile_names:
            pickup_events = [event for event in full_pickup_report if event['profile_name'] == profile_name]
            delivery_events = [event for event in full_delivery_report if event['profile_name'] == profile_name]
            return_events = [event for event in full_return_report if event['profile_name'] == profile_name]

            profile_wise_report[profile_name] = {
                "recommended_loads": len([event for event in pickup_events + delivery_events + return_events if event.get('recommended_appointment')]),
                "scheduled_loads": len([event for event in pickup_events + delivery_events + return_events if event.get('actual_event_time')]),
                "delta_loads": len([event for event in pickup_events + delivery_events + return_events if event.get('actual_event_time') and event.get('recommended_appointment')])
            }


        # Generate CSV content
        csv_content = "Load Scheduling Summary Report\n\n"
        csv_content += "Summary Metrics\n"
        csv_content += f"Total Recommended Scheduled Moves,{total_recommended_scheduled_loads}\n"
        csv_content += f"Total Scheduled Moves,{total_scheduled_loads}\n\n"
        csv_content += f"Total Recommended Pickup Loads,{total_recommended_pickup_loads}\n"
        csv_content += f"Total Recommended Delivery Loads,{total_recommended_delivery_loads}\n"
        csv_content += f"Total Recommended Return Loads,{total_recommended_return_loads}\n\n"
        csv_content += f"Total Pickup Loads,{total_scheduled_pickup_loads}\n"
        csv_content += f"Total Delivery Loads,{total_scheduled_delivery_loads}\n"
        csv_content += f"Total Return Loads,{total_scheduled_return_loads}\n\n"
        csv_content += f"Success Pickup Loads,{delta_pickup_loads}\n"
        csv_content += f"Success Delivery Loads,{delta_delivery_loads}\n"
        csv_content += f"Success Return Loads,{delta_return_loads}\n\n"

        # Add pickup report
        csv_content += "\nPickup Report\n"
        csv_content += "Reference Number,Load Type,Profile Name,Customer Name,Recommended Appointment,Actual Event Time,Actual Appointment Time,LFD,Container Size,Container Type,Container Owner,Considered In Input\n"
        for event in full_pickup_report:
            recommended = event.get('recommended_appointment','')
            actual = event.get('actual_event_time','')
            actual_appointment_time = event.get('actual_appointment_time','');
            considered_in_input = event.get('considered_in_input', 'No')
            lfd = event.get('free_return_date','');
            if recommended:
                recommended = recommended.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual:
                actual = actual.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual_appointment_time:
                actual_appointment_time = actual_appointment_time.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if lfd:
                lfd = lfd.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            customer_name = event.get('customer_name', '').replace(',', '').replace('"', '')  # Remove commas from callerName
            csv_content += f"{event['reference_number']},{event['type_of_load']},\"{event.get('profile_name','')}\",\"{customer_name}\",{recommended},{actual},{actual_appointment_time},{lfd},\"{event.get('container_size','')}\",\"{event.get('container_type','')}\",\"{event.get('container_owner','')}\",\"{considered_in_input}\"\n"

        # Add delivery report  
        csv_content += "\nDelivery Report\n"
        csv_content += "Reference Number,Load Type,Profile Name,Customer Name,Recommended Appointment,Actual Event Time,Actual Appointment Time,LFD,Container Size,Container Type,Container Owner,Considered In Input\n"
        for event in full_delivery_report:
            recommended = event.get('recommended_appointment','')
            actual = event.get('actual_event_time','')
            considered_in_input = event.get('considered_in_input', 'No')
            actual_appointment_time = event.get('actual_appointment_time','');
            lfd = event.get('free_return_date','');
            if recommended:
                recommended = recommended.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual:
                actual = actual.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual_appointment_time:
                actual_appointment_time = actual_appointment_time.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if lfd:
                lfd = lfd.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            customer_name = event.get('customer_name', '').replace(',', '').replace('"', '') 
            csv_content += f"{event['reference_number']},{event['type_of_load']},\"{event.get('profile_name','')}\",\"{customer_name}\",{recommended},{actual},{actual_appointment_time},{lfd},\"{event.get('container_size','')}\",\"{event.get('container_type','')}\",\"{event.get('container_owner','')}\",\"{considered_in_input}\"\n"

        # Add return report
        csv_content += "\nReturn Report\n" 
        csv_content += "Reference Number,Load Type,Profile Name,Customer Name,Recommended Appointment,Actual Event Time,Actual Appointment Time,LFD,Container Size,Container Type,Container Owner,Considered In Input\n"
        for event in full_return_report:
            recommended = event.get('recommended_appointment','')
            actual = event.get('actual_event_time','')
            considered_in_input = event.get('considered_in_input', 'No')
            actual_appointment_time = event.get('actual_appointment_time','');
            lfd = event.get('free_return_date','');
            if recommended:
                recommended = recommended.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual:
                actual = actual.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if actual_appointment_time:
                actual_appointment_time = actual_appointment_time.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            if lfd:
                lfd = lfd.astimezone(pytz.timezone(timeZone)).strftime('%Y-%m-%d %I:%M %p')
            customer_name = event.get('customer_name', '').replace(',', '').replace('"', '') 
            csv_content += f"{event['reference_number']},{event['type_of_load']},\"{event.get('profile_name','')}\",\"{customer_name}\",{recommended},{actual},{actual_appointment_time},{lfd},\"{event.get('container_size','')}\",\"{event.get('container_type','')}\",\"{event.get('container_owner','')}\",\"{considered_in_input}\"\n"

        # Add profile wise report
        csv_content += "\nProfile Wise Report\n"
        csv_content += "Profile Name,Recommended Loads,Scheduled Loads,Success Loads\n"
        for profile_name, report in profile_wise_report.items():
            csv_content += f"\"{profile_name}\",{report['recommended_loads']},{report['scheduled_loads']},{report['delta_loads']}\n"

        return {
            "carrier": carrier,
            "plan_date": plan_date,
            "csv_content": csv_content
        }
    
    except Exception as e:
        logger.error(f"Error verifying scheduled plan: {str(e)}")
        raise Exception(f"Failed to verify scheduled plan: {str(e)}")