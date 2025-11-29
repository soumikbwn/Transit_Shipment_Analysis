import json
import pandas as pd
import numpy as np
from datetime import datetime

INPUT_FILE = "Swift Assignment 4 - Dataset (2).json"

def parse_timestamp(ts):
    if not ts:
        return None

    if isinstance(ts, dict) and "$numberLong" in ts:
        try:
            return datetime.fromtimestamp(int(ts["$numberLong"]) / 1000)
        except Exception:
            return None

    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None

    return None


def safe_get(dct, path, default=None):
    cur = dct
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return default
    return cur if cur is not None else default


def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    detailed_rows = []

    for shipment in data:
        track_details = shipment.get("trackDetails", [])
        if not track_details:
            continue

        td = track_details[0]

        tracking_no = td.get("trackingNumber")

        service_type = safe_get(td, ["service", "type"])
        carrier = td.get("carrierCode")

        package_weight = safe_get(td, ["packageWeight", "value"])
        packaging = safe_get(td, ["packaging", "type"])

        origin_city = safe_get(td, ["shipperAddress", "city"])
        origin_state = safe_get(td, ["shipperAddress", "stateOrProvinceCode"])

        dest_city = safe_get(td, ["destinationAddress", "city"])
        dest_state = safe_get(td, ["destinationAddress", "stateOrProvinceCode"])

        delivery_location_type = td.get("deliveryLocationType")

        events = td.get("events", [])
        parsed_events = []

        for ev in events:
            ts = parse_timestamp(ev.get("timestamp"))
            parsed_events.append(
                {
                    "eventtype": ev.get("eventType"),
                    "timestamp": ts,
                    "location": ev.get("arrivalLocation"),
                }
            )

        parsed_events = [e for e in parsed_events if e["timestamp"] is not None]
        parsed_events.sort(key=lambda x: x["timestamp"])

        total_events_count = len(parsed_events)

        pickup_events = [e for e in parsed_events if e["eventtype"] == "PU"]
        delivery_events = [e for e in parsed_events if e["eventtype"] == "DL"]

        pickup_time = pickup_events[0]["timestamp"] if pickup_events else None
        delivery_time = delivery_events[-1]["timestamp"] if delivery_events else None

        if pickup_time and delivery_time:
            total_transit_hours = (delivery_time - pickup_time).total_seconds() / 3600
        else:
            total_transit_hours = None

        facilities = {
            e["location"]
            for e in parsed_events
            if e["location"] and "FACILITY" in str(e["location"]).upper()
        }
        num_facilities = len(facilities)

        in_transit_events = [e for e in parsed_events if e["eventtype"] == "IT"]
        num_in_transit_events = len(in_transit_events)

        od_events = [e for e in parsed_events if e["eventtype"] == "OD"]
        num_od_attempts = len(od_events)

        first_attempt = num_od_attempts == 1 and delivery_time is not None

        facility_event_times = [
            e["timestamp"]
            for e in parsed_events
            if e["location"] and "FACILITY" in str(e["location"]).upper()
        ]
        time_inter_facility = 0.0
        if len(facility_event_times) >= 2:
            facility_event_times.sort()
            for i in range(1, len(facility_event_times)):
                diff = (
                    facility_event_times[i] - facility_event_times[i - 1]
                ).total_seconds() / 3600
                time_inter_facility += diff

        if total_transit_hours is not None and num_facilities > 0:
            avg_per_facility = total_transit_hours / num_facilities
        else:
            avg_per_facility = None

        is_express = False
        if service_type:
            s = service_type.lower()
            if "express" in s or "priority" in s:
                is_express = True

        detailed_rows.append(
            {
                "tracking_number": tracking_no,
                "service_type": service_type,
                "carrier_code": carrier,
                "package_weight_kg": package_weight,
                "packaging_type": packaging,
                "origin_city": origin_city,
                "origin_state": origin_state,
                "destination_city": dest_city,
                "destination_state": dest_state,
                "pickup_datetime_ist": pickup_time,
                "delivery_datetime_ist": delivery_time,
                "total_transit_hours": total_transit_hours,
                "num_facilities_visited": num_facilities,
                "num_in_transit_events": num_in_transit_events,
                "time_in_inter_facility_transit_hours": time_inter_facility,
                "avg_hours_per_facility": avg_per_facility,
                "is_express_service": is_express,
                "delivery_location_type": delivery_location_type,
                "num_out_for_delivery_attempts": num_od_attempts,
                "first_attempt_delivery": first_attempt,
                "total_events_count": total_events_count,
            }
        )

    df = pd.DataFrame(detailed_rows)

    df.to_csv("transit_performance_detailed.csv", index=False)
    print("Created: transit_performance_detailed.csv")

    summary = {}

    valid_transit = df["total_transit_hours"].dropna()
    if not valid_transit.empty:
        summary["total_shipments_analyzed"] = len(df)
        summary["avg_transit_hours"] = valid_transit.mean()
        summary["median_transit_hours"] = valid_transit.median()
        summary["std_dev_transit_hours"] = valid_transit.std()
        summary["min_transit_hours"] = valid_transit.min()
        summary["max_transit_hours"] = valid_transit.max()
    else:
        summary["total_shipments_analyzed"] = len(df)
        summary["avg_transit_hours"] = None
        summary["median_transit_hours"] = None
        summary["std_dev_transit_hours"] = None
        summary["min_transit_hours"] = None
        summary["max_transit_hours"] = None

    summary["avg_facilities_per_shipment"] = df["num_facilities_visited"].mean()
    summary["median_facilities_per_shipment"] = df["num_facilities_visited"].median()

    mode_facilities = (
        df["num_facilities_visited"].mode().iloc[0]
        if not df["num_facilities_visited"].mode().empty
        else None
    )
    summary["mode_facilities_per_shipment"] = mode_facilities

    valid_avg_per_facility = df["avg_hours_per_facility"].dropna()
    if not valid_avg_per_facility.empty:
        summary["avg_hours_per_facility"] = valid_avg_per_facility.mean()
        summary["median_hours_per_facility"] = valid_avg_per_facility.median()
    else:
        summary["avg_hours_per_facility"] = None
        summary["median_hours_per_facility"] = None

    if not df.empty:
        summary["pct_first_attempt_delivery"] = (
            df["first_attempt_delivery"].mean() * 100
        )
        summary["avg_out_for_delivery_attempts"] = (
            df["num_out_for_delivery_attempts"].mean()
        )
    else:
        summary["pct_first_attempt_delivery"] = None
        summary["avg_out_for_delivery_attempts"] = None

    summary_df = pd.DataFrame([summary])
    summary_df.to_csv("transit_performance_summary.csv", index=False)
    print("Created: transit_performance_summary.csv")


if __name__ == "__main__":
    main()
