->  This project processes shipment tracking data from a JSON file and analyzes the transit performance of each shipment. 

->  The script extracts tracking details, parses timestamps, orders all events chronologically, and identifies key milestones such as pickup and delivery. 

->  It calculates total transit time in hours, counts the number of facilities visited, measures time spent between facilities, and records in-transit and out-for-delivery events. 

->  It also determines whether a shipment was delivered on the first attempt. All shipment-level metrics are exported into transit_performance_detailed.csv. 

->  Additionally, the script generates an overall performance summary—computing average, median, minimum, maximum, and standard deviation of transit times, 
    as well as facility-related statistics, average hours per facility, percentage of first-attempt deliveries, and average delivery attempts.
    
->  These aggregated insights are saved in transit_performance_summary.csv.
