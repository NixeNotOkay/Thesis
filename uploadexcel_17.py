
# QUERY SCRIPT 

import time
from colorama import init, Fore
init(autoreset=True)
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import pandas as pd

# connect to neo4j [cloud]
# uri = "neo4j+s://b856c2f8.databases.neo4j.io"
# user = "neo4j"
# password = "oJb9t2hd3Ms7ghfEsHRdmKaYp7uPYMx22bm7T4d3nco"

# connect to pi
uri = "bolt://localhost:7687"
user = "neo4j"
password = keyichidema

driver = GraphDatabase.driver(uri, auth=(user, password))

# read the excel file
df = pd.read_excel(r"C:\Users\Avary\Downloads\python\main\unexpected_running_conditions.xlsx")

# remove if file doesn't have error column but honestly even if it's not there, this shouldn't affect the file at all
df = df.drop(columns=['expected error'], errors='ignore')

# Force numeric types for all monitored parameters
for col in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity', 'SoC']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# timestamps data
last_received = {param: datetime.now() for param in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']}

# RESET INTERPOLATED LIMITS CACHE
interpolated_limits = None

# Calculates SoC limits at start up
def precalculate_interpolated_limits(tx):
    """Pre-calculate ALL SoC-dependent limits for SoC 0-100 at startup using interpolation"""

    # Get ALL existing SoC limits from Neo4j for Voltage, Impedance, and Voltage_RoC
    query = """
    MATCH (soc:SoC)-[:EXPECTED_RANGE]->(l:Limit)
    MATCH (p:Parameter)-[:HAS_LIMIT]->(l)
    WHERE p.name IN ['Voltage', 'Impedance', 'Voltage_RoC']
    RETURN soc.level AS soc_level, p.name AS param_name, l.type AS limit_type, l.value AS limit_value
    ORDER BY soc_level, param_name, limit_type
    """
    
    results = tx.run(query).data()
    
    # Organize limits by parameter and SoC level
    param_limits = {
        'Voltage': {},
        'Impedance': {},
        'Voltage_RoC': {}
    }
    
    for r in results:
        param = r['param_name']
        soc = r['soc_level']
        limit_type = r['limit_type']
        
        if soc not in param_limits[param]:
            param_limits[param][soc] = {}
        param_limits[param][soc][limit_type] = r['limit_value']
    
    print(f"Found SoC levels with defined limits:")
    for param, limits in param_limits.items():
        print(f"  {param}: {len(limits)} SoC levels")
    
    print()
    # Pre-calculate interpolated limits for all SoC levels 0-100 for all parameters
    all_limits = {}
    total_socs = 101  # 0 to 100 inclusive
    
    print(f"Calculating interpolated limits for all parameters:")
    print(Fore.CYAN + "Progress: [", end="")
    
    for soc in range(0, 101):
        # Simple progress indicator
        if soc % 10 == 0:
            print(Fore.CYAN + "█", end="")
        
        all_limits[soc] = {}
        
        # Interpolate Voltage limits
        if soc in param_limits['Voltage']:
            all_limits[soc]['Voltage'] = param_limits['Voltage'][soc]
        else:
            # Find bounding SoC levels for Voltage interpolation
            voltage_socs = list(param_limits['Voltage'].keys())
            lower_soc = max([s for s in voltage_socs if s <= soc], default=None)
            upper_soc = min([s for s in voltage_socs if s >= soc], default=None)
            
            if lower_soc is not None and upper_soc is not None:
                ratio = (soc - lower_soc) / (upper_soc - lower_soc)
                
                lower_min = param_limits['Voltage'][lower_soc]['Volt_lower_Limit']
                lower_max = param_limits['Voltage'][lower_soc]['Volt_upper_Limit']
                upper_min = param_limits['Voltage'][upper_soc]['Volt_lower_Limit']
                upper_max = param_limits['Voltage'][upper_soc]['Volt_upper_Limit']
                
                all_limits[soc]['Voltage'] = {
                    'Volt_lower_Limit': round(lower_min + (upper_min - lower_min) * ratio, 3),
                    'Volt_upper_Limit': round(lower_max + (upper_max - lower_max) * ratio, 3)
                }
            else:
                all_limits[soc]['Voltage'] = {'Volt_lower_Limit': 3.0, 'Volt_upper_Limit': 4.2}
        
        # Interpolate Impedance limits
        if soc in param_limits['Impedance']:
            all_limits[soc]['Impedance'] = param_limits['Impedance'][soc]
        else:
            # Find bounding SoC levels for Impedance interpolation
            impedance_socs = list(param_limits['Impedance'].keys())
            lower_soc = max([s for s in impedance_socs if s <= soc], default=None)
            upper_soc = min([s for s in impedance_socs if s >= soc], default=None)
            
            if lower_soc is not None and upper_soc is not None:
                ratio = (soc - lower_soc) / (upper_soc - lower_soc)
                
                lower_min = param_limits['Impedance'][lower_soc]['Impedance_lower_Limit']
                lower_max = param_limits['Impedance'][lower_soc]['Impedance_upper_Limit']
                upper_min = param_limits['Impedance'][upper_soc]['Impedance_lower_Limit']
                upper_max = param_limits['Impedance'][upper_soc]['Impedance_upper_Limit']
                
                all_limits[soc]['Impedance'] = {
                    'Impedance_lower_Limit': round(lower_min + (upper_min - lower_min) * ratio, 4),
                    'Impedance_upper_Limit': round(lower_max + (upper_max - lower_max) * ratio, 4)
                }
            else:
                all_limits[soc]['Impedance'] = {'Impedance_lower_Limit': 0.0, 'Impedance_upper_Limit': 0.05}
        
        # Interpolate Voltage Rate of Change limits
        if soc in param_limits['Voltage_RoC']:
            all_limits[soc]['Voltage_RoC'] = param_limits['Voltage_RoC'][soc]
        else:
            # Find bounding SoC levels for Voltage_RoC interpolation
            roc_socs = list(param_limits['Voltage_RoC'].keys())
            lower_soc = max([s for s in roc_socs if s <= soc], default=None)
            upper_soc = min([s for s in roc_socs if s >= soc], default=None)
            
            if lower_soc is not None and upper_soc is not None:
                ratio = (soc - lower_soc) / (upper_soc - lower_soc)
                
                lower_max = param_limits['Voltage_RoC'][lower_soc]['Rate_of_Change_Upper_Limit']
                upper_max = param_limits['Voltage_RoC'][upper_soc]['Rate_of_Change_Upper_Limit']
                
                all_limits[soc]['Voltage_RoC'] = {
                    'Rate_of_Change_Upper_Limit': round(lower_max + (upper_max - lower_max) * ratio, 2)
                }
            else:
                all_limits[soc]['Voltage_RoC'] = {'Rate_of_Change_Upper_Limit': 0.1}
    
    print(Fore.CYAN + "] 100%")
    return all_limits

# CHARGING SAFETY CHECK
def check_charging_safety(tx, row, status):
    """Check for overcharging conditions during charging"""
    if status != "Charging":
        return None
    
    current_voltage = row.get('Voltage')
    current_soc = int(row.get('SoC')) if not pd.isna(row.get('SoC')) else 0
    
    if interpolated_limits and current_soc in interpolated_limits:
        voltage_limits = interpolated_limits[current_soc].get('Voltage', {})
        max_voltage = voltage_limits.get('Volt_upper_Limit', 4.2)
        
        if current_voltage > max_voltage:
            return "Overvoltage_Charging"
    
    return None

# check for faults here and match closest soc level
# there is a problem here where if SoC is missing, code will pass it as a NaN, and NO FAULTS WILL BE DETECTED

# THIS DEFINES AND RUNS THE DATABASE QUERY TO FETCH FAULTS AND LIMITS FROM NEO4J, INCLUDING SoC-DEPENDENT AND INDEPENDENT CASES. 
def check_faults_and_alert(tx, row, status, previous_row=None):
    global interpolated_limits
    
    triggered = {}
    
    # Calculate rate of change if previous row exists
    voltage_roc = 0
    if previous_row is not None and 'Voltage' in previous_row and 'Voltage' in row:
        voltage_roc = row['Voltage'] - previous_row['Voltage']  # Simple difference for now
    
    current_voltage = row.get('Voltage')
    current_impedance = row.get('Impedance') 
    current_inttemp = row.get('IntTemp')
    current_surftemp = row.get('SurfaceTemp')
    current_capacity = row.get('Capacity')
    current_soc = int(row.get('SoC')) if not pd.isna(row.get('SoC')) else 0

    # Use pre-calculated interpolated limits (fast dict lookup)
    if interpolated_limits and current_soc in interpolated_limits:
        soc_limits = interpolated_limits[current_soc]
        
        # DEBUG: Show limits being used
        print(Fore.LIGHTBLACK_EX + f"LOGIC CHECK: For SoC {current_soc}% - Limits:")
        for param, limits in soc_limits.items():
            print(Fore.LIGHTBLACK_EX + f"  {param}: {limits}")
    else:
        print(Fore.RED + f"ERROR: No interpolated limits found for SoC {current_soc}")
        print("_" * 40)
        print()
        return triggered, row

    # Get SoC-independent parameter limits (temperature, capacity) from Neo4j
    query = """
    MATCH (p:Parameter)-[:HAS_LIMIT]->(l:Limit)
    WHERE p.name IN ['IntTemp', 'SurfaceTemp', 'Capacity']
    RETURN p.name AS param, l.type AS limit_type, l.value AS limit_val
    """
    
    limits = tx.run(query).data()
    
    # Convert limits to dictionary for easy access
    limits_dict = {}
    for limit in limits:
        param = limit['param']
        limit_type = limit['limit_type']
        limits_dict[(param, limit_type)] = limit['limit_val']

    # Add interpolated SoC-dependent limits to the limits_dict
    if 'Voltage' in soc_limits:
        limits_dict[('Voltage', 'Volt_lower_Limit')] = soc_limits['Voltage']['Volt_lower_Limit']
        limits_dict[('Voltage', 'Volt_upper_Limit')] = soc_limits['Voltage']['Volt_upper_Limit']
    
    if 'Impedance' in soc_limits:
        limits_dict[('Impedance', 'Impedance_lower_Limit')] = soc_limits['Impedance']['Impedance_lower_Limit']
        limits_dict[('Impedance', 'Impedance_upper_Limit')] = soc_limits['Impedance']['Impedance_upper_Limit']
    
    if 'Voltage_RoC' in soc_limits:
        limits_dict[('Voltage_RoC', 'Rate_of_Change_Upper_Limit')] = soc_limits['Voltage_RoC']['Rate_of_Change_Upper_Limit']

    # FAULT DETECTION LOGIC
    faults_detected = []

    # OVERCHARGE DETECTION - Simple voltage check during charging
    charging_fault = check_charging_safety(tx, row, status)
    if charging_fault:
        faults_detected.append(charging_fault)

    # 1. Sudden Voltage Drop (Rate of Change) - Now uses interpolated RoC limits
    roc_key = ('Voltage_RoC', 'Rate_of_Change_Upper_Limit')
    if roc_key in limits_dict and abs(voltage_roc) > limits_dict[roc_key]:
        if voltage_roc < 0:
            faults_detected.append('Sudden_Voltage_Drop')

    # 2. Deep Voltage Drop (Absolute low voltage)
    v_lower_key = ('Voltage', 'Volt_lower_Limit')
    if v_lower_key in limits_dict and current_voltage < limits_dict[v_lower_key] - 0.5:
        faults_detected.append('Deep_Voltage_Drop')

    # 3. Undervolt [V only]
    if v_lower_key in limits_dict and current_voltage < limits_dict[v_lower_key]:
        imp_upper_key = ('Impedance', 'Impedance_upper_Limit')
        if current_impedance <= limits_dict.get(imp_upper_key, float('inf')):
            faults_detected.append('Undervolt_V_Only')

    # 4. Undervolt [V and Imp] -> Battery Aging
    imp_upper_key = ('Impedance', 'Impedance_upper_Limit')
    if (v_lower_key in limits_dict and current_voltage < limits_dict[v_lower_key] and
        imp_upper_key in limits_dict and current_impedance > limits_dict[imp_upper_key]):
        faults_detected.append('Battery_Aging')

    # 5. Sudden Voltage Increase - Now uses interpolated RoC limits
    if roc_key in limits_dict and voltage_roc > limits_dict[roc_key]:
        faults_detected.append('Sudden_Voltage_Increase')

    # 6. Overvoltage [V only]
    v_upper_key = ('Voltage', 'Volt_upper_Limit')
    if v_upper_key in limits_dict and current_voltage > limits_dict[v_upper_key]:
        if current_impedance <= limits_dict.get(imp_upper_key, float('inf')):
            faults_detected.append('Overvoltage_V_Only')

    # 7. Overvoltage [V & Imp]
    if (v_upper_key in limits_dict and current_voltage > limits_dict[v_upper_key] and
        imp_upper_key in limits_dict and current_impedance > limits_dict[imp_upper_key]):
        faults_detected.append('Overvoltage_V_Imp')

    # 8. Battery Aging [Impedance]
    if (imp_upper_key in limits_dict and current_impedance > limits_dict[imp_upper_key] and
        current_voltage >= limits_dict.get(v_lower_key, 0) and 
        current_voltage <= limits_dict.get(v_upper_key, float('inf'))):
        faults_detected.append('Battery_Aging_Impedance')

    # 9. Battery Aging [Int Temp]
    int_temp_key = ('IntTemp', 'Temperature_Upper_Limit')
    if (int_temp_key in limits_dict and current_inttemp > limits_dict[int_temp_key] and
        current_inttemp > 60):  # Thermal runaway threshold
        faults_detected.append('Thermal_Runaway')
    elif int_temp_key in limits_dict and current_inttemp > limits_dict[int_temp_key]:
        faults_detected.append('Battery_Aging_IntTemp')

    # 10. Battery Aging [Surface Temp]  
    surf_temp_key = ('SurfaceTemp', 'Surface_Temperature_Upper_Limit')
    if (surf_temp_key in limits_dict and current_surftemp > limits_dict[surf_temp_key] and
        current_surftemp > 55):  # Thermal runaway threshold
        faults_detected.append('Thermal_Runaway')
    elif surf_temp_key in limits_dict and current_surftemp > limits_dict[surf_temp_key]:
        faults_detected.append('Battery_Aging_SurfTemp')

    # 11. Battery Aging [Capacity]
    cap_key = ('Capacity', 'Capacity_Lower_Limit')
    if cap_key in limits_dict and current_capacity < limits_dict[cap_key]:
        faults_detected.append('Battery_Aging_Capacity')

    # 12. Battery Aging [All] - Multiple parameters indicating aging
    aging_count = 0
    if imp_upper_key in limits_dict and current_impedance > limits_dict[imp_upper_key]:
        aging_count += 1
    if int_temp_key in limits_dict and current_inttemp > limits_dict[int_temp_key]:
        aging_count += 1  
    if surf_temp_key in limits_dict and current_surftemp > limits_dict[surf_temp_key]:
        aging_count += 1
    if cap_key in limits_dict and current_capacity < limits_dict[cap_key]:
        aging_count += 1
        
    if aging_count >= 3:
        faults_detected.append('Battery_Aging_All')

    # Get mitigations for detected faults - WITH PRIORITY FILTERING
    # Only show the highest priority fault (lowest severity number) to avoid confusion and ensure user safety
    if faults_detected:
        # Query Neo4j for fault severities to determine priority order
        # Lower severity number = higher priority (1 = most critical, 10 = least critical)
        severity_query = """
        MATCH (f:Fault) 
        WHERE f.name IN $faults
        RETURN f.name AS fault, f.severity AS severity
        ORDER BY f.severity
        """
        
        fault_severities = tx.run(severity_query, faults=faults_detected).data()
        
        if fault_severities:
            # Get the highest priority fault (lowest severity number)
            highest_priority_fault = fault_severities[0]
            fault_name = highest_priority_fault['fault']
            fault_severity = highest_priority_fault['severity']
            
            # Get mitigations only for the highest priority fault
            mitigation_query = """
            MATCH (f:Fault {name: $fault_name})-[:MITIGATED_BY|RECOVERY_ACTION]->(m:Mitigation)
            OPTIONAL MATCH (m)-[:APPLICABLE_IN]->(op:OperatingMode {name: $status})
            RETURN f.name AS fault, m.name AS mitigation
            """
            
            mitigations = tx.run(mitigation_query, fault_name=fault_name, status=status).data()
            fault_mitigations = [m['mitigation'] for m in mitigations if m['mitigation']]
            
            # Print only the highest priority fault with its mitigations
            print(Fore.RED + f"FAULT DETECTED: {fault_name}")

            # picks the mitigation strategy
            if fault_mitigations:
                # separate mitigation from recovery actions
                immediate_actions = []
                recovery_actions = []

                # query to get the relationships types
                mitigation_query = """
                MATCH (f:Fault {name: $fault_name}) - [r:MITIGATED_BY|RECOVERY_ACTION] -> (m:Mitigation)
                OPTIONAL MATCH (m) - [:APPLICABLE_IN] -> (op:OperatingMode {name:$status})
                RETURN m.name AS mitigation, type(r) AS relationship_type
                """

                mitigations_with_types = tx.run(mitigation_query, fault_name=fault_name, status=status).data()

                for m in mitigations_with_types:
                    if m['relationship_type'] == 'MITIGATED_BY':
                        immediate_actions.append(m['mitigation'])
                    elif m['relationship_type'] == 'RECOVERY_ACTION':
                        recovery_actions.append(m['mitigation'])

                # okay now display in that order
                if immediate_actions:
                    print(Fore.YELLOW + 'Mitigated By:')
                    for act in set(immediate_actions):
                        if act and ("Alert" in act or "Warning" in act or "Evacuation" in act):
                            print (Fore.RED + f"{act}")
                        elif act:
                            print(Fore.YELLOW + f"{act}")
                
                if recovery_actions:
                    print(f"Recovery action[s]:")
                    for act in set(recovery_actions):
                        print (f"{act}")

            # DEBUG: Show all detected faults for reference (can be removed in production)
            print(Fore.LIGHTMAGENTA_EX + f"DEBUG: All detected faults: {faults_detected}")
            print(Fore.CYAN + f"DEBUG: Showing highest priority: {fault_name} (severity {fault_severity})")
            
            # Store only the highest priority fault in triggered for tracking
            triggered[fault_name] = fault_mitigations
        else:
            print(Fore.YELLOW + f"Warning: No severity data found for faults: {faults_detected}")
            # Fallback to original behavior if no severity data available
            mitigation_query = """
            MATCH (f:Fault)-[:MITIGATED_BY|RECOVERY_ACTION]->(m:Mitigation)
            WHERE f.name IN $faults
            OPTIONAL MATCH (m)-[:APPLICABLE_IN]->(op:OperatingMode {name: $status})
            RETURN f.name AS fault, m.name AS mitigation
            """
            
            mitigations = tx.run(mitigation_query, faults=faults_detected, status=status).data()
            
            # Group mitigations by fault
            for fault in faults_detected:
                fault_mitigations = [m['mitigation'] for m in mitigations if m['fault'] == fault]
                triggered[fault] = fault_mitigations
            
            # Print all faults as fallback
            for fault, mitigations in triggered.items():
                print(Fore.RED + f"FAULT DETECTED: {fault}")
                if mitigations:
                    print(Fore.YELLOW + 'Recommended Actions:')
                    for act in set(mitigations):
                        if act and ("Alert" in act or "Warning" in act or "Evacuation" in act):
                            print(Fore.RED + f"WARNING: {act}")
                        elif act:
                            print(Fore.CYAN + f"{act}")
    else:
        print(Fore.GREEN + "Normal - No faults detected")

    print("_" * 80)
    return triggered, row  # Return current row as previous for next iteration


# Pre-calculate all interpolated limits once at startup
print("Detecting limits:")
start_time_calc = datetime.now()

with driver.session() as session:
    interpolated_limits = session.execute_read(precalculate_interpolated_limits)
    
    end_time_calc = datetime.now()
    calc_duration = (end_time_calc - start_time_calc).total_seconds()
    
    print(f"Calculation completed in {calc_duration:.2f} seconds")

# MAIN MONITORING LOOP
previous_row = None
with driver.session() as session:
    # start monitoring clock to track how long the data upload has been running
    start_time = datetime.now()

    for index, row in df.iterrows():

        print ()
        # print elapsed time in HH:MM:SS format
        elapsed = datetime.now() - start_time
        print(f"Monitoring time: {str(elapsed).split('.')[0]}")

        # check for missing data
        for param in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']:
            if pd.isna(row[param]) or row[param] == 0:
                if (datetime.now() - last_received[param]).total_seconds() > 30:
                    print(f"ALERT: {param} data missing for >30s")
            else:
                last_received[param] = datetime.now()

        # fill missing with last known value
        for param in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']:
            if pd.isna(row[param]) or row[param] == 0:
                row[param] = last_received.get(param, row[param])

        # determine operating status
        if 'Status' not in row or pd.isna(row['Status']):
            print(Fore.RED + "Warning: Operation state missing!!")
            status_str = "Unknown"
        else:
            status_str = "Charging" if row['Status'] == 1 else "Discharging"

        print(pd.DataFrame([row]))
        print(f"Status: ({status_str})")

        # run the check_faults_and_alert function and store faults
        faults, previous_row = session.execute_read(check_faults_and_alert, row, status_str, previous_row)

        # target time for 5s interval
        target_time = start_time + timedelta(seconds=2 * (index + 1))
        sleep_time = (target_time - datetime.now()).total_seconds()
        if sleep_time > 0:
            time.sleep(sleep_time)

        # OKAY THIS IS SUPPOSED TO BE 1HZ BUT I AM GOING TO DO IT 5S APART BC I DON'T WANT MY PC TO SCREAM

# Close driver at the end
driver.close()