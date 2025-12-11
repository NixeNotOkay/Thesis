# QUERY SCRIPT - MODIFIED FOR PI WITH SOCKET SERVER

import time
from colorama import init, Fore
init(autoreset=True)
from datetime import datetime, timedelta
import pandas as pd
import pickle
import socket
import threading
import json

# LOAD KNOWLEDGE GRAPH FROM FILE
print("Loading knowledge graph from file...")
with open('knowledge_graph.pkl', 'rb') as f:
    kg_data = pickle.load(f)

print(Fore.GREEN + "Knowledge graph loaded successfully!")

# SOCKET SERVER CONFIGURATION
HOST = '0.0.0.0'  # Pi's IP
PORT = 5000

# GLOBAL VARIABLES
interpolated_limits = None
previous_row = None
last_received = {param: datetime.now() for param in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']}

# SOCKET SERVER FUNCTIONS
def start_data_server():
    """Start a socket server to receive real-time data from laptop"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(Fore.GREEN + f"Data server listening on {HOST}:{PORT}")
    print("Waiting for laptop connection...")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(Fore.CYAN + f"Connection established from {addr}")
        
        # Handle client in a thread
        client_thread = threading.Thread(
            target=handle_client, 
            args=(client_socket,)
        )
        client_thread.daemon = True
        client_thread.start()

def handle_client(client_socket):
    """Handle incoming data from laptop"""
    global previous_row
    
    try:
        while True:
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                break
                
            print(Fore.GREEN + f"Received sensor data")
            
            # Parse JSON data
            sensor_data = json.loads(data)
            process_realtime_data(sensor_data, client_socket)
            
    except Exception as e:
        print(Fore.RED + f"Client connection error: {e}")
    finally:
        client_socket.close()
        print(Fore.YELLOW + "Client disconnected")

def process_realtime_data(sensor_data, client_socket):
    """Process real-time sensor data and check for faults"""
    global previous_row, last_received
    
    # Convert sensor data to row format
    row = {
        'Voltage': float(sensor_data.get('Voltage', 0)),
        'Impedance': float(sensor_data.get('Impedance', 0)),
        'IntTemp': float(sensor_data.get('IntTemp', 0)),
        'SurfaceTemp': float(sensor_data.get('SurfaceTemp', 0)),
        'Capacity': float(sensor_data.get('Capacity', 0)),
        'SoC': float(sensor_data.get('SoC', 0)),
        'Status': int(sensor_data.get('Status', 0))
    }
    
    # Update last received timestamps
    for param in last_received:
        if row[param] > 0:
            last_received[param] = datetime.now()
    
    # Check for missing data
    for param in ['Voltage', 'Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']:
        if row[param] == 0:
            if (datetime.now() - last_received[param]).total_seconds() > 30:
                print(Fore.RED + f"ALERT: {param} data missing for >30s")
    
    print()
    elapsed = datetime.now() - start_time
    print(f"Monitoring time: {str(elapsed).split('.')[0]}")
    
    # Determine operating status
    status_str = "Charging" if row['Status'] == 1 else "Discharging"
    
    print(pd.DataFrame([row]))
    print(f"Status: ({status_str})")
    
    # Run fault detection
    faults, previous_row = check_faults_and_alert(row, status_str, previous_row)
    

# KNOWLEDGE GRAPH PROCESSING FUNCTIONS
def precalculate_interpolated_limits():
    """Pre-calculate ALL SoC-dependent limits for SoC 0-100 using interpolation"""
    
    param_limits = kg_data['parameter_limits']
    
    print(f"Found SoC levels with defined limits:")
    for param, limits in param_limits.items():
        if 'limits_per_soc' in limits:
            print(f"  {param}: {len(limits['limits_per_soc'])} SoC levels")
    
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
        if 'Voltage' in param_limits and 'limits_per_soc' in param_limits['Voltage']:
            voltage_data = param_limits['Voltage']['limits_per_soc']
            voltage_socs = [item['soc'] for item in voltage_data]
            
            if soc in voltage_socs:
                # Exact match found
                exact_data = next(item for item in voltage_data if item['soc'] == soc)
                all_limits[soc]['Voltage'] = {
                    'Volt_lower_Limit': exact_data['v_min'],
                    'Volt_upper_Limit': exact_data['v_max']
                }
            else:
                # Interpolate
                lower_soc = max([s for s in voltage_socs if s <= soc], default=None)
                upper_soc = min([s for s in voltage_socs if s >= soc], default=None)
                
                if lower_soc is not None and upper_soc is not None:
                    ratio = (soc - lower_soc) / (upper_soc - lower_soc)
                    
                    lower_data = next(item for item in voltage_data if item['soc'] == lower_soc)
                    upper_data = next(item for item in voltage_data if item['soc'] == upper_soc)
                    
                    all_limits[soc]['Voltage'] = {
                        'Volt_lower_Limit': round(lower_data['v_min'] + (upper_data['v_min'] - lower_data['v_min']) * ratio, 3),
                        'Volt_upper_Limit': round(lower_data['v_max'] + (upper_data['v_max'] - lower_data['v_max']) * ratio, 3)
                    }
                else:
                    all_limits[soc]['Voltage'] = {'Volt_lower_Limit': 3.0, 'Volt_upper_Limit': 4.2}
        
        # Interpolate Impedance limits
        if 'Impedance' in param_limits and 'limits_per_soc' in param_limits['Impedance']:
            impedance_data = param_limits['Impedance']['limits_per_soc']
            impedance_socs = [item['soc'] for item in impedance_data]
            
            if soc in impedance_socs:
                exact_data = next(item for item in impedance_data if item['soc'] == soc)
                all_limits[soc]['Impedance'] = {
                    'Impedance_lower_Limit': exact_data['imp_min'],
                    'Impedance_upper_Limit': exact_data['imp_max']
                }
            else:
                lower_soc = max([s for s in impedance_socs if s <= soc], default=None)
                upper_soc = min([s for s in impedance_socs if s >= soc], default=None)
                
                if lower_soc is not None and upper_soc is not None:
                    ratio = (soc - lower_soc) / (upper_soc - lower_soc)
                    
                    lower_data = next(item for item in impedance_data if item['soc'] == lower_soc)
                    upper_data = next(item for item in impedance_data if item['soc'] == upper_soc)
                    
                    all_limits[soc]['Impedance'] = {
                        'Impedance_lower_Limit': round(lower_data['imp_min'] + (upper_data['imp_min'] - lower_data['imp_min']) * ratio, 4),
                        'Impedance_upper_Limit': round(lower_data['imp_max'] + (upper_data['imp_max'] - lower_data['imp_max']) * ratio, 4)
                    }
                else:
                    all_limits[soc]['Impedance'] = {'Impedance_lower_Limit': 0.0, 'Impedance_upper_Limit': 0.05}
        
        # Voltage Rate of Change limits
        if 'Voltage_RoC' in param_limits:
            all_limits[soc]['Voltage_RoC'] = {
                'Rate_of_Change_Upper_Limit': param_limits['Voltage_RoC']['max']
            }
    
    print(Fore.CYAN + "] 100%")
    return all_limits

# CHARGING SAFETY CHECK
def check_charging_safety(row, status):
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

# CHECK FAULTS AND ALERT
def check_faults_and_alert(row, status, previous_row=None):
    global interpolated_limits
    
    triggered = {}
    
    # Calculate rate of change if previous row exists
    voltage_roc = 0
    if previous_row is not None and 'Voltage' in previous_row and 'Voltage' in row:
        voltage_roc = row['Voltage'] - previous_row['Voltage']
    
    current_voltage = row.get('Voltage')
    current_impedance = row.get('Impedance') 
    current_inttemp = row.get('IntTemp')
    current_surftemp = row.get('SurfaceTemp')
    current_capacity = row.get('Capacity')
    current_soc = int(row.get('SoC')) if not pd.isna(row.get('SoC')) else 0

    # Use pre-calculated interpolated limits
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

    # Get limits from kg_data
    limits_dict = {}
    param_limits = kg_data['parameter_limits']
    
    # Add SoC-independent limits
    if 'IntTemp' in param_limits:
        limits_dict[('IntTemp', 'Temperature_Upper_Limit')] = param_limits['IntTemp']['max']
    if 'SurfaceTemp' in param_limits:
        limits_dict[('SurfaceTemp', 'Surface_Temperature_Upper_Limit')] = param_limits['SurfaceTemp']['max']
    if 'Capacity' in param_limits:
        limits_dict[('Capacity', 'Capacity_Lower_Limit')] = param_limits['Capacity']['min']
    
    # Add interpolated SoC-dependent limits
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

    # OVERCHARGE DETECTION
    charging_fault = check_charging_safety(row, status)
    if charging_fault:
        faults_detected.append(charging_fault)

    # 1. Sudden Voltage Drop (Rate of Change)
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

    # 5. Sudden Voltage Increase
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

    # GET MITIGATIONS FOR DETECTED FAULTS
    if faults_detected:
        # Get fault severities from kg_data
        fault_severities = []
        for fault in faults_detected:
            if fault in kg_data['faults_detailed']:
                severity = kg_data['faults_detailed'][fault]['severity']
                fault_severities.append({'fault': fault, 'severity': severity})
        
        # Sort by severity (lowest number = highest priority)
        fault_severities.sort(key=lambda x: x['severity'])
        
        if fault_severities:
            # Get the highest priority fault
            highest_priority_fault = fault_severities[0]
            fault_name = highest_priority_fault['fault']
            fault_severity = highest_priority_fault['severity']
            
            # Get mitigations from kg_data
            fault_mitigations = []
            if fault_name in kg_data['mitigations']:
                fault_mitigations = kg_data['mitigations'][fault_name]['mitigations']
            
            # Print only the highest priority fault with its mitigations
            print(Fore.RED + f"FAULT DETECTED: {fault_name}")

            if fault_mitigations:
                # Display mitigations
                print(Fore.YELLOW + 'Recommended Actions:')
                for act in fault_mitigations:
                    if act and ("Alert" in act or "Warning" in act or "Evacuation" in act):
                        print(Fore.RED + f"WARNING: {act}")
                    elif act:
                        print(Fore.CYAN + f"{act}")
            
            # DEBUG: Show all detected faults for reference
            print(Fore.LIGHTMAGENTA_EX + f"DEBUG: All detected faults: {faults_detected}")
            print(Fore.CYAN + f"DEBUG: Showing highest priority: {fault_name} (severity {fault_severity})")
            
            # Store only the highest priority fault in triggered for tracking
            triggered[fault_name] = fault_mitigations
        else:
            print(Fore.YELLOW + f"Warning: No severity data found for faults: {faults_detected}")
    else:
        print(Fore.GREEN + "Normal - No faults detected")

    print("_" * 80)
    return triggered, row

# INITIALIZATION
print("Initializing real-time battery monitoring system...")
start_time = datetime.now()

# Pre-calculate all interpolated limits once at startup
print("Detecting limits:")
start_time_calc = datetime.now()

interpolated_limits = precalculate_interpolated_limits()

end_time_calc = datetime.now()
calc_duration = (end_time_calc - start_time_calc).total_seconds()
print(f"Calculation completed in {calc_duration:.2f} seconds")

# Start the socket server in a background thread
server_thread = threading.Thread(target=start_data_server)
server_thread.daemon = True
server_thread.start()

print(Fore.GREEN + "Real-time monitoring system ready!")
print("Waiting for sensor data from laptop...")
print("Press Ctrl+C to stop monitoring")

# MAIN LOOP - Keep the program running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(Fore.YELLOW + "\nMonitoring stopped by user")
    print(Fore.GREEN + "Real-time monitoring system shutdown complete!")
