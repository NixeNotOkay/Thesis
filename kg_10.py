####
# this script connects to a Neo4j database, verifies the connection, and builds a knowledge graph for battery parameters and faults.
###

print("system starting...")

import time
from colorama import init, Fore
init(autoreset=True)
from datetime import datetime
from neo4j import GraphDatabase
import pandas as pd

uri = "neo4j+s://b856c2f8.databases.neo4j.io"
user = "neo4j"
password = "oJb9t2hd3Ms7ghfEsHRdmKaYp7uPYMx22bm7T4d3nco"


expected_kernel = "5.27-aura"
expected_cypher = ["5", "25"]

driver = GraphDatabase.driver(uri, auth=(user, password))

# Clear existing data before building
with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
    print("Cleared existing graph data")

# TEST CONNECTION
def test_connection(tx):
    result = tx.run("CALL dbms.components() YIELD name, versions RETURN name, versions")
    kernel_version = None
    cypher_version = None
    for record in result:
        if record["name"] == "Neo4j Kernel":
            kernel_version = record["versions"][0]
        elif record["name"] == "Cypher":
            cypher_version = record["versions"]
    if kernel_version == expected_kernel:
        print("Kernel", Fore.GREEN + "Verified")
    else:
        print("Kernel", Fore.RED + "Error")
        print(f"Kernel: {kernel_version}, Expected: {expected_kernel}")

    if cypher_version == expected_cypher:
        print("Cypher", Fore.GREEN + "Verified")
    else:
        print("Cypher", Fore.RED + "Error")
        print(f"Cypher: {cypher_version}, Expected: {expected_cypher}")

    if kernel_version == expected_kernel and cypher_version == expected_cypher:
        print("Connected")
    else:
        print("Connection Error")

with driver.session(database="neo4j") as session:
    session.execute_read(test_connection)

# BUILD KNOWLEDGE GRAPH 
print("Building knowledge graph...")
start_time = time.time()

def build_kg(tx):       # added
    parameters = ['Impedance','Voltage','IntTemp','SurfaceTemp',  'Capacity']
    soc_levels = [0, 20, 40, 60, 80, 100]
    fault_types = ['Overvoltage', 'Voltage_Drop', 'Battery_Aging', 'Thermal_Runaway']

    for parameter in parameters:
        tx.run("MERGE (p:Parameter {name: $name})", name=parameter)

    for soc in soc_levels:
        tx.run("MERGE (s:SoC {level: $level})", level=soc)

    for fault in fault_types:
        tx.run("MERGE (f:Fault {name: $name})", name=fault)


# ADD PARAMETER LIMITS
def add_parameter_limits(tx):
    tx.run("""
    // VOLTAGE LIMITS PER SOC
    MERGE (v:Parameter {name: 'Voltage'})
    WITH v
    UNWIND [
        {soc: 0, v_min: 3.0, v_max: 3.3},
        {soc: 20, v_min: 3.3, v_max: 3.6},
        {soc: 40, v_min: 3.6, v_max: 3.8},
        {soc: 60, v_min: 3.8, v_max: 4.0},
        {soc: 80, v_min: 4.0, v_max: 4.1},
        {soc: 100, v_min: 4.1, v_max: 4.2}
    ] AS step
    MERGE (soc_node:SoC {level: step.soc})
    MERGE (vol_lower:Limit {type: 'Volt_lower_Limit', value: step.v_min})
    MERGE (vol_upper:Limit {type: 'Volt_upper_Limit', value: step.v_max})
    MERGE (v)-[:HAS_LIMIT]->(vol_lower)
    MERGE (v)-[:HAS_LIMIT]->(vol_upper)
    MERGE (soc_node)-[:EXPECTED_RANGE]->(vol_lower)
    MERGE (soc_node)-[:EXPECTED_RANGE]->(vol_upper)

    // INTERNAL IMPEDANCE LIMITS
    MERGE (imp:Parameter {name: 'Impedance'})
    WITH imp
    UNWIND [
        {soc: 0, imp_min: 0.0, imp_max: 0.02},
        {soc: 20, imp_min: 0.02, imp_max: 0.03},
        {soc: 40, imp_min: 0.03, imp_max: 0.035},
        {soc: 60, imp_min: 0.035, imp_max: 0.04},
        {soc: 80, imp_min: 0.04, imp_max: 0.045},
        {soc: 100, imp_min: 0.045, imp_max: 0.05}
    ] AS step
    MERGE (soc_node:SoC {level: step.soc})
    MERGE (imp_lower:Limit {type: 'Impedance_lower_Limit', value: step.imp_min})
    MERGE (imp_upper:Limit {type: 'Impedance_upper_Limit', value: step.imp_max})
    MERGE (imp)-[:HAS_LIMIT]->(imp_lower)
    MERGE (imp)-[:HAS_LIMIT]->(imp_upper)
    MERGE (soc_node)-[:EXPECTED_RANGE]->(imp_lower)
    MERGE (soc_node)-[:EXPECTED_RANGE]->(imp_upper)

    // INTERNAL TEMPERATURE
    MERGE (int_temp:Parameter {name: 'IntTemp'})
    MERGE (int_temp_upper:Limit {type: 'Temperature_Upper_Limit', value: 58})
    MERGE (int_temp)-[:HAS_LIMIT]->(int_temp_upper)

    // SURFACE TEMPERATURE
    MERGE (sfc_temp:Parameter {name: 'SurfaceTemp'})
    MERGE (sfc_temp_upper:Limit {type: 'Surface_Temperature_Upper_Limit', value: 55})
    MERGE (sfc_temp)-[:HAS_LIMIT]->(sfc_temp_upper)

    // CAPACITY
    MERGE (cap:Parameter {name: 'Capacity'})
    MERGE (cap_lower:Limit {type: 'Capacity_Lower_Limit', value: 0.8})
    MERGE (cap)-[:HAS_LIMIT]->(cap_lower)
        
    // VOLTAGE RATE OF CHANGE
    MERGE(roc:Parameter {name: 'Voltage_RoC'})
    WITH roc
    UNWIND [
        {soc: 0, roc_max: 0.1},
        {soc: 20, roc_max: 0.1},
        {soc: 40, roc_max: 0.1},
        {soc: 60, roc_max: 0.1},
        {soc: 80, roc_max: 0.1},
        {soc: 100, roc_max: 0.1}
    ] AS step
    MERGE (soc_node:SoC {level: step.soc})
    MERGE (roc_upper:Limit {type: 'Rate_of_Change_Upper_Limit', value: step.roc_max})
    MERGE (roc)-[:HAS_LIMIT]->(roc_upper)
    MERGE (soc_node)-[:EXPECTED_RANGE]->(roc_upper)  
    """)


# ADD FAULTS WITH SEVERITY LEVELS
# SEVERITY NOTES: Lower number = higher priority. User safety is prioritized over equipment damage.
# 1-3: Immediate danger to user, 4-6: Electrical hazards, 7-8: Property damage, 9-10: Performance issues
def add_faults(tx):
     tx.run("""
        // CRITICAL FAULTS (Severity 1-3) - Immediate danger to user safety
        // THERMAL RUNAWAY - Most critical (Severity 1) - Fire/explosion risk requires immediate evacuation
        MERGE (fault_thermal_runaway:Fault {name: 'Thermal_Runaway', severity: 1}) 
        WITH fault_thermal_runaway 
        MATCH (int_temp:Parameter {name: 'IntTemp'})-[:HAS_LIMIT]->(int_temp_upper:Limit {type: 'Temperature_Upper_Limit'}), 
              (sfc_temp:Parameter {name: 'SurfaceTemp'})-[:HAS_LIMIT]->(sfc_temp_upper:Limit {type: 'Surface_Temperature_Upper_Limit'}), 
              (imp:Parameter {name: 'Impedance'})-[:HAS_LIMIT]->(imp_upper:Limit {type: 'Impedance_upper_Limit'}) 
        MERGE (fault_thermal_runaway)-[:TRIGGERED_BY]->(int_temp_upper) 
        MERGE (fault_thermal_runaway)-[:TRIGGERED_BY]->(sfc_temp_upper) 
        MERGE (fault_thermal_runaway)-[:ASSOCIATED_WITH]->(imp_upper)

        // HIGH RISK FAULTS (Severity 4-6) - Electrical hazards that could cause injury
        // OVERVOLTAGE WITH HIGH IMPEDANCE (Severity 4) - Electrical shock/fire risk from high voltage + high impedance combo
        MERGE (fault_overvoltage_imp:Fault {name: 'Overvoltage_V_Imp', severity: 4}) 
        WITH fault_overvoltage_imp 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(vol_max:Limit {type: 'Volt_upper_Limit'}),
              (imp:Parameter {name: 'Impedance'})-[:HAS_LIMIT]->(imp_upper:Limit {type: 'Impedance_upper_Limit'}) 
        MERGE (fault_overvoltage_imp)-[:TRIGGERED_BY]->(vol_max)
        MERGE (fault_overvoltage_imp)-[:TRIGGERED_BY]->(imp_upper)
             
        // OVERVOLTAGE ONLY (Severity 5) - Electrical shock risk from high voltage alone
        MERGE (fault_overvoltage:Fault {name: 'Overvoltage_V_Only', severity: 5}) 
        WITH fault_overvoltage 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(vol_max:Limit {type: 'Volt_upper_Limit'}) 
        MERGE (fault_overvoltage)-[:TRIGGERED_BY]->(vol_max)

        // SUDDEN VOLTAGE DROP/INCREASE (Severity 6) - System instability could cause equipment failure
        MERGE (fault_sudden_drop:Fault {name: 'Sudden_Voltage_Drop', severity: 6}) 
        WITH fault_sudden_drop 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(v_rate:Limit {type:'Rate_of_Change_Upper_Limit'}) 
        MERGE (fault_sudden_drop)-[:TRIGGERED_BY]->(v_rate) 

        MERGE (fault_sudden_increase:Fault {name: 'Sudden_Voltage_Increase', severity: 6}) 
        WITH fault_sudden_increase 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(v_rate:Limit {type:'Rate_of_Change_Upper_Limit'}) 
        MERGE (fault_sudden_increase)-[:TRIGGERED_BY]->(v_rate) 

        // MODERATE FAULTS (Severity 7-8) - Property damage risks (battery/equipment)
        // DEEP VOLTAGE DROP (Severity 7) - Permanent battery damage requiring expensive replacement
        MERGE (fault_deep_drop:Fault {name: 'Deep_Voltage_Drop', severity: 7}) 
        WITH fault_deep_drop 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(v_min:Limit {type:'Volt_lower_Limit'}) 
        MERGE (fault_deep_drop)-[:TRIGGERED_BY]->(v_min) 

        // BATTERY AGING - MULTIPLE PARAMETERS (Severity 8) - Multiple aging indicators suggesting significant degradation
        MERGE (fault_battery_aging:Fault {name: 'Battery_Aging', severity: 8}) 
        WITH fault_battery_aging 
        MATCH (imp:Parameter {name: 'Impedance'})-[:HAS_LIMIT]->(imp_upper:Limit {type: 'Impedance_upper_Limit'}),
              (int_temp:Parameter {name: 'IntTemp'})-[:HAS_LIMIT]->(int_temp_upper:Limit {type: 'Temperature_Upper_Limit'}),
              (cap:Parameter {name: 'Capacity'})-[:HAS_LIMIT]->(cap_lower:Limit {type: 'Capacity_Lower_Limit'})
        MERGE (fault_battery_aging)-[:TRIGGERED_BY]->(cap_lower)
        MERGE (fault_battery_aging)-[:ASSOCIATED_WITH]->(imp_upper)
        MERGE (fault_battery_aging)-[:ASSOCIATED_WITH]->(int_temp_upper)

        // LOW SEVERITY FAULTS (Severity 9-10) - Performance issues and single parameter warnings
        // UNDERVOLTAGE ONLY (Severity 9) - Low voltage condition affecting performance but not immediate danger
        MERGE (fault_undervolt:Fault {name: 'Undervolt_V_Only', severity: 9}) 
        WITH fault_undervolt 
        MATCH (v:Parameter {name: 'Voltage'})-[:HAS_LIMIT]->(vol_min:Limit {type: 'Volt_lower_Limit'}) 
        MERGE (fault_undervolt)-[:TRIGGERED_BY]->(vol_min) 

        // INDIVIDUAL AGING FACTORS (Severity 10) - Single parameter degradation warnings for monitoring
        MERGE (fault_aging_impedance:Fault {name: 'Battery_Aging_Impedance', severity: 10}) 
        WITH fault_aging_impedance 
        MATCH (imp:Parameter {name: 'Impedance'})-[:HAS_LIMIT]->(imp_upper:Limit {type: 'Impedance_upper_Limit'})
        MERGE (fault_aging_impedance)-[:TRIGGERED_BY]->(imp_upper)

        MERGE (fault_aging_inttemp:Fault {name: 'Battery_Aging_IntTemp', severity: 10}) 
        WITH fault_aging_inttemp 
        MATCH (int_temp:Parameter {name: 'IntTemp'})-[:HAS_LIMIT]->(int_temp_upper:Limit {type: 'Temperature_Upper_Limit'})
        MERGE (fault_aging_inttemp)-[:TRIGGERED_BY]->(int_temp_upper)

        MERGE (fault_aging_surftemp:Fault {name: 'Battery_Aging_SurfTemp', severity: 10}) 
        WITH fault_aging_surftemp 
        MATCH (sfc_temp:Parameter {name: 'SurfaceTemp'})-[:HAS_LIMIT]->(sfc_temp_upper:Limit {type: 'Surface_Temperature_Upper_Limit'})
        MERGE (fault_aging_surftemp)-[:TRIGGERED_BY]->(sfc_temp_upper)

        MERGE (fault_aging_capacity:Fault {name: 'Battery_Aging_Capacity', severity: 10}) 
        WITH fault_aging_capacity 
        MATCH (cap:Parameter {name: 'Capacity'})-[:HAS_LIMIT]->(cap_lower:Limit {type: 'Capacity_Lower_Limit'})
        MERGE (fault_aging_capacity)-[:TRIGGERED_BY]->(cap_lower)

        // COMPREHENSIVE AGING (Severity 8 - same as multi-parameter aging) - Multiple degradation indicators
        MERGE (fault_aging_all:Fault {name: 'Battery_Aging_All', severity: 8}) 
        WITH fault_aging_all 
        MATCH (imp:Parameter {name: 'Impedance'})-[:HAS_LIMIT]->(imp_upper:Limit {type: 'Impedance_upper_Limit'}),
              (int_temp:Parameter {name: 'IntTemp'})-[:HAS_LIMIT]->(int_temp_upper:Limit {type: 'Temperature_Upper_Limit'}),
              (sfc_temp:Parameter {name: 'SurfaceTemp'})-[:HAS_LIMIT]->(sfc_temp_upper:Limit {type: 'Surface_Temperature_Upper_Limit'}),
              (cap:Parameter {name: 'Capacity'})-[:HAS_LIMIT]->(cap_lower:Limit {type: 'Capacity_Lower_Limit'})
        MERGE (fault_aging_all)-[:TRIGGERED_BY]->(imp_upper)
        MERGE (fault_aging_all)-[:TRIGGERED_BY]->(int_temp_upper)
        MERGE (fault_aging_all)-[:TRIGGERED_BY]->(sfc_temp_upper)
        MERGE (fault_aging_all)-[:TRIGGERED_BY]->(cap_lower)
    """)


# ADD FAULT RESPONSES
def add_mitigations(tx):
    tx.run("""
        // CRITICAL MITIGATIONS - Thermal Runaway (immediate safety actions)
        MERGE (f_tr:Fault {name: "Thermal_Runaway"})
        MERGE (m_shutdown_tr:Mitigation {name: "Immediate_Shutdown"})
        MERGE (m_evacuate_tr:Mitigation {name: "Evacuation_Warning"})
        MERGE (op_chg_tr:OperatingMode {name: "Charging"})
        MERGE (op_dis_tr:OperatingMode {name: "Discharging"})

        MERGE (f_tr)-[:MITIGATED_BY]->(m_shutdown_tr)
        MERGE (m_shutdown_tr)-[:APPLICABLE_IN]->(op_chg_tr)
        MERGE (m_shutdown_tr)-[:APPLICABLE_IN]->(op_dis_tr)
        MERGE (f_tr)-[:RECOVERY_ACTION]->(m_evacuate_tr)
        MERGE (m_evacuate_tr)-[:APPLICABLE_IN]->(op_chg_tr)
        MERGE (m_evacuate_tr)-[:APPLICABLE_IN]->(op_dis_tr)
            
        // ------------------------------------------------------------------
            
        // OVERVOLTAGE MITIGATIONS - Electrical hazard responses
        MERGE (f_over_imp:Fault {name: "Overvoltage_V_Imp"})
        MERGE (f_over_only:Fault {name: "Overvoltage_V_Only"})
        MERGE (m_stop_over:Mitigation {name: "Stop_Charging"})
        MERGE (m_resume_over:Mitigation {name: "Resume_Reduced_Charging"})
        MERGE (op_chg:OperatingMode {name: "Charging"})

        MERGE (f_over_imp)-[:MITIGATED_BY]->(m_stop_over)
        MERGE (f_over_only)-[:MITIGATED_BY]->(m_stop_over)
        MERGE (m_stop_over)-[:APPLICABLE_IN]->(op_chg)
        MERGE (f_over_imp)-[:RECOVERY_ACTION]->(m_resume_over)
        MERGE (f_over_only)-[:RECOVERY_ACTION]->(m_resume_over)
        MERGE (m_resume_over)-[:APPLICABLE_IN]->(op_chg)
            
        // ------------------------------------------------------------------
            
        // VOLTAGE DROP MITIGATIONS - System protection responses
        MERGE (f_sudden_drop:Fault {name: "Sudden_Voltage_Drop"})
        MERGE (f_deep_drop:Fault {name: "Deep_Voltage_Drop"})
        MERGE (f_undervolt:Fault {name: "Undervolt_V_Only"})
        MERGE (m_reduce_load:Mitigation {name: "Reduce_Load"})
        MERGE (op_dis:OperatingMode {name: "Discharging"})

        MERGE (f_sudden_drop)-[:MITIGATED_BY]->(m_reduce_load)
        MERGE (f_deep_drop)-[:MITIGATED_BY]->(m_reduce_load)
        MERGE (f_undervolt)-[:MITIGATED_BY]->(m_reduce_load)
        MERGE (m_reduce_load)-[:APPLICABLE_IN]->(op_dis)
            
        // ------------------------------------------------------------------
            
        // BATTERY AGING MITIGATIONS - Long-term health management
        MERGE (f_ba:Fault {name: "Battery_Aging"})
        MERGE (f_ba_all:Fault {name: "Battery_Aging_All"})
        MERGE (m_reduce_ba:Mitigation {name: "Reduce_Charging_Power"})
        MERGE (m_replace_ba:Mitigation {name: "Recommend_Replacement"})
        MERGE (op_chg_ba:OperatingMode {name: "Charging"})

        MERGE (f_ba)-[:MITIGATED_BY]->(m_reduce_ba)
        MERGE (f_ba_all)-[:MITIGATED_BY]->(m_reduce_ba)
        MERGE (m_reduce_ba)-[:APPLICABLE_IN]->(op_chg_ba)
        MERGE (f_ba)-[:RECOVERY_ACTION]->(m_replace_ba)
        MERGE (f_ba_all)-[:RECOVERY_ACTION]->(m_replace_ba)
        MERGE (m_replace_ba)-[:APPLICABLE_IN]->(op_chg_ba)
            
        // ------------------------------------------------------------------
            
        // INDIVIDUAL AGING FACTOR MITIGATIONS - Monitoring and alert responses
        MERGE (f_aging_imp:Fault {name: "Battery_Aging_Impedance"})
        MERGE (f_aging_int:Fault {name: "Battery_Aging_IntTemp"})
        MERGE (f_aging_surf:Fault {name: "Battery_Aging_SurfTemp"})
        MERGE (f_aging_cap:Fault {name: "Battery_Aging_Capacity"})
        MERGE (m_monitor:Mitigation {name: "Increase_Monitoring_Frequency"})
        MERGE (m_alert_user:Mitigation {name: "Alert_User"})

        MERGE (f_aging_imp)-[:MITIGATED_BY]->(m_monitor)
        MERGE (f_aging_int)-[:MITIGATED_BY]->(m_monitor)
        MERGE (f_aging_surf)-[:MITIGATED_BY]->(m_monitor)
        MERGE (f_aging_cap)-[:MITIGATED_BY]->(m_monitor)
        MERGE (f_aging_imp)-[:RECOVERY_ACTION]->(m_alert_user)
        MERGE (f_aging_int)-[:RECOVERY_ACTION]->(m_alert_user)
        MERGE (f_aging_surf)-[:RECOVERY_ACTION]->(m_alert_user)
        MERGE (f_aging_cap)-[:RECOVERY_ACTION]->(m_alert_user)
    """)        
    
    
with driver.session() as session:
    
    # build parameters, SoC levels, and faults
    session.execute_write(build_kg)

    # add parameter limits
    session.execute_write(add_parameter_limits)

    # add faults and triggers with severity levels
    session.execute_write(add_faults)

    # add mitigations and recovery actions
    session.execute_write(add_mitigations)


end_time = time.time()
print(f"Build completed in {end_time - start_time:.2f} seconds.")

driver.close()