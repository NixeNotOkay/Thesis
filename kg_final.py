####
# this script builds a knowledge graph for battery parameters and faults and exports to files
###

print("system starting...")

import time
from colorama import init, Fore
init(autoreset=True)
from datetime import datetime
import pandas as pd
import pickle
import json

# BUILD KNOWLEDGE GRAPH 
print("Building knowledge graph...")
start_time = time.time()

def build_kg():    
    kg_data = {
        'parameters': ['Impedance','Voltage','IntTemp','SurfaceTemp','Capacity'],
        'soc_levels': [0, 20, 40, 60, 80, 100],
        'fault_types': ['Overvoltage', 'Voltage_Drop', 'Battery_Aging', 'Thermal_Runaway'],
        'nodes': {
            'parameters': [],
            'soc_levels': [],
            'faults': []
        }
    }
    
    # Build the node data instead of writing to Neo4j
    for parameter in kg_data['parameters']:
        kg_data['nodes']['parameters'].append({
            'type': 'Parameter',
            'name': parameter
        })
    
    for soc in kg_data['soc_levels']:
        kg_data['nodes']['soc_levels'].append({
            'type': 'SoC', 
            'level': soc
        })
    
    for fault in kg_data['fault_types']:
        kg_data['nodes']['faults'].append({
            'type': 'Fault',
            'name': fault
        })
    
    return kg_data

# ADD PARAMETER LIMITS
def add_parameter_limits(kg_data):
    kg_data['parameter_limits'] = {
        'Voltage': {
            'limits_per_soc': [
                {'soc': 0, 'v_min': 3.0, 'v_max': 3.3},
                {'soc': 20, 'v_min': 3.3, 'v_max': 3.6},
                {'soc': 40, 'v_min': 3.6, 'v_max': 3.8},
                {'soc': 60, 'v_min': 3.8, 'v_max': 4.0},
                {'soc': 80, 'v_min': 4.0, 'v_max': 4.1},
                {'soc': 100, 'v_min': 4.1, 'v_max': 4.2}
            ]
        },
        'Impedance': {
            'limits_per_soc': [
                {'soc': 0, 'imp_min': 0.0, 'imp_max': 0.02},
                {'soc': 20, 'imp_min': 0.02, 'imp_max': 0.03},
                {'soc': 40, 'imp_min': 0.03, 'imp_max': 0.035},
                {'soc': 60, 'imp_min': 0.035, 'imp_max': 0.04},
                {'soc': 80, 'imp_min': 0.04, 'imp_max': 0.045},
                {'soc': 100, 'imp_min': 0.045, 'imp_max': 0.05}
            ]
        },
        'IntTemp': {'max': 58},
        'SurfaceTemp': {'max': 55},
        'Capacity': {'min': 0.8},
        'Voltage_RoC': {'max': 0.1}
    }
    return kg_data

# ADD FAULTS WITH SEVERITY LEVELS
# SEVERITY NOTES: Lower number = higher priority. User safety is prioritized over equipment damage.
# 1-3: Immediate danger to user, 4-6: Electrical hazards, 7-8: Property damage, 9-10: Performance issues
def add_faults(kg_data):
    kg_data['faults_detailed'] = {
        # CRITICAL FAULTS (Severity 1-3) - Immediate danger to user safety
        'Thermal_Runaway': {
            'severity': 1, 
            'description': 'Fire/explosion risk requires immediate evacuation',
            'triggers': ['IntTemp', 'SurfaceTemp', 'Impedance']
        },
        
        # HIGH RISK FAULTS (Severity 4-6) - Electrical hazards that could cause injury
        'Overvoltage_V_Imp': {
            'severity': 4,
            'description': 'Electrical shock/fire risk from high voltage + high impedance combo',
            'triggers': ['Voltage', 'Impedance']
        },
        'Overvoltage_V_Only': {
            'severity': 5,
            'description': 'Electrical shock risk from high voltage alone', 
            'triggers': ['Voltage']
        },
        'Sudden_Voltage_Drop': {
            'severity': 6,
            'description': 'System instability could cause equipment failure',
            'triggers': ['Voltage_RoC']
        },
        'Sudden_Voltage_Increase': {
            'severity': 6,
            'description': 'System instability could cause equipment failure',
            'triggers': ['Voltage_RoC']
        },
        
        # MODERATE FAULTS (Severity 7-8) - Property damage risks (battery/equipment)
        'Deep_Voltage_Drop': {
            'severity': 7,
            'description': 'Permanent battery damage requiring expensive replacement',
            'triggers': ['Voltage']
        },
        'Battery_Aging': {
            'severity': 8,
            'description': 'Multiple aging indicators suggesting significant degradation',
            'triggers': ['Capacity', 'Impedance', 'IntTemp']
        },
        
        # LOW SEVERITY FAULTS (Severity 9-10) - Performance issues and single parameter warnings
        'Undervolt_V_Only': {
            'severity': 9,
            'description': 'Low voltage condition affecting performance but not immediate danger',
            'triggers': ['Voltage']
        },
        'Battery_Aging_Impedance': {
            'severity': 10,
            'description': 'Single parameter degradation warning for monitoring',
            'triggers': ['Impedance']
        },
        'Battery_Aging_IntTemp': {
            'severity': 10,
            'description': 'Single parameter degradation warning for monitoring',
            'triggers': ['IntTemp']
        },
        'Battery_Aging_SurfTemp': {
            'severity': 10,
            'description': 'Single parameter degradation warning for monitoring',
            'triggers': ['SurfaceTemp']
        },
        'Battery_Aging_Capacity': {
            'severity': 10,
            'description': 'Single parameter degradation warning for monitoring',
            'triggers': ['Capacity']
        },
        'Battery_Aging_All': {
            'severity': 8,
            'description': 'Multiple degradation indicators',
            'triggers': ['Impedance', 'IntTemp', 'SurfaceTemp', 'Capacity']
        }
    }
    return kg_data

# ADD FAULT RESPONSES
def add_mitigations(kg_data):
    kg_data['mitigations'] = {
        'Thermal_Runaway': {
            'mitigations': ['Immediate_Shutdown', 'Evacuation_Warning'],
            'operating_modes': ['Charging', 'Discharging']},
        'Overvoltage_V_Imp': {
            'mitigations': ['Stop_Charging', 'Resume_Reduced_Charging'],
            'operating_modes': ['Charging']},
        'Overvoltage_V_Only': {
            'mitigations': ['Stop_Charging', 'Resume_Reduced_Charging'],
            'operating_modes': ['Charging']},
        'Sudden_Voltage_Drop': {
            'mitigations': ['Reduce_Load'],
            'operating_modes': ['Discharging']},
        'Deep_Voltage_Drop': {
            'mitigations': ['Reduce_Load'],
            'operating_modes': ['Discharging']},
        'Undervolt_V_Only': {
            'mitigations': ['Reduce_Load'],
            'operating_modes': ['Discharging']},
        'Battery_Aging': {
            'mitigations': ['Reduce_Charging_Power', 'Recommend_Replacement'],
            'operating_modes': ['Charging']},
        'Battery_Aging_All': {
            'mitigations': ['Reduce_Charging_Power', 'Recommend_Replacement'],
            'operating_modes': ['Charging']},
        'Battery_Aging_Impedance': {
            'mitigations': ['Increase_Monitoring_Frequency', 'Alert_User'],
            'operating_modes': ['All']},
        'Battery_Aging_IntTemp': {
            'mitigations': ['Increase_Monitoring_Frequency', 'Alert_User'],
            'operating_modes': ['All']},
        'Battery_Aging_SurfTemp': {
            'mitigations': ['Increase_Monitoring_Frequency', 'Alert_User'],
            'operating_modes': ['All']},
        'Battery_Aging_Capacity': {
            'mitigations': ['Increase_Monitoring_Frequency', 'Alert_User'],
            'operating_modes': ['All']}
    }
    return kg_data

# BUILD THE COMPLETE KNOWLEDGE GRAPH
kg_data = build_kg()
kg_data = add_parameter_limits(kg_data)
kg_data = add_faults(kg_data)
kg_data = add_mitigations(kg_data)

# Add metadata
kg_data['metadata'] = {
    'build_timestamp': datetime.now().isoformat(),
    'build_duration': time.time() - start_time,
    'version': '1.0'
}

# Export to pickle file
with open('knowledge_graph.pkl', 'wb') as f:
    pickle.dump(kg_data, f)
print(Fore.GREEN + "Knowledge graph exported to knowledge_graph.pkl")

# Also export to JSON for readability
with open('knowledge_graph.json', 'w') as f:
    json.dump(kg_data, f, indent=2)
print(Fore.GREEN + "Knowledge graph exported to knowledge_graph.json")

# Print summary
print(f"\nKnowledge Graph Summary:")
print(f"Parameters: {len(kg_data['parameters'])}")
print(f"SoC Levels: {len(kg_data['soc_levels'])}")
print(f"Fault Types: {len(kg_data['fault_types'])}")
print(f"Detailed Faults: {len(kg_data['faults_detailed'])}")
print(f"Mitigations: {len(kg_data['mitigations'])}")
print(f"Built in {time.time() - start_time:.2f} seconds")

print(Fore.GREEN + "Knowledge graph build complete! Files ready for Raspberry Pi.")