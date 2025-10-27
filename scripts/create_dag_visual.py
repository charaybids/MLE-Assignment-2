"""
Create Visual Summary of DAG Architecture
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import warnings
warnings.filterwarnings('ignore')

# Create figure
fig, ax = plt.subplots(figsize=(16, 10))
ax.set_xlim(0, 10)
ax.set_ylim(0, 10)
ax.axis('off')

# Title
ax.text(5, 9.5, 'Airflow ML Pipeline Architecture', 
        ha='center', fontsize=24, fontweight='bold')
ax.text(5, 9.1, '3 Independent DAGs for Training, Inference & Monitoring', 
        ha='center', fontsize=14, color='gray')

# DAG 1: Training (Top)
dag1_y = 7.5
ax.add_patch(FancyBboxPatch((0.5, dag1_y), 9, 1.2, 
                            boxstyle="round,pad=0.1", 
                            edgecolor='#2E86AB', facecolor='#E8F4F8', linewidth=2))
ax.text(5, dag1_y + 1, 'DAG 1: ML Training Pipeline', 
        ha='center', fontsize=14, fontweight='bold', color='#2E86AB')
ax.text(5, dag1_y + 0.7, 'Schedule: Weekly (Sundays 2 AM) | Duration: 2-3 hours', 
        ha='center', fontsize=10, color='#555')

tasks1 = ['Bronze\nIngest', 'Silver\nClean', 'Gold\nFeatures', 'Train\nModels', 'Store\nModel']
x_start = 1.5
for i, task in enumerate(tasks1):
    x = x_start + i * 1.6
    ax.add_patch(FancyBboxPatch((x-0.3, dag1_y + 0.05), 0.6, 0.5, 
                                boxstyle="round,pad=0.05", 
                                facecolor='#A8DADC', edgecolor='#457B9D', linewidth=1.5))
    ax.text(x, dag1_y + 0.3, task, ha='center', va='center', fontsize=9, fontweight='bold')
    if i < len(tasks1) - 1:
        ax.annotate('', xy=(x+0.5, dag1_y + 0.3), xytext=(x+0.3, dag1_y + 0.3),
                   arrowprops=dict(arrowstyle='->', lw=2, color='#457B9D'))

# Output label for DAG 1
ax.text(8.5, dag1_y - 0.3, '📦 model_store/', ha='center', fontsize=10, 
        style='italic', color='#2E86AB', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# DAG 2: Inference (Middle)
dag2_y = 5.2
ax.add_patch(FancyBboxPatch((0.5, dag2_y), 9, 1.2, 
                            boxstyle="round,pad=0.1", 
                            edgecolor='#E63946', facecolor='#FFE5E7', linewidth=2))
ax.text(5, dag2_y + 1, 'DAG 2: ML Inference Pipeline', 
        ha='center', fontsize=14, fontweight='bold', color='#E63946')
ax.text(5, dag2_y + 0.7, 'Schedule: Daily (3 AM) | Duration: 10-20 minutes', 
        ha='center', fontsize=10, color='#555')

tasks2 = ['Prepare\nData', 'Retrieve\nModel', 'Make\nPredictions', 'Store\nResults']
x_start = 2.2
for i, task in enumerate(tasks2):
    x = x_start + i * 1.8
    ax.add_patch(FancyBboxPatch((x-0.35, dag2_y + 0.05), 0.7, 0.5, 
                                boxstyle="round,pad=0.05", 
                                facecolor='#F4A3A8', edgecolor='#C1121F', linewidth=1.5))
    ax.text(x, dag2_y + 0.3, task, ha='center', va='center', fontsize=9, fontweight='bold')
    if i < len(tasks2) - 1:
        ax.annotate('', xy=(x+0.6, dag2_y + 0.3), xytext=(x+0.35, dag2_y + 0.3),
                   arrowprops=dict(arrowstyle='->', lw=2, color='#C1121F'))

# Output label for DAG 2
ax.text(8.5, dag2_y - 0.3, '📊 predictions.parquet', ha='center', fontsize=10, 
        style='italic', color='#E63946', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# DAG 3: Monitoring (Bottom)
dag3_y = 2.2
ax.add_patch(FancyBboxPatch((0.5, dag3_y), 9, 2, 
                            boxstyle="round,pad=0.1", 
                            edgecolor='#06A77D', facecolor='#E8F9F3', linewidth=2))
ax.text(5, dag3_y + 1.8, 'DAG 3: ML Monitoring & Governance Pipeline', 
        ha='center', fontsize=14, fontweight='bold', color='#06A77D')
ax.text(5, dag3_y + 1.5, 'Schedule: Daily (4 AM) | Duration: 5-10 minutes', 
        ha='center', fontsize=10, color='#555')

tasks3 = ['Fetch\nData', 'Calculate\nMetrics', 'Store\nResults', 'Visualize\nDashboard']
x_start = 1.8
for i, task in enumerate(tasks3):
    x = x_start + i * 1.8
    ax.add_patch(FancyBboxPatch((x-0.35, dag3_y + 0.85), 0.7, 0.5, 
                                boxstyle="round,pad=0.05", 
                                facecolor='#90E0BF', edgecolor='#048654', linewidth=1.5))
    ax.text(x, dag3_y + 1.1, task, ha='center', va='center', fontsize=9, fontweight='bold')
    if i < len(tasks3) - 1:
        ax.annotate('', xy=(x+0.6, dag3_y + 1.1), xytext=(x+0.35, dag3_y + 1.1),
                   arrowprops=dict(arrowstyle='->', lw=2, color='#048654'))

# Governance Gate (Diamond)
gate_x = 5
gate_y = dag3_y + 0.3
diamond = mpatches.FancyBboxPatch((gate_x-0.5, gate_y), 1, 0.6,
                                  boxstyle="round,pad=0.1",
                                  facecolor='#FFD166', edgecolor='#F77F00', linewidth=2)
ax.add_patch(diamond)
ax.text(gate_x, gate_y + 0.3, 'Governance\nGate', ha='center', va='center', 
        fontsize=9, fontweight='bold', color='#D62828')

# Branches from governance
# OK branch
ax.annotate('', xy=(7.5, gate_y + 0.3), xytext=(gate_x + 0.5, gate_y + 0.3),
           arrowprops=dict(arrowstyle='->', lw=2, color='#06A77D'))
ax.add_patch(FancyBboxPatch((7.2, gate_y + 0.05), 0.6, 0.5,
                            boxstyle="round,pad=0.05",
                            facecolor='#90E0BF', edgecolor='#048654', linewidth=1.5))
ax.text(7.5, gate_y + 0.3, 'OK\nEnd', ha='center', va='center', fontsize=9, fontweight='bold')
ax.text(6.5, gate_y + 0.55, '✅ Performance OK', fontsize=8, color='#06A77D')

# Degraded branch
ax.annotate('', xy=(2.5, gate_y + 0.3), xytext=(gate_x - 0.5, gate_y + 0.3),
           arrowprops=dict(arrowstyle='->', lw=2, color='#D62828'))
ax.add_patch(FancyBboxPatch((2.2, gate_y + 0.05), 0.6, 0.5,
                            boxstyle="round,pad=0.05",
                            facecolor='#F4A3A8', edgecolor='#C1121F', linewidth=1.5))
ax.text(2.5, gate_y + 0.3, 'Trigger\nDAG 1', ha='center', va='center', fontsize=8, fontweight='bold')
ax.text(3.5, gate_y + 0.55, '🔴 Retrain', fontsize=8, color='#D62828')

# Connection arrows between DAGs
# DAG 1 -> DAG 2 (model flow)
ax.annotate('', xy=(8.5, dag2_y + 1.2), xytext=(8.5, dag1_y),
           arrowprops=dict(arrowstyle='->', lw=3, color='#457B9D', linestyle='dashed'))
ax.text(8.8, dag2_y + 1.5, 'Model\nReady', fontsize=9, color='#457B9D', ha='left')

# DAG 2 -> DAG 3 (predictions flow)
ax.annotate('', xy=(8.5, dag3_y + 2), xytext=(8.5, dag2_y),
           arrowprops=dict(arrowstyle='->', lw=3, color='#C1121F', linestyle='dashed'))
ax.text(8.8, dag3_y + 2.3, 'Predictions\nReady', fontsize=9, color='#C1121F', ha='left')

# DAG 3 -> DAG 1 (retrain trigger)
ax.annotate('', xy=(1.5, dag1_y), xytext=(2.5, gate_y + 0.05),
           arrowprops=dict(arrowstyle='->', lw=3, color='#D62828', 
                          linestyle='dashed', connectionstyle="arc3,rad=0.3"))
ax.text(0.8, dag1_y + 0.5, 'Auto\nRetrain', fontsize=9, color='#D62828', ha='center',
        bbox=dict(boxstyle='round', facecolor='#FFE5E7', alpha=0.8))

# Legend
legend_y = 0.8
ax.text(1, legend_y, '📋 Legend:', fontsize=11, fontweight='bold')
ax.text(1, legend_y - 0.25, '• Weekly Training: Sundays 2 AM', fontsize=9)
ax.text(1, legend_y - 0.45, '• Daily Inference: 3 AM', fontsize=9)
ax.text(1, legend_y - 0.65, '• Daily Monitoring: 4 AM', fontsize=9)

ax.text(5.5, legend_y, '🎯 Key Metrics:', fontsize=11, fontweight='bold')
ax.text(5.5, legend_y - 0.25, '• AUC ≥ 0.70 | Precision ≥ 0.60', fontsize=9)
ax.text(5.5, legend_y - 0.45, '• Recall ≥ 0.50 | F1 ≥ 0.55', fontsize=9)
ax.text(5.5, legend_y - 0.65, '• PSI < 0.2 (Drift Detection)', fontsize=9)

# Footer
ax.text(5, 0.2, 'Production-Ready ML Pipeline | 100% Notebook Logic Implemented | Automatic Governance', 
        ha='center', fontsize=10, style='italic', color='gray')

plt.tight_layout()
plt.savefig('code/dag_architecture_visual.png', dpi=300, bbox_inches='tight', facecolor='white')
print("✅ Visual diagram saved: code/dag_architecture_visual.png")
plt.close()
