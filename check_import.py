import importlib.util
import sys
import os
import sys
from unittest.mock import MagicMock

# Mock yaml
sys.modules["yaml"] = MagicMock()

# Set CWD to where the scripts are
os.chdir('/Users/jimmyswindler/Desktop/MarcomProductionSuite')
sys.path.append('/Users/jimmyswindler/Desktop/MarcomProductionSuite')

print("Attempting to import 00_Controller...")
try:
    spec = importlib.util.spec_from_file_location("controller", "00_Controller.py")
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)
    print("[SUCCESS] 00_Controller imported successfully.")
    
    # Check if utils_ui is available in the module
    if hasattr(controller, 'utils_ui'):
        print("[SUCCESS] utils_ui is defined in controller.")
    else:
        print("[FAIL] utils_ui is NOT defined in controller.")
        
except Exception as e:
    print(f"[FAIL] Import failed: {e}")
    import traceback
    traceback.print_exc()
