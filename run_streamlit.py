import subprocess
import sys
import os

def run_streamlit():
    """Run the Streamlit app"""
    try:
        # Change to the current directory
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        
        # Run streamlit
        subprocess.run([sys.executable, "-m", "streamlit", "run", "streamlit_app.py", "--server.port", "8501"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Streamlit: {e}")
    except KeyboardInterrupt:
        print("\nStreamlit app stopped.")

if __name__ == "__main__":
    run_streamlit()
